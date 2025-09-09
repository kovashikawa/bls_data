# bls_data/database/repository.py

import logging
import re
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Optional

import pandas as pd
from sqlalchemy import and_, desc, func, or_, text
from sqlalchemy.orm import Session

from .models import (
    BLSAlias,
    BLSDataFreshness,
    BLSDataPoint,
    BLSDataQuality,
    BLSExtractionLog,
    BLSSeries,
)

log = logging.getLogger("bls")


def period_to_date(year: int, period: str) -> date:
    """
    Convert BLS period format to date.

    BLS periods can be:
    - M01, M02, ..., M12 (monthly)
    - Q01, Q02, Q03, Q04 (quarterly)
    - A01 (annual)
    - S01, S02 (semiannual)

    Args:
        year: Year (e.g., 2023)
        period: Period string (e.g., "M01", "Q02", "A01")

    Returns:
        date object
    """
    if period.startswith("M"):
        # Monthly data: M01 = January, M02 = February, etc.
        month = int(period[1:])
        return date(year, month, 1)

    elif period.startswith("Q"):
        # Quarterly data: Q01 = Q1, Q02 = Q2, etc.
        quarter = int(period[1:])
        month = (quarter - 1) * 3 + 1  # Q1=Jan, Q2=Apr, Q3=Jul, Q4=Oct
        return date(year, month, 1)

    elif period.startswith("A"):
        # Annual data: A01 = January 1st
        return date(year, 1, 1)

    elif period.startswith("S"):
        # Semiannual data: S01 = January, S02 = July
        semester = int(period[1:])
        month = 1 if semester == 1 else 7
        return date(year, month, 1)

    else:
        # Fallback: try to extract month if it's a number
        try:
            month = int(period)
            if 1 <= month <= 12:
                return date(year, month, 1)
        except ValueError:
            pass

        # If we can't parse it, default to January 1st
        log.warning(
            f"Could not parse period '{period}' for year {year}, defaulting to January 1st"
        )
        return date(year, 1, 1)


class BLSDataRepository:
    def __init__(self, session: Session):
        self.session = session

    def upsert_series_data(
        self, series_data: list[dict], extraction_id: str = None
    ) -> tuple[int, int, int]:
        """Insert or update series data with conflict resolution
        Returns: (inserted_count, updated_count, skipped_count)
        """
        if not extraction_id:
            extraction_id = str(uuid.uuid4())

        inserted_count = 0
        updated_count = 0
        skipped_count = 0

        try:
            for series_item in series_data:
                series_id = series_item.get("series_id")
                if not series_id:
                    continue

                # Upsert series metadata
                series = (
                    self.session.query(BLSSeries).filter_by(series_id=series_id).first()
                )
                if not series:
                    series = BLSSeries(
                        series_id=series_id,
                        series_title=series_item.get("series_title"),
                        survey_name=series_item.get("survey_name"),
                        measure_data_type=series_item.get("measure_data_type"),
                        area=series_item.get("area"),
                        item=series_item.get("item"),
                        seasonality=series_item.get("seasonality"),
                        latest=series_item.get("latest", False),
                    )
                    self.session.add(series)
                    inserted_count += 1
                else:
                    # Update existing series metadata
                    series.series_title = series_item.get(
                        "series_title", series.series_title
                    )
                    series.survey_name = series_item.get(
                        "survey_name", series.survey_name
                    )
                    series.measure_data_type = series_item.get(
                        "measure_data_type", series.measure_data_type
                    )
                    series.area = series_item.get("area", series.area)
                    series.item = series_item.get("item", series.item)
                    series.seasonality = series_item.get(
                        "seasonality", series.seasonality
                    )
                    series.latest = series_item.get("latest", series.latest)
                    series.updated_at = func.current_timestamp()

                # Upsert data points
                for data_point in series_item.get("data", []):
                    year = int(data_point.get("year"))
                    period = data_point.get("period")
                    value = data_point.get("value")

                    if value is None or value == "":
                        continue

                    # Check if data point exists
                    existing_point = (
                        self.session.query(BLSDataPoint)
                        .filter_by(series_id=series_id, year=year, period=period)
                        .first()
                    )

                    if existing_point:
                        # Update existing point
                        existing_point.value = float(value)
                        existing_point.period_name = data_point.get("periodName")
                        existing_point.date = period_to_date(year, period)
                        footnotes = data_point.get("footnotes")
                        if isinstance(footnotes, (list, dict)):
                            footnotes = str(footnotes)
                        existing_point.footnotes = footnotes
                        existing_point.extraction_id = extraction_id
                        existing_point.updated_at = func.current_timestamp()
                        updated_count += 1
                    else:
                        # Insert new point
                        new_point = BLSDataPoint(
                            series_id=series_id,
                            year=year,
                            period=period,
                            period_name=data_point.get("periodName"),
                            date=period_to_date(year, period),
                            value=float(value),
                            footnotes=(
                                str(data_point.get("footnotes"))
                                if data_point.get("footnotes")
                                else None
                            ),
                            extraction_id=extraction_id,
                        )
                        self.session.add(new_point)
                        inserted_count += 1

                # Update freshness tracking
                self._update_freshness_tracking(series_id, extraction_id)

            # Don't commit here - let the caller handle it
            log.info(
                f"Data upsert completed: {inserted_count} inserted, {updated_count} updated"
            )

        except Exception as e:
            log.error(f"Error upserting series data: {e}")
            raise

        return inserted_count, updated_count, skipped_count

    def get_series_data(
        self,
        series_ids: list[str],
        start_year: int = None,
        end_year: int = None,
        include_metadata: bool = True,
    ) -> pd.DataFrame:
        """Retrieve data from database with optional filtering"""
        try:
            query = (
                self.session.query(BLSDataPoint, BLSSeries)
                .join(BLSSeries, BLSDataPoint.series_id == BLSSeries.series_id)
                .filter(BLSDataPoint.series_id.in_(series_ids))
            )

            if start_year:
                query = query.filter(BLSDataPoint.year >= start_year)
            if end_year:
                query = query.filter(BLSDataPoint.year <= end_year)

            results = query.order_by(
                BLSDataPoint.series_id, BLSDataPoint.year, BLSDataPoint.period
            ).all()

            if not results:
                return pd.DataFrame()

            # Convert to DataFrame
            data = []
            for data_point, series in results:
                row = {
                    "series_id": data_point.series_id,
                    "year": data_point.year,
                    "period": data_point.period,
                    "period_name": data_point.period_name,
                    "value": float(data_point.value) if data_point.value else None,
                    "footnotes": data_point.footnotes,
                    "data_source": data_point.data_source,
                }

                if include_metadata:
                    row.update(
                        {
                            "series_title": series.series_title,
                            "survey_name": series.survey_name,
                            "measure_data_type": series.measure_data_type,
                            "area": series.area,
                            "item": series.item,
                            "seasonality": series.seasonality,
                            "latest": series.latest,
                        }
                    )

                data.append(row)

            return pd.DataFrame(data)

        except Exception as e:
            log.error(f"Error retrieving series data: {e}")
            return pd.DataFrame()

    def get_data_freshness(self, series_ids: list[str]) -> dict[str, dict]:
        """Get freshness information for series"""
        try:
            freshness_data = (
                self.session.query(BLSDataFreshness)
                .filter(BLSDataFreshness.series_id.in_(series_ids))
                .all()
            )

            result = {}
            for freshness in freshness_data:
                result[freshness.series_id] = {
                    "last_extracted": freshness.last_extracted,
                    "last_updated": freshness.last_updated,
                    "data_completeness": (
                        float(freshness.data_completeness)
                        if freshness.data_completeness
                        else 0.0
                    ),
                    "next_expected_update": freshness.next_expected_update,
                    "extraction_priority": freshness.extraction_priority,
                }

            return result

        except Exception as e:
            log.error(f"Error retrieving freshness data: {e}")
            return {}

    def mark_series_updated(self, series_id: str, extraction_id: str):
        """Update freshness tracking"""
        try:
            freshness = (
                self.session.query(BLSDataFreshness)
                .filter_by(series_id=series_id)
                .first()
            )
            if not freshness:
                freshness = BLSDataFreshness(series_id=series_id)
                self.session.add(freshness)

            freshness.last_extracted = func.current_timestamp()
            freshness.last_updated = func.current_timestamp()
            freshness.updated_at = func.current_timestamp()

            # Don't commit here - let the caller handle it

        except Exception as e:
            log.error(f"Error updating freshness tracking: {e}")
            raise

    def get_stale_series(self, max_age_hours: int = 24) -> list[str]:
        """Get series that need updating based on age"""
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)

            stale_series = (
                self.session.query(BLSDataFreshness.series_id)
                .filter(
                    or_(
                        BLSDataFreshness.last_extracted.is_(None),
                        BLSDataFreshness.last_extracted < cutoff_time,
                    )
                )
                .all()
            )

            return [series_id for (series_id,) in stale_series]

        except Exception as e:
            log.error(f"Error retrieving stale series: {e}")
            return []

    def search_series(self, query: str, limit: int = 100) -> list[dict]:
        """Full-text search for series"""
        try:
            # Use PostgreSQL full-text search
            search_query = (
                self.session.query(BLSSeries)
                .filter(
                    func.to_tsvector(
                        "english",
                        func.concat(
                            BLSSeries.series_title,
                            " ",
                            func.coalesce(BLSSeries.area, ""),
                            " ",
                            func.coalesce(BLSSeries.item, ""),
                        ),
                    ).match(func.plainto_tsquery("english", query))
                )
                .limit(limit)
            )

            results = []
            for series in search_query.all():
                results.append(
                    {
                        "series_id": series.series_id,
                        "series_title": series.series_title,
                        "survey_name": series.survey_name,
                        "area": series.area,
                        "item": series.item,
                        "seasonality": series.seasonality,
                    }
                )

            return results

        except Exception as e:
            log.error(f"Error searching series: {e}")
            return []

    def _update_freshness_tracking(self, series_id: str, extraction_id: str):
        """Update freshness tracking for a series"""
        try:
            freshness = (
                self.session.query(BLSDataFreshness)
                .filter_by(series_id=series_id)
                .first()
            )
            if not freshness:
                freshness = BLSDataFreshness(series_id=series_id)
                self.session.add(freshness)

            freshness.last_extracted = func.current_timestamp()
            freshness.last_updated = func.current_timestamp()
            freshness.updated_at = func.current_timestamp()

        except Exception as e:
            log.error(f"Error updating freshness tracking for {series_id}: {e}")

    def log_extraction(
        self,
        extraction_id: str,
        series_ids: list[str],
        start_year: int = None,
        end_year: int = None,
        status: str = "success",
        records_extracted: int = 0,
        records_updated: int = 0,
        records_inserted: int = 0,
        error_message: str = None,
        api_calls_made: int = 0,
        extraction_duration_seconds: int = 0,
        metadata: dict = None,
    ):
        """Log extraction details"""
        try:
            log_entry = BLSExtractionLog(
                extraction_id=extraction_id,
                series_ids=series_ids,
                start_year=start_year,
                end_year=end_year,
                records_extracted=records_extracted,
                records_updated=records_updated,
                records_inserted=records_inserted,
                extraction_status=status,
                error_message=error_message,
                api_calls_made=api_calls_made,
                extraction_duration_seconds=extraction_duration_seconds,
                metadata=metadata,
            )
            self.session.add(log_entry)
            # Don't commit here - let the caller handle it

        except Exception as e:
            log.error(f"Error logging extraction: {e}")
            raise
