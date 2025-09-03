# data_extraction/bls_client.py
"""
This module provides a client for interacting with the Bureau of Labor Statistics (BLS) API.
It includes functionality for making requests, handling retries, and managing API keys.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from .api_key import get_random_bls_key
from bls_logging.config import get_logger

BLS_V2_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

log = get_logger(__name__)


@dataclass
class BLSClient:
    """
    A client for the BLS API, responsible for making requests and handling responses.
    Now includes optional database integration for caching and persistence.
    """

    api_key: Optional[str] = field(default_factory=get_random_bls_key)
    url: str = BLS_V2_URL
    session: requests.Session = field(default_factory=requests.Session)

    series_limit: int = 50  # per request (v2)
    years_limit: int = 20  # per request (v2)
    
    # Database integration (optional)
    use_database: bool = False
    repository: Optional[Any] = None  # Will be BLSDataRepository when database is enabled

    def __post_init__(self) -> None:
        """
        Initializes the client and sets up retry logic for requests.
        """
        if self.api_key is None:
            self.api_key = get_random_bls_key()
        self._configure_retries()
        
        # Initialize database if enabled
        if self.use_database and self.repository is None:
            self._init_database()

    def _configure_retries(self) -> None:
        """
        Configures retry logic for HTTP requests to handle transient errors.
        """
        try:
            retry = Retry(
                total=5,
                backoff_factor=1.2,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset({"GET", "POST"}),
            )
            adapter = HTTPAdapter(max_retries=retry)
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
        except Exception:
            log.warning("Could not configure retries for requests.")

    def _init_database(self) -> None:
        """
        Initialize database connection and repository.
        """
        try:
            from database.config import DatabaseConfig
            from database.repository import BLSDataRepository
            
            db_config = DatabaseConfig()
            if not db_config.check_connection():
                log.warning("Database connection failed. Disabling database features.")
                self.use_database = False
                return
            
            # Store the database config for creating sessions
            self._db_config = db_config
            log.info("Database integration enabled successfully.")
                
        except ImportError as e:
            log.warning(f"Database dependencies not available: {e}. Disabling database features.")
            self.use_database = False
        except Exception as e:
            log.warning(f"Database initialization failed: {e}. Disabling database features.")
            self.use_database = False
    
    def _get_repository(self):
        """Get a repository instance with a fresh session."""
        if not self.use_database or not hasattr(self, '_db_config'):
            return None
        
        try:
            from database.repository import BLSDataRepository
            session = self._db_config.SessionLocal()
            return BLSDataRepository(session)
        except Exception as e:
            log.warning(f"Failed to create repository: {e}")
            return None

    def _post(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sends a POST request to the BLS API with the given payload.
        """
        headers = {"Content-Type": "application/json"}
        try:
            resp = self.session.post(self.url, json=payload, headers=headers, timeout=60)
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise RuntimeError(f"BLS API HTTP error: {e} — body: {resp.text[:500]}") from e

        data = resp.json()
        if data.get("status") != "REQUEST_SUCCEEDED":
            raise RuntimeError(f'BLS API returned status={data.get("status")}: {data.get("message")}')
        return data

    def _year_chunks(self, start: int, end: int) -> List[Tuple[int, int]]:
        """
        Splits a date range into smaller chunks to respect API limits.
        """
        if start > end:
            start, end = end, start
        years = end - start + 1
        if years <= self.years_limit:
            return [(start, end)]

        chunks = []
        s = start
        while s <= end:
            e = min(s + self.years_limit - 1, end)
            chunks.append((s, e))
            s = e + 1
        return chunks

    def fetch(
        self,
        series_ids: Iterable[str],
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        *,
        catalog: bool = False,
        calculations: bool = False,
        annualaverage: bool = False,
        aspects: bool = False,
    ) -> Dict[str, Any]:
        """
        Fetches data from the BLS API, automatically handling series and year limits.
        """
        sids = list(series_ids)
        if not sids:
            raise ValueError("No series IDs provided.")

        merged: Dict[str, Any] = {"status": "REQUEST_SUCCEEDED", "Results": {"series": []}, "message": None}

        series_chunks = [sids[i : i + self.series_limit] for i in range(0, len(sids), self.series_limit)]
        year_chunks = self._year_chunks(start_year, end_year) if (start_year and end_year) else [(start_year, end_year)]

        for sc in series_chunks:
            for ys, ye in year_chunks:
                payload: Dict[str, Any] = {"seriesid": sc}
                if self.api_key:
                    payload["registrationkey"] = self.api_key
                if ys is not None:
                    payload["startyear"] = int(ys)
                if ye is not None:
                    payload["endyear"] = int(ye)
                if catalog:
                    payload["catalog"] = True
                if calculations:
                    payload["calculations"] = True
                if annualaverage:
                    payload["annualaverage"] = True
                if aspects:
                    payload["aspects"] = True

                data = self._post(payload)
                chunk_series = data.get("Results", {}).get("series", [])
                merged["Results"]["series"].extend(chunk_series)
        return merged

    def fetch_with_database(
        self,
        series_ids: Iterable[str],
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        *,
        catalog: bool = False,
        calculations: bool = False,
        annualaverage: bool = False,
        aspects: bool = False,
        use_cache: bool = True,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        """
        Enhanced fetch method that uses database for caching when available.
        Falls back to regular fetch if database is not available.
        """
        if not self.use_database:
            log.info("Database not available, using standard fetch.")
            return self.fetch(
                series_ids, start_year, end_year,
                catalog=catalog, calculations=calculations,
                annualaverage=annualaverage, aspects=aspects
            )
        
        sids = list(series_ids)
        if not sids:
            raise ValueError("No series IDs provided.")
        
        # Check what data we have in the database
        if use_cache and not force_refresh:
            try:
                repository = self._get_repository()
                if repository:
                    # Get cached data from database
                    cached_data = repository.get_series_data(
                        sids, start_year, end_year, include_metadata=catalog
                    )
                    
                    if not cached_data.empty:
                        # Check if we need to fetch any missing data
                        missing_series = repository.get_stale_series(max_age_hours=24)
                        series_to_fetch = [sid for sid in sids if sid in missing_series]
                        
                        if not series_to_fetch:
                            log.info(f"All data for {len(sids)} series found in cache.")
                            return self._dataframe_to_api_format(cached_data)
                        else:
                            log.info(f"Fetching {len(series_to_fetch)} stale series from API.")
                            # Fetch only the missing/stale data
                            api_data = self.fetch(
                                series_to_fetch, start_year, end_year,
                                catalog=catalog, calculations=calculations,
                                annualaverage=annualaverage, aspects=aspects
                            )
                            # Store new data in database
                            self._store_api_data(api_data)
                            # Return combined data
                            return self._combine_cached_and_api_data(cached_data, api_data, sids)
            except Exception as e:
                log.warning(f"Database operation failed: {e}. Falling back to API.")
        
        # Fetch from API and store in database
        api_data = self.fetch(
            sids, start_year, end_year,
            catalog=catalog, calculations=calculations,
            annualaverage=annualaverage, aspects=aspects
        )
        
        if self.use_database:
            self._store_api_data(api_data)
        
        return api_data

    def _dataframe_to_api_format(self, df) -> Dict[str, Any]:
        """Convert pandas DataFrame back to API format for compatibility."""
        if df.empty:
            return {"status": "REQUEST_SUCCEEDED", "Results": {"series": []}, "message": None}
        
        # Group by series_id and convert to API format
        series_data = {}
        for _, row in df.iterrows():
            series_id = row['series_id']
            if series_id not in series_data:
                series_data[series_id] = {
                    "seriesID": series_id,
                    "data": [],
                    "catalog": {
                        "seriesTitle": row.get('series_title'),
                        "surveyName": row.get('survey_name'),
                        "measureDataType": row.get('measure_data_type'),
                        "area": row.get('area'),
                        "item": row.get('item'),
                        "seasonality": row.get('seasonality')
                    }
                }
            
            # Add data point
            data_point = {
                "year": str(row['year']),
                "period": row['period'],
                "periodName": row.get('period_name'),
                "value": str(row['value']) if row['value'] is not None else "",
                "footnotes": [{"text": row.get('footnotes')}] if row.get('footnotes') else []
            }
            series_data[series_id]["data"].append(data_point)
        
        return {
            "status": "REQUEST_SUCCEEDED",
            "Results": {"series": list(series_data.values())},
            "message": None
        }
    
    def _store_api_data(self, api_data: Dict[str, Any]) -> None:
        """Store API data in database."""
        if self.use_database:
            try:
                repository = self._get_repository()
                if repository:
                    # Convert API data to format expected by repository
                    series_data = []
                    for series in api_data.get("Results", {}).get("series", []):
                        series_item = {
                            "series_id": series.get("seriesID"),
                            "series_title": series.get("catalog", {}).get("seriesTitle"),
                            "survey_name": series.get("catalog", {}).get("surveyName"),
                            "measure_data_type": series.get("catalog", {}).get("measureDataType"),
                            "area": series.get("catalog", {}).get("area"),
                            "item": series.get("catalog", {}).get("item"),
                            "seasonality": series.get("catalog", {}).get("seasonality"),
                            "latest": series.get("latest", False),
                            "data": series.get("data", [])
                        }
                        series_data.append(series_item)
                    
                    # Store in database
                    try:
                        repository.upsert_series_data(series_data)
                        repository.session.commit()
                        log.info(f"Stored {len(series_data)} series in database")
                    finally:
                        repository.session.close()
                
            except Exception as e:
                log.warning(f"Failed to store data in database: {e}")
    
    def _combine_cached_and_api_data(self, cached_df, api_data: Dict[str, Any], all_series_ids: List[str]) -> Dict[str, Any]:
        """Combine cached and newly fetched API data."""
        # Convert cached DataFrame to API format
        cached_api_format = self._dataframe_to_api_format(cached_df)
        
        # Merge with new API data
        combined_series = {}
        
        # Add cached series
        for series in cached_api_format.get("Results", {}).get("series", []):
            series_id = series.get("seriesID")
            if series_id in all_series_ids:
                combined_series[series_id] = series
        
        # Add/update with new API data
        for series in api_data.get("Results", {}).get("series", []):
            series_id = series.get("seriesID")
            combined_series[series_id] = series
        
        return {
            "status": "REQUEST_SUCCEEDED",
            "Results": {"series": list(combined_series.values())},
            "message": None
        }
    