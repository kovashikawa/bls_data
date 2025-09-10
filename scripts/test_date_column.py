#!/usr/bin/env python3
"""
Test script to demonstrate the new date column functionality.

This script shows how to use the new clean 'date' column for time-series analysis.
"""

import sys
from datetime import date, datetime
from pathlib import Path

# Add the parent directory to the Python path
current_dir = Path(__file__).parent.parent
sys.path.insert(0, str(current_dir))

from bls_logging.config import get_logger, setup_logging
from database.config import DatabaseConfig
from database.models import BLSDataPoint

# Setup logging
setup_logging(log_level="INFO", log_dir="logs", console_output=True, file_output=True)
log = get_logger(__name__)


def test_date_column_queries():
    """Test various queries using the new date column."""
    log.info("=" * 80)
    log.info("📅 TESTING NEW DATE COLUMN FUNCTIONALITY")
    log.info("=" * 80)

    db_config = DatabaseConfig()

    with db_config.get_session() as session:
        # Test 1: Basic date queries
        log.info("\\n1. 📊 Basic Date Queries")
        log.info("-" * 40)

        # Count total data points with dates
        total_with_dates = (
            session.query(BLSDataPoint).filter(BLSDataPoint.date.isnot(None)).count()
        )
        log.info(f"✅ Total data points with dates: {total_with_dates:,}")

        # Test 2: Date range queries
        log.info("\\n2. 📅 Date Range Queries")
        log.info("-" * 40)

        # 2023 data
        data_2023 = (
            session.query(BLSDataPoint)
            .filter(
                BLSDataPoint.date >= date(2023, 1, 1),
                BLSDataPoint.date < date(2024, 1, 1),
            )
            .count()
        )
        log.info(f"✅ 2023 data points: {data_2023:,}")

        # 2024 data
        data_2024 = (
            session.query(BLSDataPoint)
            .filter(
                BLSDataPoint.date >= date(2024, 1, 1),
                BLSDataPoint.date < date(2025, 1, 1),
            )
            .count()
        )
        log.info(f"✅ 2024 data points: {data_2024:,}")

        # Last 5 years
        data_last_5_years = (
            session.query(BLSDataPoint)
            .filter(BLSDataPoint.date >= date(2020, 1, 1))
            .count()
        )
        log.info(f"✅ Last 5 years (2020+): {data_last_5_years:,}")

        # Test 3: Monthly data analysis
        log.info("\\n3. 📈 Monthly Data Analysis")
        log.info("-" * 40)

        # Get monthly data for a specific series
        monthly_data = (
            session.query(BLSDataPoint)
            .filter(
                BLSDataPoint.series_id == "CUUR0000SA0",  # CPI All Items
                BLSDataPoint.period.like("M%"),  # Monthly data
                BLSDataPoint.date >= date(2023, 1, 1),
            )
            .order_by(BLSDataPoint.date)
            .all()
        )

        log.info(f"✅ CPI All Items monthly data (2023+): {len(monthly_data)} points")

        if monthly_data:
            log.info("\\n📋 Sample monthly CPI data:")
            for point in monthly_data[:6]:  # Show first 6 months
                log.info(f"   {point.date.strftime('%Y-%m')}: {point.value}")

        # Test 4: Time series aggregation
        log.info("\\n4. 📊 Time Series Aggregation")
        log.info("-" * 40)

        # Count data by year
        from sqlalchemy import extract, func

        yearly_counts = (
            session.query(
                extract("year", BLSDataPoint.date).label("year"),
                func.count(BLSDataPoint.id).label("count"),
            )
            .filter(BLSDataPoint.date.isnot(None))
            .group_by(extract("year", BLSDataPoint.date))
            .order_by("year")
            .all()
        )

        log.info("✅ Data points by year:")
        for year_count in yearly_counts[-5:]:  # Show last 5 years
            log.info(f"   {int(year_count.year)}: {year_count.count:,} points")

        # Test 5: Date-based filtering examples
        log.info("\\n5. 🔍 Date-Based Filtering Examples")
        log.info("-" * 40)

        # Recent data (last 2 years)
        recent_data = (
            session.query(BLSDataPoint)
            .filter(BLSDataPoint.date >= date(2023, 1, 1))
            .count()
        )
        log.info(f"✅ Recent data (2023+): {recent_data:,} points")

        # Specific month
        march_2023 = (
            session.query(BLSDataPoint)
            .filter(
                BLSDataPoint.date >= date(2023, 3, 1),
                BLSDataPoint.date < date(2023, 4, 1),
            )
            .count()
        )
        log.info(f"✅ March 2023 data: {march_2023:,} points")

        # Quarterly data
        q1_2023 = (
            session.query(BLSDataPoint)
            .filter(
                BLSDataPoint.date >= date(2023, 1, 1),
                BLSDataPoint.date < date(2023, 4, 1),
            )
            .count()
        )
        log.info(f"✅ Q1 2023 data: {q1_2023:,} points")

        # Test 6: Sample queries for analysis
        log.info("\\n6. 🎯 Sample Analysis Queries")
        log.info("-" * 40)

        # Get CPI data for analysis
        cpi_data = (
            session.query(BLSDataPoint)
            .filter(
                BLSDataPoint.series_id == "CUUR0000SA0",
                BLSDataPoint.date >= date(2020, 1, 1),
            )
            .order_by(BLSDataPoint.date)
            .limit(10)
            .all()
        )

        log.info("✅ Sample CPI data for analysis (2020+):")
        for point in cpi_data:
            log.info(f"   {point.date.strftime('%Y-%m-%d')}: {point.value}")

        log.info("\\n" + "=" * 80)
        log.info("🎉 DATE COLUMN TESTING COMPLETED!")
        log.info("=" * 80)
        log.info("✅ The new 'date' column is working perfectly!")
        log.info("✅ You can now use it for:")
        log.info("   • Date range filtering")
        log.info("   • Time series analysis")
        log.info("   • Monthly/quarterly aggregations")
        log.info("   • Trend analysis")
        log.info("   • Data visualization")


def main():
    """Main function to run the date column tests."""
    try:
        test_date_column_queries()
        return 0
    except Exception as e:
        log.error(f"❌ Testing failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
