#!/usr/bin/env python3
"""
Database Example for BLS Data Repository

This script demonstrates how to use the database integration features of the BLS data repository.
It shows database setup, data extraction with caching, and various database operations.
"""

import sys
import time
from pathlib import Path
from typing import Any

# Add the parent directory to the Python path
current_dir = Path(__file__).parent.parent
sys.path.insert(0, str(current_dir))

from bls_logging.config import get_logger, setup_logging
from data_extraction.main import get_bls_data
from database.config import DatabaseConfig
from database.repository import BLSDataRepository
from database.utils import (
    create_database_indexes,
    get_database_stats,
    load_initial_series_metadata,
    setup_database,
)

# Setup logging
setup_logging(log_level="INFO", log_dir="logs", console_output=True, file_output=True)
log = get_logger(__name__)


def setup_database_example():
    """Demonstrate database setup and initialization."""
    log.info("=" * 60)
    log.info("DATABASE SETUP EXAMPLE")
    log.info("=" * 60)

    try:
        # Setup database connection and create tables
        log.info("1. Setting up database connection and tables...")
        db_config = setup_database()
        log.info("✅ Database setup completed successfully")

        # Create additional indexes for performance
        log.info("2. Creating database indexes...")
        create_database_indexes()
        log.info("✅ Database indexes created successfully")

        # Load initial CPI series metadata (optional)
        log.info("3. Loading initial CPI series metadata...")
        loaded_count = load_initial_series_metadata()
        if loaded_count > 0:
            log.info(f"✅ Loaded {loaded_count} CPI series metadata records")
        else:
            log.warning("⚠️  No CPI series metadata loaded (file not found or empty)")

        return db_config

    except Exception as e:
        log.error(f"❌ Database setup failed: {e}", exc_info=True)
        return None


def basic_database_operations_example():
    """Demonstrate basic database operations."""
    log.info("\n" + "=" * 60)
    log.info("BASIC DATABASE OPERATIONS EXAMPLE")
    log.info("=" * 60)

    try:
        # Get database configuration
        db_config = DatabaseConfig()

        # Create a repository instance
        with db_config.get_session() as session:
            repo = BLSDataRepository(session)

            # Search for series
            log.info("1. Searching for CPI-related series...")
            search_results = repo.search_series("consumer price index", limit=5)
            log.info(
                f"Found {len(search_results)} series matching 'consumer price index'"
            )
            for series in search_results:
                log.info(f"  - {series['series_id']}: {series['series_title']}")

            # Get data freshness information
            log.info("\n2. Checking data freshness...")
            if search_results:
                series_ids = [s["series_id"] for s in search_results[:3]]
                freshness = repo.get_data_freshness(series_ids)
                for series_id, info in freshness.items():
                    log.info(
                        f"  - {series_id}: Last extracted: {info.get('last_extracted', 'Never')}"
                    )

            # Get stale series
            log.info("\n3. Finding stale series...")
            stale_series = repo.get_stale_series(max_age_hours=24)
            log.info(f"Found {len(stale_series)} stale series (older than 24 hours)")
            if stale_series:
                log.info(f"  Examples: {stale_series[:5]}")

    except Exception as e:
        log.info(f"❌ Database operations failed: {e}")


def data_extraction_with_database_example():
    """Demonstrate data extraction with database caching."""
    log.info("\n" + "=" * 60)
    log.info("DATA EXTRACTION WITH DATABASE CACHING EXAMPLE")
    log.info("=" * 60)

    # Define series to fetch
    series_to_fetch = [
        "cpi_all_items",  # Consumer Price Index for All Urban Consumers
        "ces_all_employees",  # All Employees, Total Nonfarm
        "unemployment_rate",  # Civilian Unemployment Rate
    ]

    start_year = 2023
    end_year = 2023

    log.info(f"Fetching data for: {', '.join(series_to_fetch)}")
    log.info(f"Time period: {start_year}-{end_year}")

    # First run - fetch from API and store in database
    log.info("\n1. First run (API + Database Storage)...")
    start_time = time.time()

    df1 = get_bls_data(
        codes_or_ids=series_to_fetch,
        start_year=start_year,
        end_year=end_year,
        catalog=True,
        use_database=True,
        use_cache=False,  # Force API call
        force_refresh=True,
    )

    api_time = time.time() - start_time
    log.info(f"✅ First run completed in {api_time:.2f} seconds")
    log.info(f"   Data shape: {df1.shape}")
    log.info("   Data source: API (stored in database)")

    # Second run - use database cache
    log.info("\n2. Second run (Database Cache)...")
    start_time = time.time()

    df2 = get_bls_data(
        codes_or_ids=series_to_fetch,
        start_year=start_year,
        end_year=end_year,
        catalog=True,
        use_database=True,
        use_cache=True,  # Use cache
        force_refresh=False,
    )

    cache_time = time.time() - start_time
    log.info(f"✅ Second run completed in {cache_time:.2f} seconds")
    log.info(f"   Data shape: {df2.shape}")
    log.info("   Data source: Database cache")

    # Performance comparison
    if api_time > 0:
        speedup = api_time / cache_time
        log.info(f"\n🚀 Performance improvement: {speedup:.1f}x faster with caching")

    # Data consistency check
    log.info("\n📊 Data consistency check:")
    log.info(f"   Data identical: {df1.equals(df2)}")
    log.info(f"   Same number of rows: {len(df1) == len(df2)}")

    return df1, df2


def database_statistics_example():
    """Demonstrate database statistics and monitoring."""
    log.info("\n" + "=" * 60)
    log.info("DATABASE STATISTICS EXAMPLE")
    log.info("=" * 60)

    try:
        # Get database statistics
        stats = get_database_stats()

        log.info("📊 Database Statistics:")
        for key, value in stats.items():
            if key == "latest_extraction" and value:
                log.info(f"   {key}: {value.extraction_id} ({value.created_at})")
            else:
                log.info(f"   {key}: {value}")

        # Additional repository operations
        db_config = DatabaseConfig()
        with db_config.get_session() as session:
            repo = BLSDataRepository(session)

            # Get data freshness summary
            log.info("\n📈 Data Freshness Summary:")
            freshness_data = repo.get_data_freshness([])  # Get all
            if freshness_data:
                log.info(f"   Total series with freshness data: {len(freshness_data)}")

                # Count by freshness status
                fresh_count = sum(
                    1
                    for info in freshness_data.values()
                    if info.get("last_extracted") is not None
                )
                log.info(f"   Series with extraction history: {fresh_count}")
                log.info(
                    f"   Series never extracted: {len(freshness_data) - fresh_count}"
                )
            else:
                log.info("   No freshness data available yet")

    except Exception as e:
        log.info(f"❌ Database statistics failed: {e}")


def error_handling_example():
    """Demonstrate error handling and fallback scenarios."""
    log.info("\n" + "=" * 60)
    log.info("ERROR HANDLING EXAMPLE")
    log.info("=" * 60)

    # Test with invalid series ID
    log.info("1. Testing with invalid series ID...")
    try:
        df = get_bls_data(
            codes_or_ids=["invalid_series_id"],
            start_year=2023,
            end_year=2023,
            use_database=True,
        )
        log.info(f"   Result: {df.shape} rows (should be empty)")
    except Exception as e:
        log.info(f"   Expected error: {e}")

    # Test database fallback (simulate database unavailable)
    log.info("\n2. Testing database fallback...")
    try:
        # This should work even if database is unavailable
        df = get_bls_data(
            codes_or_ids=["cpi_all_items"],
            start_year=2023,
            end_year=2023,
            use_database=True,  # Will fall back to API if database fails
        )
        log.info(f"   Fallback successful: {df.shape} rows")
    except Exception as e:
        log.info(f"   Fallback failed: {e}")


def advanced_database_operations_example():
    """Demonstrate advanced database operations."""
    log.info("\n" + "=" * 60)
    log.info("ADVANCED DATABASE OPERATIONS EXAMPLE")
    log.info("=" * 60)

    try:
        db_config = DatabaseConfig()
        with db_config.get_session() as session:
            repo = BLSDataRepository(session)

            # Get data from database directly
            log.info("1. Retrieving data directly from database...")
            df_db = repo.get_series_data(
                series_ids=["CUUR0000SA0"],  # CPI All Items
                start_year=2023,
                end_year=2023,
                include_metadata=True,
            )

            if not df_db.empty:
                log.info(f"   Retrieved {len(df_db)} rows from database")
                log.info(f"   Columns: {list(df_db.columns)}")
                log.info("   Sample data:")
                log.info(df_db.head(3).to_string(index=False))
            else:
                log.info(
                    "   No data found in database (try running data extraction first)"
                )

            # Log a test extraction
            log.info("\n2. Logging test extraction...")
            import uuid

            repo.log_extraction(
                extraction_id=str(uuid.uuid4()),
                series_ids=["CUUR0000SA0"],
                start_year=2023,
                end_year=2023,
                status="success",
                records_extracted=12,
                records_updated=0,
                records_inserted=12,
                api_calls_made=1,
                extraction_duration_seconds=2,
                metadata={"test": True, "example": "database_operations"},
            )
            log.info("   ✅ Test extraction logged successfully")

    except Exception as e:
        log.info(f"❌ Advanced database operations failed: {e}")


def main():
    """Run all database examples."""
    log.info("🗄️  BLS DATA DATABASE INTEGRATION EXAMPLES")
    log.info("=" * 60)

    # Check if database is available
    try:
        db_config = DatabaseConfig()
        if not db_config.check_connection():
            log.info(
                "❌ Database connection failed. Please check your database configuration."
            )
            log.info(
                "   Make sure PostgreSQL is running and your .env file is configured correctly."
            )
            return
    except Exception as e:
        log.info(f"❌ Database configuration error: {e}")
        return

    # Run examples
    try:
        # 1. Database setup
        setup_database_example()

        # 2. Basic database operations
        basic_database_operations_example()

        # 3. Data extraction with caching
        data_extraction_with_database_example()

        # 4. Database statistics
        database_statistics_example()

        # 5. Error handling
        error_handling_example()

        # 6. Advanced operations
        advanced_database_operations_example()

        log.info("\n" + "=" * 60)
        log.info("🎉 ALL EXAMPLES COMPLETED SUCCESSFULLY!")
        log.info("=" * 60)
        log.info("\nNext steps:")
        log.info("1. Try the command line interface with database support:")
        log.info(
            "   python -m bls_data.data_extraction.main cpi_all_items --use-database"
        )
        log.info("2. Explore the database directly with your PostgreSQL client")
        log.info("3. Check the database statistics with get_database_stats()")

    except KeyboardInterrupt:
        log.info("\n\n⚠️  Examples interrupted by user")
    except Exception as e:
        log.info(f"\n❌ Examples failed with error: {e}")
        log.exception("Full error details:")


if __name__ == "__main__":
    main()
