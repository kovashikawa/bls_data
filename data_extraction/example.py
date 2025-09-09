# example.py
"""
This script provides a working example of how to use the BLS data fetching library.
It demonstrates both traditional API-only usage and the new database integration features
for caching and persistence. The script shows how to call the main function `get_bls_data`
to retrieve economic data for a specified date range using human-readable aliases
defined in `code_mapping.csv`.
"""

import sys
import time
from pathlib import Path

import pandas as pd

# Add the current directory to the Python path
current_dir = Path(__file__).parent.parent
sys.path.insert(0, str(current_dir))

from bls_logging.config import get_logger, setup_logging
from data_extraction.main import get_bls_data

# Setup logging
setup_logging(log_level="INFO", log_dir="logs", console_output=True, file_output=True)
log = get_logger(__name__)


def run_basic_example():
    """
    Runs a basic demonstration of the BLS data fetching functionality without database.

    This function shows the traditional API-only usage, which remains fully functional
    and unchanged from previous versions.
    """
    log.info("=" * 60)
    log.info("BASIC API-ONLY EXAMPLE")
    log.info("=" * 60)

    # Define the series aliases you want to fetch. These aliases are mapped to
    # official BLS series IDs in the 'code_mapping.csv' file.
    series_to_fetch = [
        "cpi_all_items",  # Consumer Price Index for All Urban Consumers
        "ces_all_employees",  # All Employees, Total Nonfarm
        "unemployment_rate",  # Civilian Unemployment Rate
    ]

    # Specify the date range for the data query.
    start_year = 2023
    end_year = 2023

    log.info(f"Attempting to fetch data for: {', '.join(series_to_fetch)}")
    log.info(f"Time period: {start_year}-{end_year}")

    try:
        # Call the main function to get the data. The function handles API requests,
        # data parsing, and returns a clean pandas DataFrame.
        bls_dataframe = get_bls_data(
            codes_or_ids=series_to_fetch,
            start_year=start_year,
            end_year=end_year,
            catalog=True,  # Set to True to include descriptive metadata for each series
            use_database=False,  # Explicitly disable database features
        )

        # Display the fetched data
        if not bls_dataframe.empty:
            log.info("\n" + "=" * 80)
            log.info(
                "Successfully fetched BLS data (API-only mode). Displaying a sample:"
            )
            log.info("=" * 80 + "\n")

            # Use pandas' option_context for cleaner display formatting
            with pd.option_context("display.width", 120, "display.max_columns", None):
                log.info("--- First 10 Rows ---")
                log.info(bls_dataframe.head(10).to_string(index=False))

            log.info(f"\nTotal rows fetched: {len(bls_dataframe)}")
            log.info("Data source: BLS API (no database caching)")
        else:
            log.warning(
                "The request was successful, but no data was returned for the specified series and years."
            )

    except KeyError as e:
        # This error occurs if an alias in `series_to_fetch` is not found in the mapping file.
        log.error(f"Error resolving series ID: {e}")
        log.error(
            "Please ensure all requested aliases exist in your 'code_mapping.csv' file."
        )
    except RuntimeError as e:
        # This error typically indicates a problem with the API request itself.
        log.error(f"An API error occurred: {e}")
    except Exception as e:
        # Catch any other unexpected errors.
        log.error(f"An unexpected error occurred: {e}")


def run_database_example():
    """
    Runs a demonstration of the BLS data fetching functionality with database integration.

    This function shows the new database features including caching, persistence,
    and performance improvements.
    """
    log.info("\n" + "=" * 60)
    log.info("DATABASE INTEGRATION EXAMPLE")
    log.info("=" * 60)

    # Define the series aliases you want to fetch
    series_to_fetch = [
        "cpi_all_items",  # Consumer Price Index for All Urban Consumers
        "ces_all_employees",  # All Employees, Total Nonfarm
        "unemployment_rate",  # Civilian Unemployment Rate
    ]

    start_year = 2023
    end_year = 2023

    log.info(f"Attempting to fetch data for: {', '.join(series_to_fetch)}")
    log.info(f"Time period: {start_year}-{end_year}")

    try:
        # First run - fetch from API and store in database
        log.info("\n1. First run (API + Database Storage)...")
        start_time = time.time()

        bls_dataframe_api = get_bls_data(
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
        log.info(f"   Data shape: {bls_dataframe_api.shape}")
        log.info("   Data source: BLS API (stored in database)")

        # Second run - use database cache
        log.info("\n2. Second run (Database Cache)...")
        start_time = time.time()

        bls_dataframe_cache = get_bls_data(
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
        log.info(f"   Data shape: {bls_dataframe_cache.shape}")
        log.info("   Data source: Database cache")

        # Performance comparison
        if api_time > 0:
            speedup = api_time / cache_time
            log.info(
                f"\n🚀 Performance improvement: {speedup:.1f}x faster with caching"
            )

        # Data consistency check
        log.info("\n📊 Data consistency check:")
        log.info(f"   Data identical: {bls_dataframe_api.equals(bls_dataframe_cache)}")
        log.info(
            f"   Same number of rows: {len(bls_dataframe_api) == len(bls_dataframe_cache)}"
        )

        # Display sample data
        if not bls_dataframe_cache.empty:
            log.info("\n" + "=" * 80)
            log.info("Sample of cached data:")
            log.info("=" * 80 + "\n")

            with pd.option_context("display.width", 120, "display.max_columns", None):
                log.info("--- First 10 Rows ---")
                log.info(bls_dataframe_cache.head(10).to_string(index=False))

            log.info(f"\nTotal rows: {len(bls_dataframe_cache)}")
            log.info(f"Unique series: {bls_dataframe_cache['series_id'].nunique()}")
            log.info(
                f"Date range: {bls_dataframe_cache['year'].min()}-{bls_dataframe_cache['year'].max()}"
            )

    except KeyError as e:
        log.error(f"Error resolving series ID: {e}")
        log.error(
            "Please ensure all requested aliases exist in your 'code_mapping.csv' file."
        )
    except RuntimeError as e:
        log.error(f"An API error occurred: {e}")
    except Exception as e:
        log.error(f"An unexpected error occurred: {e}")


def run_example():
    """
    Runs demonstrations of both basic and database-enhanced BLS data fetching.

    This function shows both the traditional API-only usage and the new database
    integration features, allowing users to see the differences and benefits.
    """
    log.info("📊 BLS DATA EXTRACTION EXAMPLES")
    log.info("=" * 60)

    # Check if database is available
    try:
        from database.config import DatabaseConfig

        db_config = DatabaseConfig()
        database_available = db_config.check_connection()
    except Exception:
        database_available = False

    if database_available:
        log.info("✅ Database connection available - will demonstrate both modes")
    else:
        log.info("⚠️  Database not available - will demonstrate API-only mode")
        log.info(
            "   To enable database features, ensure PostgreSQL is running and configured"
        )

    # Run basic example (always works)
    run_basic_example()

    # Run database example (only if database is available)
    if database_available:
        run_database_example()

        log.info("\n" + "=" * 60)
        log.info("🎉 EXAMPLES COMPLETED!")
        log.info("=" * 60)
        log.info("\nKey benefits of database integration:")
        log.info("• 🚀 Faster subsequent data requests (caching)")
        log.info("• 💾 Persistent data storage")
        log.info("• 📊 Data quality tracking")
        log.info("• 🔄 Automatic fallback to API if database unavailable")
        log.info("• 📈 Performance monitoring and statistics")

        log.info("\nNext steps:")
        log.info(
            "• Try the command line interface: python -m bls_data.data_extraction.main cpi_all_items --use-database"
        )
        log.info("• Explore database examples: python bls_data/database/example.py")
        log.info("• Check database statistics and monitoring features")
    else:
        log.info("\n" + "=" * 60)
        log.info("📝 BASIC EXAMPLE COMPLETED!")
        log.info("=" * 60)
        log.info("\nTo enable database features:")
        log.info("1. Install PostgreSQL and create a database")
        log.info("2. Configure your .env file with database credentials")
        log.info("3. Run: python bls_data/setup_database.py")
        log.info("4. Re-run this example to see database features")


if __name__ == "__main__":
    run_example()
