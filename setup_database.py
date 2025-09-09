#!/usr/bin/env python3
"""
Database setup script for BLS data repository.
Run this script to initialize the database and create tables.
"""

import os
import sys
from pathlib import Path

# Add the current directory to the Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from bls_logging.config import get_logger, setup_logging
from database.utils import (
    create_database_indexes,
    load_initial_series_metadata,
    setup_database,
)

# Setup logging
setup_logging(log_level="INFO", log_dir="logs", console_output=True, file_output=True)
log = get_logger(__name__)


def main():
    """Setup the database with tables and initial data."""
    log.info("Setting up BLS data database...")

    try:
        # Setup database connection and create tables
        log.info("1. Creating database connection and tables...")
        setup_database()
        log.info("✅ Database tables created successfully")

        # Create additional indexes
        log.info("2. Creating database indexes...")
        create_database_indexes()
        log.info("✅ Database indexes created successfully")

        # Load initial CPI series metadata (optional)
        log.info("3. Loading initial CPI series metadata...")
        loaded_count = load_initial_series_metadata()
        if loaded_count > 0:
            log.info(f"✅ Loaded {loaded_count} CPI series metadata records")
        else:
            log.info("⚠️  No CPI series metadata loaded (file not found or empty)")

        log.info("\n🎉 Database setup completed successfully!")
        log.info("\nYou can now use the BLS data extraction with database support:")
        log.info(
            "  python -m bls_data.data_extraction.main cpi_all_items --use-database"
        )

    except Exception as e:
        log.info(f"❌ Database setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
