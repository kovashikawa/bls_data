# bls_data/database/utils.py

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from .config import DatabaseConfig
from .models import Base

log = logging.getLogger("bls")


def setup_database(database_url: str = None) -> DatabaseConfig:
    """Setup and initialize the database"""
    try:
        db_config = DatabaseConfig(database_url)

        # Test connection
        if not db_config.check_connection():
            raise ConnectionError("Cannot connect to database")

        # Create tables
        db_config.create_tables()
        log.info("Database setup completed successfully")

        return db_config

    except Exception as e:
        log.error(f"Database setup failed: {e}")
        raise


def load_initial_series_metadata(csv_path: str = None) -> int:
    """Load initial series metadata from CPI master list CSV"""
    if not csv_path:
        # Look for the CPI master list in the cu_series directory
        csv_path = (
            Path(__file__).parent.parent / "cu_series" / "cpi_series_master_list.csv"
        )

    if not Path(csv_path).exists():
        log.warning(f"CPI master list not found at {csv_path}")
        return 0

    try:
        import pandas as pd

        from .config import DatabaseConfig
        from .models import BLSSeries

        db_config = DatabaseConfig()
        df = pd.read_csv(csv_path)

        with db_config.get_session() as session:
            loaded_count = 0
            for _, row in df.iterrows():
                series = BLSSeries(
                    series_id=row["series_id"],
                    series_title=row["series_title"],
                    survey_name="Consumer Price Index",
                    area=row["area_name"],
                    item=row["item_name"],
                    seasonality=row["seasonal"],
                    begin_year=row["begin_year"],
                    begin_period=row["begin_period"],
                    end_year=row["end_year"],
                    end_period=row["end_period"],
                )
                session.merge(series)  # Use merge to handle duplicates
                loaded_count += 1

            session.commit()
            log.info(f"Loaded {loaded_count} series metadata records")
            return loaded_count

    except Exception as e:
        log.error(f"Error loading initial series metadata: {e}")
        return 0


def create_database_indexes():
    """Create additional database indexes for performance"""
    try:
        from .config import DatabaseConfig

        db_config = DatabaseConfig()

        with db_config.engine.connect() as conn:
            # Create additional indexes for better performance
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_data_points_value_non_null ON bls_data_points(series_id, year, period) WHERE value IS NOT NULL;",
                "CREATE INDEX IF NOT EXISTS idx_series_latest ON bls_series(series_id) WHERE latest = true;",
                "CREATE INDEX IF NOT EXISTS idx_freshness_stale ON bls_data_freshness(series_id) WHERE last_extracted IS NULL OR last_extracted < NOW() - INTERVAL '24 hours';",
            ]

            for index_sql in indexes:
                conn.execute(index_sql)

            conn.commit()
            log.info("Database indexes created successfully")

    except Exception as e:
        log.error(f"Error creating database indexes: {e}")


def get_database_stats() -> Dict:
    """Get database statistics"""
    try:
        from .config import DatabaseConfig
        from .models import BLSDataPoint, BLSExtractionLog, BLSSeries

        db_config = DatabaseConfig()

        with db_config.get_session() as session:
            stats = {
                "total_series": session.query(BLSSeries).count(),
                "total_data_points": session.query(BLSDataPoint).count(),
                "total_extractions": session.query(BLSExtractionLog).count(),
                "latest_extraction": session.query(BLSExtractionLog)
                .order_by(BLSExtractionLog.created_at.desc())
                .first(),
            }

            return stats

    except Exception as e:
        log.error(f"Error getting database stats: {e}")
        return {}


def cleanup_old_data(retention_days: int = 365):
    """Clean up old extraction logs and data"""
    try:
        from .config import DatabaseConfig
        from .models import BLSExtractionLog

        db_config = DatabaseConfig()
        cutoff_date = datetime.utcnow() - timedelta(days=retention_days)

        with db_config.get_session() as session:
            deleted_count = (
                session.query(BLSExtractionLog)
                .filter(BLSExtractionLog.created_at < cutoff_date)
                .delete()
            )

            session.commit()
            log.info(f"Cleaned up {deleted_count} old extraction logs")

    except Exception as e:
        log.error(f"Error cleaning up old data: {e}")


def backup_database(backup_path: str = None):
    """Create a database backup"""
    try:
        from .config import DatabaseConfig

        if not backup_path:
            backup_path = (
                f"bls_data_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
            )

        db_config = DatabaseConfig()
        db_url = db_config.database_url

        # Extract database name from URL
        db_name = db_url.split("/")[-1]

        # Use pg_dump to create backup
        import subprocess

        cmd = ["pg_dump", db_url]
        with open(backup_path, "w") as f:
            subprocess.run(cmd, stdout=f, check=True)

        log.info(f"Database backup created: {backup_path}")

    except Exception as e:
        log.error(f"Error creating database backup: {e}")
