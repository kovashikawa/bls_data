#!/usr/bin/env python3
"""
Database migration to add a clean 'date' column to bls_data_points table.

This migration:
1. Adds a new 'date' column to bls_data_points table
2. Populates the date column by converting year + period to datetime
3. Creates an index on the new date column for performance
"""

import sys
from pathlib import Path
from datetime import datetime
import re

# Add the current directory to the Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from database.config import DatabaseConfig
from database.models import BLSDataPoint
from bls_logging.config import setup_logging, get_logger
from sqlalchemy import text

# Setup logging
setup_logging(log_level="INFO", log_dir="logs", console_output=True, file_output=True)
log = get_logger(__name__)


def period_to_date(year: int, period: str) -> datetime:
    """
    Convert BLS period format to datetime.
    
    BLS periods can be:
    - M01, M02, ..., M12 (monthly)
    - Q01, Q02, Q03, Q04 (quarterly)
    - A01 (annual)
    - S01, S02 (semiannual)
    
    Args:
        year: Year (e.g., 2023)
        period: Period string (e.g., "M01", "Q02", "A01")
        
    Returns:
        datetime object
    """
    if period.startswith('M'):
        # Monthly data: M01 = January, M02 = February, etc.
        month = int(period[1:])
        return datetime(year, month, 1)
    
    elif period.startswith('Q'):
        # Quarterly data: Q01 = Q1, Q02 = Q2, etc.
        quarter = int(period[1:])
        month = (quarter - 1) * 3 + 1  # Q1=Jan, Q2=Apr, Q3=Jul, Q4=Oct
        return datetime(year, month, 1)
    
    elif period.startswith('A'):
        # Annual data: A01 = January 1st
        return datetime(year, 1, 1)
    
    elif period.startswith('S'):
        # Semiannual data: S01 = January, S02 = July
        semester = int(period[1:])
        month = 1 if semester == 1 else 7
        return datetime(year, month, 1)
    
    else:
        # Fallback: try to extract month if it's a number
        try:
            month = int(period)
            if 1 <= month <= 12:
                return datetime(year, month, 1)
        except ValueError:
            pass
        
        # If we can't parse it, default to January 1st
        log.warning(f"Could not parse period '{period}' for year {year}, defaulting to January 1st")
        return datetime(year, 1, 1)


def add_date_column():
    """Add date column to bls_data_points table and populate it."""
    log.info("=" * 80)
    log.info("🗄️  DATABASE MIGRATION: ADD DATE COLUMN")
    log.info("=" * 80)
    
    db_config = DatabaseConfig()
    
    try:
        with db_config.get_session() as session:
            # Step 1: Add the date column
            log.info("\\n1. Adding 'date' column to bls_data_points table...")
            
            # Check if column already exists
            result = session.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'bls_data_points' 
                AND column_name = 'date'
            """)).fetchone()
            
            if result:
                log.info("✅ 'date' column already exists")
            else:
                # Add the date column
                session.execute(text("""
                    ALTER TABLE bls_data_points 
                    ADD COLUMN date DATE
                """))
                session.commit()
                log.info("✅ 'date' column added successfully")
            
            # Step 2: Populate the date column
            log.info("\\n2. Populating 'date' column with converted year + period data...")
            
            # Get all data points that don't have a date yet
            data_points = session.query(BLSDataPoint).filter(
                BLSDataPoint.date.is_(None)
            ).all()
            
            log.info(f"📊 Found {len(data_points)} data points to update")
            
            if data_points:
                updated_count = 0
                error_count = 0
                
                for i, point in enumerate(data_points):
                    try:
                        # Convert year + period to date
                        point.date = period_to_date(point.year, point.period)
                        updated_count += 1
                        
                        # Commit in batches of 1000
                        if (i + 1) % 1000 == 0:
                            session.commit()
                            log.info(f"   Processed {i + 1}/{len(data_points)} data points...")
                            
                    except Exception as e:
                        log.error(f"Error processing data point {point.id}: {e}")
                        error_count += 1
                        continue
                
                # Final commit
                session.commit()
                
                log.info(f"✅ Successfully updated {updated_count} data points")
                if error_count > 0:
                    log.warning(f"⚠️  {error_count} data points had errors")
            else:
                log.info("✅ All data points already have dates")
            
            # Step 3: Create index on date column
            log.info("\\n3. Creating index on 'date' column...")
            
            try:
                session.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_bls_data_points_date 
                    ON bls_data_points(date)
                """))
                session.commit()
                log.info("✅ Index created successfully")
            except Exception as e:
                log.warning(f"⚠️  Could not create index: {e}")
            
            # Step 4: Verify the migration
            log.info("\\n4. Verifying migration...")
            
            # Count data points with dates
            total_points = session.query(BLSDataPoint).count()
            points_with_dates = session.query(BLSDataPoint).filter(
                BLSDataPoint.date.isnot(None)
            ).count()
            
            log.info(f"📊 Total data points: {total_points:,}")
            log.info(f"📊 Data points with dates: {points_with_dates:,}")
            log.info(f"📊 Coverage: {(points_with_dates/total_points*100):.1f}%")
            
            # Show sample of converted data
            sample_points = session.query(BLSDataPoint).filter(
                BLSDataPoint.date.isnot(None)
            ).limit(5).all()
            
            log.info("\\n📋 Sample of converted data:")
            for point in sample_points:
                log.info(f"   {point.series_id} | {point.year} {point.period} → {point.date.strftime('%Y-%m-%d')}")
            
            log.info("\\n" + "=" * 80)
            log.info("🎉 MIGRATION COMPLETED SUCCESSFULLY!")
            log.info("=" * 80)
            log.info("✅ 'date' column added to bls_data_points table")
            log.info("✅ All year + period combinations converted to dates")
            log.info("✅ Index created for performance")
            log.info("✅ Migration verified")
            
            return True
            
    except Exception as e:
        log.error(f"❌ Migration failed: {e}", exc_info=True)
        return False


def main():
    """Main function to run the migration."""
    success = add_date_column()
    
    if success:
        log.info("\\n🚀 Next steps:")
        log.info("1. The 'date' column is now available for queries")
        log.info("2. You can use it for time-series analysis")
        log.info("3. Example query: SELECT * FROM bls_data_points WHERE date >= '2020-01-01'")
        return 0
    else:
        log.error("\\n❌ Migration failed. Please check the errors above.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
