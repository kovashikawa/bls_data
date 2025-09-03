#!/usr/bin/env python3
"""
Extract all CPI data for U.S. city average area from BLS API.

This script efficiently extracts all Consumer Price Index (CPI) data
for the U.S. city average area (area_code = '0000') using the database
for caching and performance optimization.
"""

import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd

# Add the current directory to the Python path (for executability)
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from cu_series.cu_series_codes import get_cu_series_codes
from data_extraction.main import get_bls_data
from bls_logging.config import setup_logging, get_logger

# Setup logging
setup_logging(log_level="INFO", log_dir="logs", console_output=True, file_output=True)
log = get_logger(__name__)


def get_us_city_avg_series() -> List[str]:
    """
    Get all CPI series IDs for U.S. city average area.
    
    Returns:
        List of series IDs for U.S. city average CPI data
    """
    log.info("🔍 Retrieving CPI series for U.S. city average...")
    
    # Get all series for U.S. city average (area_code = '0000')
    series_ids = get_cu_series_codes({'area_code': '0000'})
    
    log.info(f"✅ Found {len(series_ids)} CPI series for U.S. city average")
    return series_ids


def extract_cpi_data_in_batches(series_ids: List[str], 
                               start_year: Optional[int] = None, 
                               end_year: Optional[int] = None,
                               batch_size: int = 50,
                               use_database: bool = True) -> pd.DataFrame:
    """
    Extract CPI data in batches to respect API limits.
    
    Args:
        series_ids: List of CPI series IDs to extract
        start_year: Start year for data extraction
        end_year: End year for data extraction
        batch_size: Number of series to process per batch (max 50 for BLS API)
        use_database: Whether to use database for caching
        
    Returns:
        Combined DataFrame with all extracted data
    """
    log.info(f"📊 Starting CPI data extraction for {len(series_ids)} series")
    if start_year and end_year:
        log.info(f"📅 Date range: {start_year}-{end_year}")
    else:
        log.info(f"📅 Date range: ALL AVAILABLE DATA (no year limits)")
    log.info(f"🔄 Batch size: {batch_size}")
    log.info(f"💾 Database caching: {'Enabled' if use_database else 'Disabled'}")
    
    all_data = []
    total_batches = (len(series_ids) + batch_size - 1) // batch_size
    
    for i in range(0, len(series_ids), batch_size):
        batch_num = (i // batch_size) + 1
        batch_series = series_ids[i:i + batch_size]
        
        log.info(f"\\n🔄 Processing batch {batch_num}/{total_batches} ({len(batch_series)} series)")
        log.info(f"   Series range: {batch_series[0]} to {batch_series[-1]}")
        
        try:
            start_time = time.time()
            
            # Extract data for this batch
            batch_data = get_bls_data(
                codes_or_ids=batch_series,
                start_year=start_year,
                end_year=end_year,
                catalog=True,  # Include metadata
                use_database=use_database,
                use_cache=True,
                force_refresh=False
            )
            
            extraction_time = time.time() - start_time
            
            if not batch_data.empty:
                all_data.append(batch_data)
                log.info(f"   ✅ Extracted {len(batch_data)} rows in {extraction_time:.2f} seconds")
                log.info(f"   📈 Average: {len(batch_data)/len(batch_series):.1f} rows per series")
            else:
                log.warning(f"   ⚠️  No data returned for batch {batch_num}")
            
            # Add small delay to be respectful to the API
            if batch_num < total_batches:
                time.sleep(1)
                
        except Exception as e:
            log.error(f"   ❌ Error processing batch {batch_num}: {e}", exc_info=True)
            continue
    
    if all_data:
        # Combine all batches
        combined_data = pd.concat(all_data, ignore_index=True)
        log.info(f"\\n🎉 Extraction completed!")
        log.info(f"   📊 Total rows: {len(combined_data)}")
        log.info(f"   📈 Unique series: {combined_data['series_id'].nunique()}")
        log.info(f"   📅 Date range: {combined_data['year'].min()}-{combined_data['year'].max()}")
        return combined_data
    else:
        log.error("❌ No data was successfully extracted")
        return pd.DataFrame()


def save_data_to_csv(data: pd.DataFrame, filename: str = None) -> str:
    """
    Save extracted data to CSV file.
    
    Args:
        data: DataFrame to save
        filename: Output filename (optional)
        
    Returns:
        Path to saved file
    """
    if filename is None:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"cpi_us_city_avg_{timestamp}.csv"
    
    output_path = Path("data") / filename
    output_path.parent.mkdir(exist_ok=True)
    
    log.info(f"💾 Saving data to {output_path}")
    data.to_csv(output_path, index=False)
    
    log.info(f"✅ Data saved successfully!")
    log.info(f"   📁 File: {output_path}")
    log.info(f"   📊 Rows: {len(data)}")
    log.info(f"   📈 Columns: {len(data.columns)}")
    
    return str(output_path)


def main():
    """Main function to extract all CPI data for U.S. city average."""
    log.info("=" * 80)
    log.info("🏛️  BLS CPI DATA EXTRACTION - U.S. CITY AVERAGE")
    log.info("=" * 80)
    
    try:
        # Step 1: Get all U.S. city average CPI series
        series_ids = get_us_city_avg_series()
        
        if not series_ids:
            log.error("❌ No CPI series found for U.S. city average")
            return 1
        
        # Step 2: Extract data in batches
        log.info("\\n" + "=" * 60)
        log.info("📊 DATA EXTRACTION")
        log.info("=" * 60)
        
        # Extract comprehensive historical data
        # BLS API doesn't return all data when no year limits are specified
        # Use a reasonable historical range that covers most CPI data
        data = extract_cpi_data_in_batches(
            series_ids=series_ids,
            start_year=1984,  # Start from 1984 (40 years of data)
            end_year=2024,    # Current year
            batch_size=50,    # BLS API limit
            use_database=True  # Enable caching for performance
        )
        
        if data.empty:
            log.error("❌ No data was extracted")
            return 1
        
        # Step 3: Save data
        log.info("\\n" + "=" * 60)
        log.info("💾 SAVING DATA")
        log.info("=" * 60)
        
        output_file = save_data_to_csv(data)
        
        # Step 4: Summary statistics
        log.info("\\n" + "=" * 60)
        log.info("📈 EXTRACTION SUMMARY")
        log.info("=" * 60)
        
        log.info(f"✅ Successfully extracted CPI data for U.S. city average")
        log.info(f"   📊 Total data points: {len(data):,}")
        log.info(f"   📈 Unique series: {data['series_id'].nunique():,}")
        log.info(f"   📅 Date range: {data['year'].min()}-{data['year'].max()}")
        log.info(f"   📁 Output file: {output_file}")
        
        # Show sample of data
        log.info("\\n📋 Sample of extracted data:")
        sample_data = data.head(10)[['series_id', 'year', 'period', 'value', 'series_title']]
        log.info(f"\\n{sample_data.to_string(index=False)}")
        
        log.info("\\n" + "=" * 80)
        log.info("🎉 CPI DATA EXTRACTION COMPLETED SUCCESSFULLY!")
        log.info("=" * 80)
        
        return 0
        
    except Exception as e:
        log.error(f"❌ Extraction failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
