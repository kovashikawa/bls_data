#!/usr/bin/env python3
"""
Custom CPI extraction script - modify parameters as needed.
"""

import sys
from pathlib import Path

# Add the current directory to the Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from cu_series.cu_series_codes import get_cu_series_codes
from data_extraction.main import get_bls_data
from bls_logging.config import setup_logging, get_logger

# Setup logging
setup_logging(log_level="INFO", log_dir="logs", console_output=True, file_output=True)
log = get_logger(__name__)

def extract_cpi_data(start_year=None, end_year=None, max_series=None, save_to_csv=True):
    """
    Extract CPI data with custom parameters.
    
    Args:
        start_year: Start year for extraction
        end_year: End year for extraction  
        max_series: Maximum number of series to extract (None for all)
        save_to_csv: Whether to save results to CSV
    """
    if start_year and end_year:
        log.info(f"🔍 Extracting CPI data for U.S. city average ({start_year}-{end_year})")
    else:
        log.info(f"🔍 Extracting CPI data for U.S. city average (ALL AVAILABLE DATA)")
    
    # Get all U.S. city average series
    all_series = get_cu_series_codes({'area_code': '0000'})
    
    if max_series:
        series_to_extract = all_series[:max_series]
        log.info(f"📊 Extracting {len(series_to_extract)} series (limited from {len(all_series)} total)")
    else:
        series_to_extract = all_series
        log.info(f"📊 Extracting all {len(series_to_extract)} series")
    
    try:
        # Extract data
        data = get_bls_data(
            codes_or_ids=series_to_extract,
            start_year=start_year,
            end_year=end_year,
            catalog=True,  # Include metadata
            use_database=True,  # Enable caching
            use_cache=True
        )
        
        log.info(f"✅ Successfully extracted {len(data)} rows")
        log.info(f"📈 Unique series: {data['series_id'].nunique()}")
        log.info(f"📅 Date range: {data['year'].min()}-{data['year'].max()}")
        
        if save_to_csv:
            import time
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"cpi_us_city_avg_{start_year}_{end_year}_{timestamp}.csv"
            
            # Create data directory if it doesn't exist
            Path("data").mkdir(exist_ok=True)
            output_path = Path("data") / filename
            
            data.to_csv(output_path, index=False)
            log.info(f"💾 Data saved to: {output_path}")
        
        return data
        
    except Exception as e:
        log.error(f"❌ Extraction failed: {e}", exc_info=True)
        return None

if __name__ == "__main__":
    # Customize these parameters as needed:
    
    # Extract ALL available data for first 10 series (for testing)
    data = extract_cpi_data(
        start_year=None,  # No start year limit - get all available data
        end_year=None,    # No end year limit - get all available data
        max_series=10,    # Limit to 10 series for testing
        save_to_csv=True
    )
    
    if data is not None:
        log.info("\\n🎉 Extraction completed successfully!")
        log.info("\\n📋 Sample data:")
        sample = data[['series_id', 'year', 'period', 'value', 'series_title']].head(10)
        log.info(f"\\n{sample.to_string(index=False)}")
    else:
        log.error("\\n❌ Extraction failed!")
        sys.exit(1)
