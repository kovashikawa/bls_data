#!/usr/bin/env python3
"""
Comprehensive CPI extraction script with flexible year range options.

This script provides multiple extraction strategies for CPI data:
1. Recent data only (default BLS behavior)
2. Historical data with specified range
3. Maximum historical data (1984-2024)
4. Custom year range
"""

import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd

# Add the current directory to the Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from cu_series.cu_series_codes import get_cu_series_codes
from data_extraction.main import get_bls_data
from bls_logging.config import setup_logging, get_logger

# Setup logging
setup_logging(log_level="INFO", log_dir="logs", console_output=True, file_output=True)
log = get_logger(__name__)


def extract_cpi_data_comprehensive(
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    max_series: Optional[int] = None,
    save_to_csv: bool = True,
    strategy: str = "historical"
) -> pd.DataFrame:
    """
    Extract CPI data with comprehensive options.
    
    Args:
        start_year: Start year (None for no limit)
        end_year: End year (None for no limit)
        max_series: Maximum number of series to extract
        save_to_csv: Whether to save results to CSV
        strategy: Extraction strategy ("recent", "historical", "maximum", "custom")
        
    Returns:
        DataFrame with extracted data
    """
    
    # Get all U.S. city average series
    all_series = get_cu_series_codes({'area_code': '0000'})
    
    if max_series:
        series_to_extract = all_series[:max_series]
        log.info(f"📊 Extracting {len(series_to_extract)} series (limited from {len(all_series)} total)")
    else:
        series_to_extract = all_series
        log.info(f"📊 Extracting all {len(series_to_extract)} series")
    
    # Set year range based on strategy
    if strategy == "recent":
        # Use BLS default behavior (recent data only)
        start_year = None
        end_year = None
        log.info("📅 Strategy: Recent data only (BLS default)")
    elif strategy == "historical":
        # Use reasonable historical range
        start_year = start_year or 2000
        end_year = end_year or 2024
        log.info(f"📅 Strategy: Historical data ({start_year}-{end_year})")
    elif strategy == "maximum":
        # Use maximum available historical range
        start_year = 1984
        end_year = 2024
        log.info(f"📅 Strategy: Maximum historical data ({start_year}-{end_year})")
    elif strategy == "custom":
        # Use provided year range
        log.info(f"📅 Strategy: Custom range ({start_year}-{end_year})")
    else:
        log.error(f"❌ Unknown strategy: {strategy}")
        return pd.DataFrame()
    
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
        if not data.empty:
            log.info(f"📅 Date range: {data['year'].min()}-{data['year'].max()}")
            log.info(f"📊 Years covered: {len(data['year'].unique())} years")
        
        if save_to_csv and not data.empty:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            strategy_suffix = f"_{strategy}" if strategy != "custom" else ""
            filename = f"cpi_us_city_avg_{start_year}_{end_year}{strategy_suffix}_{timestamp}.csv"
            
            # Create data directory if it doesn't exist
            Path("data").mkdir(exist_ok=True)
            output_path = Path("data") / filename
            
            data.to_csv(output_path, index=False)
            log.info(f"💾 Data saved to: {output_path}")
        
        return data
        
    except Exception as e:
        log.error(f"❌ Extraction failed: {e}", exc_info=True)
        return pd.DataFrame()


def main():
    """Main function with different extraction strategies."""
    log.info("=" * 80)
    log.info("🏛️  COMPREHENSIVE CPI DATA EXTRACTION")
    log.info("=" * 80)
    
    # Test different strategies with a small sample first
    test_series_limit = 10
    
    log.info("\\n🧪 Testing different extraction strategies...")
    
    # Strategy 1: Recent data only (BLS default)
    log.info("\\n" + "=" * 60)
    log.info("📊 STRATEGY 1: RECENT DATA ONLY")
    log.info("=" * 60)
    
    recent_data = extract_cpi_data_comprehensive(
        max_series=test_series_limit,
        strategy="recent",
        save_to_csv=False
    )
    
    if not recent_data.empty:
        log.info(f"✅ Recent data: {len(recent_data)} rows, {recent_data['year'].min()}-{recent_data['year'].max()}")
    
    # Strategy 2: Historical data (2000-2024)
    log.info("\\n" + "=" * 60)
    log.info("📊 STRATEGY 2: HISTORICAL DATA (2000-2024)")
    log.info("=" * 60)
    
    historical_data = extract_cpi_data_comprehensive(
        max_series=test_series_limit,
        strategy="historical",
        save_to_csv=False
    )
    
    if not historical_data.empty:
        log.info(f"✅ Historical data: {len(historical_data)} rows, {historical_data['year'].min()}-{historical_data['year'].max()}")
    
    # Strategy 3: Maximum historical data (1984-2024)
    log.info("\\n" + "=" * 60)
    log.info("📊 STRATEGY 3: MAXIMUM HISTORICAL DATA (1984-2024)")
    log.info("=" * 60)
    
    max_data = extract_cpi_data_comprehensive(
        max_series=test_series_limit,
        strategy="maximum",
        save_to_csv=False
    )
    
    if not max_data.empty:
        log.info(f"✅ Maximum data: {len(max_data)} rows, {max_data['year'].min()}-{max_data['year'].max()}")
    
    # Summary
    log.info("\\n" + "=" * 60)
    log.info("📈 EXTRACTION SUMMARY")
    log.info("=" * 60)
    
    strategies = [
        ("Recent (BLS default)", recent_data),
        ("Historical (2000-2024)", historical_data),
        ("Maximum (1984-2024)", max_data)
    ]
    
    for strategy_name, data in strategies:
        if not data.empty:
            years_covered = len(data['year'].unique())
            log.info(f"✅ {strategy_name}: {len(data):,} rows, {years_covered} years")
        else:
            log.info(f"❌ {strategy_name}: No data")
    
    log.info("\\n" + "=" * 80)
    log.info("🎯 RECOMMENDATIONS")
    log.info("=" * 80)
    log.info("1. For recent analysis: Use 'recent' strategy")
    log.info("2. For comprehensive analysis: Use 'maximum' strategy (1984-2024)")
    log.info("3. For balanced approach: Use 'historical' strategy (2000-2024)")
    log.info("4. For full extraction: Remove max_series limit and run with desired strategy")
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
