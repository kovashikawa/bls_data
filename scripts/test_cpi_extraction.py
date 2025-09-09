#!/usr/bin/env python3
"""
Test script to extract a small sample of CPI data for U.S. city average.
"""

import sys
from pathlib import Path

# Add the current directory to the Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from bls_logging.config import get_logger, setup_logging
from cu_series.cu_series_codes import get_cu_series_codes
from data_extraction.main import get_bls_data

# Setup logging
setup_logging(log_level="INFO", log_dir="logs", console_output=True, file_output=True)
log = get_logger(__name__)


def test_cpi_extraction():
    """Test extraction with a small sample of CPI series."""
    log.info("🧪 Testing CPI extraction for U.S. city average...")

    # Get all U.S. city average series
    all_series = get_cu_series_codes({"area_code": "0000"})
    log.info(f"Total U.S. city average series available: {len(all_series)}")

    # Test with first 5 series
    test_series = all_series[:5]
    log.info(f"Testing with {len(test_series)} series: {test_series}")

    try:
        # Extract data for 2023 only
        data = get_bls_data(
            codes_or_ids=test_series,
            start_year=2023,
            end_year=2023,
            catalog=True,
            use_database=True,
            use_cache=True,
        )

        log.info(f"✅ Successfully extracted {len(data)} rows")
        log.info(f"📊 Data shape: {data.shape}")
        log.info(f"📈 Unique series: {data['series_id'].nunique()}")

        # Show sample
        log.info("\\n📋 Sample data:")
        sample = data[["series_id", "year", "period", "value", "series_title"]].head(10)
        log.info(f"\\n{sample.to_string(index=False)}")

        return True

    except Exception as e:
        log.error(f"❌ Test failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = test_cpi_extraction()
    if success:
        log.info("\\n🎉 Test completed successfully!")
        log.info(
            "You can now run the full extraction with: python extract_all_cpi_us_city_avg.py"
        )
    else:
        log.error("\\n❌ Test failed. Please check the errors above.")
        sys.exit(1)
