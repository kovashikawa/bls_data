# download_and_parse_bls_metadata.py (Simplified)
"""
This script downloads and parses the master metadata files for the Consumer Price
Index (CPI) from the BLS website to create a single, comprehensive series list.

It calls the system's `curl` command in a subprocess to bypass potential Python
SSL/TLS issues, processes the data in memory, merges the files, and saves the
final output to 'cpi_series_master_list.csv'.
"""

import logging
import io
from pathlib import Path
import pandas as pd
import subprocess  # Import the subprocess module

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# Define the BLS URLs and corresponding column headers
BLS_CPI_BASE_URL = "https://download.bls.gov/pub/time.series/cu/"
METADATA_CONFIG = {
    "series": {
        "url": f"{BLS_CPI_BASE_URL}cu.series",
        "headers": [
            "series_id", "area_code", "item_code", "seasonality_code",
            "periodicity_code", "base_code", "base_period", "series_title",
            "footnote_codes", "begin_year", "begin_period", "end_year", "end_period"
        ]
    },
    "item": {
        "url": f"{BLS_CPI_BASE_URL}cu.item",
        "headers": ["item_code", "item_name", "display_level", "selectable", "sort_sequence"]
    },
    "area": {
        "url": f"{BLS_CPI_BASE_URL}cu.area",
        "headers": ["area_code", "area_name", "display_level", "selectable", "sort_sequence"]
    }
}

def main():
    """
    Downloads, parses, and merges BLS CPI metadata into a single master list.
    """
    final_csv_path = Path("cpi_series_master_list.csv")
    dataframes = {}

    # The User-Agent you successfully used with curl
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"

    try:
        # --- Step 1: Download Each Metadata File using curl ---
        for key, config in METADATA_CONFIG.items():
            logging.info(f"Requesting data from {config['url']} using curl...")
            
            # --- FIX: Call the curl command directly from Python ---
            command = [
                "curl",
                "-A", user_agent,  # Set the User-Agent
                "-s",              # Silent mode (don't show progress)
                "-L",              # Follow redirects
                config['url']
            ]
            
            # Execute the command. capture_output=True saves the output to result.stdout.
            # text=True decodes it as text. check=True raises an error if curl fails.
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            
            # Use the captured text output from curl
            text_data = io.StringIO(result.stdout)
            
            df = pd.read_csv(
                text_data,
                sep='\t',
                dtype=str
            ).apply(lambda x: x.str.strip() if x.dtype == "object" else x)

            df.columns = df.columns.str.strip()
            
            dataframes[key] = df
            logging.info(f"Successfully parsed '{Path(config['url']).name}'.")

        # --- Step 2: Merge the DataFrames into a Single Master List ---
        logging.info("Merging dataframes...")
        master_df = pd.merge(
            dataframes['series'],
            dataframes['area'][['area_code', 'area_name']],
            on='area_code',
            how='left'
        )
        master_df = pd.merge(
            master_df,
            dataframes['item'][['item_code', 'item_name']],
            on='item_code',
            how='left'
        )

        # --- Step 3: Save the Final Result ---
        master_df.to_csv(final_csv_path, index=False)
        
        logging.info(f"\nSUCCESS: Master CPI series list created at '{final_csv_path}'")
        logging.info(f"Total series found: {len(master_df):,}")

    except FileNotFoundError:
        logging.error("Error: 'curl' command not found. Please ensure curl is installed and in your system's PATH.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing curl: {e}")
        logging.error(f"Curl stderr: {e.stderr}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
