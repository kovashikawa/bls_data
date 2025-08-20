import os
import random
import requests
import pandas as pd
import json
import time
import logging
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional

from data_extraction.api_key import get_random_bls_key

# --- Configuration Constants ---
# As per BLS API v2 documentation for registered users
MAX_SERIES_PER_QUERY = 50
MAX_YEARS_PER_QUERY = 20
# Concurrency settings
MAX_WORKERS = 5  # Number of parallel threads to fetch data
# Retry settings for failed requests
RETRY_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 2

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_batch(series_ids: List[str], start_year: int, end_year: int, api_key: str) -> List[Dict]:
    """
    Fetches a single batch of series data from the BLS API with retries.

    Args:
        series_ids: A list of series IDs for this batch (up to 50).
        start_year: The first year of data to retrieve.
        end_year: The last year of data to retrieve.
        api_key: The BLS API key to use for the request.

    Returns:
        A list of dictionaries containing the processed data points, or an empty list if the request fails.
    """
    headers = {'Content-type': 'application/json'}
    data = json.dumps({
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": api_key,
        "annualaverage": "true"
    })
    processed_data = []

    for attempt in range(RETRY_ATTEMPTS):
        try:
            response = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers, timeout=30)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

            json_data = response.json()
            if json_data['status'] != 'REQUEST_SUCCEEDED':
                logging.warning(f"BLS API returned an error for batch {series_ids[0]}...: {json_data.get('message')}")
                time.sleep(RETRY_DELAY_SECONDS)
                continue

            # If successful, parse and return the data immediately
            for series in json_data['Results']['series']:
                series_id = series['seriesID']
                for item in series['data']:
                    if item['period'] == 'M13':  # M13 is the annual average
                        processed_data.append({
                            'year': int(item['year']),
                            'series_id': series_id,
                            'value': float(item['value'])
                        })

        except requests.exceptions.RequestException as e:
            logging.warning(f"Request failed for batch {series_ids[0]}... (Attempt {attempt + 1}/{RETRY_ATTEMPTS}): {e}")
            time.sleep(RETRY_DELAY_SECONDS)

    return processed_data


def fetch_bls_data_optimized(series_map: Dict[str, str], start_year: int, end_year: int) -> Optional[pd.DataFrame]:
    """
    Orchestrates the fetching of BLS data using concurrent batch requests.

    Args:
        series_map: A dictionary mapping Series IDs to friendly names.
        start_year: The starting year for the data query.
        end_year: The ending year for the data query.

    Returns:
        A pivoted pandas DataFrame with years as the index and series names as columns,
        or None if the process fails.
    """
    if (end_year - start_year + 1) > MAX_YEARS_PER_QUERY:
        logging.error(f"Date range cannot exceed {MAX_YEARS_PER_QUERY} years. Requested: {end_year - start_year + 1} years.")

    all_series_ids = list(series_map.keys())
    # Automatically create batches of series IDs based on the API limit
    batches = [
        all_series_ids[i:i + MAX_SERIES_PER_QUERY] 
        for i in range(0, len(all_series_ids), MAX_SERIES_PER_QUERY)
    ]
    
    logging.info(f"Fetching {len(all_series_ids)} series in {len(batches)} batches using up to {MAX_WORKERS} workers.")
    
    all_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all batch jobs to the thread pool
        future_to_batch = {
            executor.submit(fetch_batch, batch, start_year, end_year, get_random_bls_key()): batch
            for batch in batches
        }
        
        for future in as_completed(future_to_batch):
            batch_series = future_to_batch[future]
            try:
                data = future.result()
                if data:
                    all_results.extend(data)
                    logging.info(f"Successfully completed batch starting with {batch_series[0]}.")
            except Exception as exc:
                logging.error(f"Batch starting with {batch_series[0]} generated an exception: {exc}")

    if not all_results:
        logging.warning("No data was returned from the API.")
        return None

    # Create DataFrame from all collected results
    df = pd.DataFrame(all_results)
    df['series_name'] = df['series_id'].map(series_map)

    # Pivot the table for the final desired format
    pivot_df = df.pivot(index='year', columns='series_name', values='value')
    
    # Ensure a logical column order
    ordered_cols = [name for name in series_map.values() if name in pivot_df.columns]
    return pivot_df[ordered_cols]


# --- Main Execution ---
if __name__ == "__main__":
    # Define all the series you want to fetch here
    # The script will automatically handle batching them.
    SERIES_TO_FETCH = {
        'CUSR0000SA0R': 'All_items',
        'CUSR0000SARF': 'Food',
        'CUSR0000SARE': 'Energy',
        'CUSR0000SARCR': 'Commodities_less_food_energy',
        'CUSR0000SARSR': 'Services_less_energy',
        'CUSR0000SARH': 'Housing',
        'CUSR0000SART': 'Transportation',
        'CUSR0000SARM': 'Medical_care'
    }

    # Define the time period
    from datetime import date
    current_year = date.today().year
    START_YEAR = 2005
    END_YEAR = current_year - 1 # Use last completed year

    start_time = time.perf_counter()
    
    # Fetch the data using the optimized function
    weights_df = fetch_bls_data_optimized(
        series_map=SERIES_TO_FETCH,
        start_year=START_YEAR,
        end_year=END_YEAR
    )
    
    end_time = time.perf_counter()
    logging.info(f"Total execution time: {end_time - start_time:.2f} seconds.")

    if weights_df is not None:
        print("\n--- CPI Relative Importance (Weights) ---")
        with pd.option_context('display.max_rows', None):
            print(weights_df)
            
        # Example of saving to a CSV file
        # weights_df.to_csv('bls_relative_weights.csv')
        # logging.info("Data saved to bls_relative_weights.csv")
