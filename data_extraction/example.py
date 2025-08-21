# example.py
"""
This script provides a working example of how to use the refactored BLS data fetching library.
It demonstrates how to call the main function `get_bls_data` to retrieve economic data
for a specified date range using human-readable aliases defined in `code_mapping.csv`.
"""

import logging
import pandas as pd
from main import get_bls_data

# Configure logging to display informative messages
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

def run_example():
    """
    Runs a demonstration of the BLS data fetching functionality.
    
    This function defines a list of economic series aliases, specifies a date range,
    and then calls the `get_bls_data` function to retrieve the data. It handles
    potential errors, such as unresolved series aliases, and prints the resulting
    DataFrame to the console.
    """
    # Define the series aliases you want to fetch. These aliases are mapped to
    # official BLS series IDs in the 'code_mapping.csv' file.
    series_to_fetch = [
        "cpi_all_items",      # Consumer Price Index for All Urban Consumers
        "ces_all_employees",  # All Employees, Total Nonfarm
        "unemployment_rate"   # Civilian Unemployment Rate
    ]
    
    # Specify the date range for the data query.
    start_year = 2018
    end_year = 2023
    
    logging.info(f"Attempting to fetch data for: {', '.join(series_to_fetch)}")
    logging.info(f"Time period: {start_year}-{end_year}")

    try:
        # Call the main function to get the data. The function handles API requests,
        # data parsing, and returns a clean pandas DataFrame.
        bls_dataframe = get_bls_data(
            codes_or_ids=series_to_fetch,
            start_year=start_year,
            end_year=end_year,
            catalog=True  # Set to True to include descriptive metadata for each series
        )
        
        # Display the fetched data. We'll show the first 15 and last 15 rows
        # to give a snapshot of the entire date range.
        if not bls_dataframe.empty:
            print("\n" + "="*80)
            print("Successfully fetched BLS data. Displaying a sample of the results:")
            print("="*80 + "\n")
            
            # Use pandas' option_context for cleaner display formatting
            with pd.option_context('display.width', 120, 'display.max_columns', None):
                print("--- First 15 Rows ---")
                print(bls_dataframe.head(15).to_string(index=False))
                print("\n... (omitting intermediate rows) ...\n")
                print("--- Last 15 Rows ---")
                print(bls_dataframe.tail(15).to_string(index=False))
            
            print(f"\nTotal rows fetched: {len(bls_dataframe)}")
        else:
            logging.warning("The request was successful, but no data was returned for the specified series and years.")

    except KeyError as e:
        # This error occurs if an alias in `series_to_fetch` is not found in the mapping file.
        logging.error(f"Error resolving series ID: {e}")
        logging.error("Please ensure all requested aliases exist in your 'code_mapping.csv' file.")
    except RuntimeError as e:
        # This error typically indicates a problem with the API request itself.
        logging.error(f"An API error occurred: {e}")
    except Exception as e:
        # Catch any other unexpected errors.
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    run_example()
