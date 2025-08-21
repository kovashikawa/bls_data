import pandas as pd
from pathlib import Path
from typing import Optional

def get_cu_series_codes(filters: Optional[dict[str, str]] = None) -> list[str]:
    """
    Returns a list of CU series IDs from the CPI master list, with optional filters.
    
    Parameters
    ----------
    filters : dict[str, str], optional
        Dictionary mapping column names to values (e.g. {"area_code": "0000", "item_code": "SA0"}).
        All conditions must be met (AND).
    
    Returns
    -------
    list[str]
        List of CPI series IDs matching the filters.
    """
    
    # Build the path to the CSV file relative to the location of this script.
    file_path = Path(__file__).parent / "cpi_series_master_list.csv"
    cu_series_df = pd.read_csv(file_path, dtype=str)  # ensure codes stay as strings
    
    if filters:
        mask = pd.Series(True, index=cu_series_df.index)
        for col, val in filters.items():
            mask &= cu_series_df[col] == val
        cu_series_df = cu_series_df[mask]
    
    return cu_series_df['series_id'].tolist()


# Example usage:
if __name__ == "__main__":
    # U.S. city average, All items
    codes = get_cu_series_codes({"area_code": "0000", "item_code": "SA0"})
    print(codes[:5])   # show first 5
    
    # Just by area
    codes = get_cu_series_codes({"area_code": "S49G"})  # Urban Alaska
    print(len(codes))
    
    # No filter (all series)
    codes = get_cu_series_codes()
    print(len(codes))
