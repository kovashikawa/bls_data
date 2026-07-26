"""CPI series code helpers — read from cpi_series_master_list.csv."""

from pathlib import Path
from typing import Optional

import pandas as pd

_MASTER_PATH = Path(__file__).parent.parent.parent / "cu_series" / "cpi_series_master_list.csv"


def get_cu_series_codes(filters: Optional[dict[str, str]] = None) -> list[str]:
    """Return CPI series IDs from the master list, optionally filtered."""
    df = pd.read_csv(_MASTER_PATH, dtype=str)
    if filters:
        mask = pd.Series(True, index=df.index)
        for col, val in filters.items():
            mask &= df[col] == val
        df = df[mask]
    return df["series_id"].tolist()