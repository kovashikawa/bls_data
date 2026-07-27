"""CPI series code helpers — read from cpi_series_master_list.csv."""

import os
from pathlib import Path
from typing import Optional

import pandas as pd

# Resolve master list path: package data > project root > env override
_PKG_DATA = Path(__file__).parent / "data" / "cpi_series_master_list.csv"
_REPO_DATA = Path(__file__).parent.parent.parent / "cu_series" / "cpi_series_master_list.csv"
_MASTER_PATH = Path(os.environ.get("BLS_CPI_MASTER_LIST", _PKG_DATA if _PKG_DATA.exists() else _REPO_DATA))


def get_cu_series_codes(filters: Optional[dict[str, str]] = None) -> list[str]:
    """Return CPI series IDs from the master list, optionally filtered."""
    df = pd.read_csv(_MASTER_PATH, dtype=str)
    if filters:
        mask = pd.Series(True, index=df.index)
        for col, val in filters.items():
            mask &= df[col] == val
        df = df[mask]
    return df["series_id"].tolist()