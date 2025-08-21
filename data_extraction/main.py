# main.py
"""
This is the main script for the BLS data fetching application. It handles command-line arguments,
orchestrates the data fetching process, and outputs the results to a CSV file or the console.
"""
import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional, Union

import pandas as pd

from bls_client import BLSClient
from data_parser import parse_results_to_df
from mapping_loader import load_mapping, resolve_series_ids

log = logging.getLogger("bls")


def get_bls_data(
    codes_or_ids: List[str],
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    *,
    mapping_path: Optional[Union[str, Path]] = None,
    client: Optional[BLSClient] = None,
    catalog: bool = False,
    calculations: bool = False,
    annualaverage: bool = False,
    aspects: bool = False,
) -> pd.DataFrame:
    """
    A high-level function to fetch and process data from the BLS API.
    """
    mapping = load_mapping(mapping_path)
    series_ids, reverse_map = resolve_series_ids(codes_or_ids, mapping)
    client = client or BLSClient()
    data = client.fetch(
        series_ids,
        start_year=start_year,
        end_year=end_year,
        catalog=catalog,
        calculations=calculations,
        annualaverage=annualaverage,
        aspects=aspects,
    )
    return parse_results_to_df(data, reverse_map)


def _parse_args(argv: List[str]) -> argparse.Namespace:
    """
    Parses command-line arguments for the script.
    """
    p = argparse.ArgumentParser(description="Fetch BLS time series data (v2 API) with alias mapping.")
    p.add_argument("codes", nargs="+", help="Series IDs or aliases defined in code_mapping.(csv|json)")
    p.add_argument("--start", type=int, help="Start year (YYYY)")
    p.add_argument("--end", type=int, help="End year (YYYY)")
    p.add_argument("--mapping", type=str, help="Path to mapping file (CSV/JSON)")
    p.add_argument("--catalog", action="store_true", help="Include series catalog metadata")
    p.add_argument("--calculations", action="store_true", help="Include net/percent changes (API computed)")
    p.add_argument("--annualaverage", action="store_true", help="Include annual averages (M13) when available")
    p.add_argument("--aspects", action="store_true", help="Include aspects")
    p.add_argument("--out", type=str, help="Write CSV to this path")
    p.add_argument("--log", type=str, default="INFO", help="Log level (DEBUG, INFO, WARNING, ERROR)")
    args = p.parse_args(argv)
    log.setLevel(getattr(logging, args.log.upper(), logging.INFO))
    return args


def main(argv: List[str]) -> int:
    """
    The main function for the script, handling argument parsing and data fetching.
    """
    args = _parse_args(argv)
    try:
        df = get_bls_data(
            args.codes,
            start_year=args.start,
            end_year=args.end,
            mapping_path=args.mapping,
            catalog=args.catalog,
            calculations=args.calculations,
            annualaverage=args.annualaverage,
            aspects=args.aspects,
        )
    except Exception as e:
        log.error(str(e))
        return 2

    if args.out:
        outp = Path(args.out)
        outp.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(outp, index=False)
        log.info(f"Wrote {len(df):,} rows to {outp}")
    else:
        with pd.option_context("display.max_columns", None, "display.width", 160):
            print(df.head(25).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
