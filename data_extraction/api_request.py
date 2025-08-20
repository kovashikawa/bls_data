# data_extraction/api_request.py
"""
api_request.py — clean BLS API client with robust code mapping.

Usage (library):
    from api_request import get_bls_data

    # Using human-friendly codes defined in code_mapping.(csv|json)
    df = get_bls_data(["cpi_all_items", "ces_all_employees"], 2010, 2024, catalog=True)

    # Or pass series IDs directly:
    df = get_bls_data(["CUUR0000SA0", "CES0000000001"], 2010, 2024)

CLI:
    python api_request.py CUUR0000SA0 CES0000000001 --start 2010 --end 2024 --catalog
"""
from __future__ import annotations

import os
import sys
import json
import csv
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Dict, Any, Optional, Tuple, Union

import requests
import pandas as pd

from api_key import get_random_bls_key

BLS_V2_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

log = logging.getLogger("bls")
if not log.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    log.addHandler(handler)
log.setLevel(logging.INFO)


# ------------------------------ Mapping ------------------------------------ #

def _norm_key(s: str) -> str:
    """
        Normalize mapping keys to be forgiving about case/spacing/punctuation.
    """
    s = (
        s.strip()
         .casefold()
         .replace("-", "")
         .replace("_", "")
         .replace(" ", "")
         .replace(".", "")
         .replace("/", "")
    )
    
    return s


def _read_csv_mapping(path: Path) -> Dict[str, Union[str, List[str]]]:
    """
    Accepts either:
      - two-column CSV: alias, series_id
      - multi-column CSV that *contains* columns named one of:
        ['alias','name','label','code'] and one of ['series','series_id','seriesid']
    Multiple rows with the same alias are grouped into a list of series IDs.
    """
    mapping: Dict[str, Union[str, List[str]]] = {}

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"{path.name} has no header row")

        cols = [c.strip().lower() for c in reader.fieldnames]
        # Guess columns
        alias_col = None
        series_col = None
        for cand in ("alias", "name", "label", "code"):
            if cand in cols:
                alias_col = cand
                break
        for cand in ("series", "series_id", "seriesid", "seriesId"):
            if cand.lower() in cols:
                series_col = cand.lower()
                break

        # If exactly two columns and not recognized, assume first is alias, second is series
        if alias_col is None or series_col is None:
            if len(cols) == 2:
                alias_col, series_col = cols[0], cols[1]
            else:
                raise ValueError(
                    f"{path.name} header must include alias/name/label/code and series/series_id, "
                    f"or be exactly two columns. Found: {cols}"
                )

        for row in reader:
            alias_raw = row.get(alias_col, "")
            sid_raw = row.get(series_col, "")
            if not alias_raw or not sid_raw:
                continue
            alias = _norm_key(str(alias_raw))
            sid = str(sid_raw).strip()
            if alias in mapping:
                prev = mapping[alias]
                if isinstance(prev, list):
                    if sid not in prev:
                        prev.append(sid)
                else:
                    if sid != prev:
                        mapping[alias] = [prev, sid]
            else:
                mapping[alias] = sid
    return mapping


def _read_json_mapping(path: Path) -> Dict[str, Union[str, List[str]]]:
    """
    JSON structure options:
      - {"alias": "SERIES_ID", "...": "..."}
      - {"alias": ["SERIES_1", "SERIES_2"]}
      - {"groups": [{"alias": "x", "series_id": "..."}, ...]}  (we'll ingest)
    """
    with path.open(encoding="utf-8") as f:
        data = json.load(f)

    mapping: Dict[str, Union[str, List[str]]] = {}
    if isinstance(data, dict):
        # direct {alias: ids} or {groups: [...]}
        if "groups" in data and isinstance(data["groups"], list):
            for g in data["groups"]:
                alias_raw = g.get("alias") or g.get("name") or g.get("label") or g.get("code")
                sid = g.get("series") or g.get("series_id") or g.get("seriesid")
                if alias_raw and sid:
                    mapping[_norm_key(str(alias_raw))] = sid
        else:
            for k, v in data.items():
                mapping[_norm_key(str(k))] = v
    elif isinstance(data, list):
        # list of objects with alias/series
        for g in data:
            if isinstance(g, dict):
                alias_raw = g.get("alias") or g.get("name") or g.get("label") or g.get("code")
                sid = g.get("series") or g.get("series_id") or g.get("seriesid")
                if alias_raw and sid:
                    mapping[_norm_key(str(alias_raw))] = sid
    else:
        raise ValueError(f"Unsupported JSON mapping schema in {path}")
    return mapping


def load_mapping(
    explicit_path: Optional[Union[str, Path]] = None,
    *,
    fallback_names: Iterable[str] = (
        "code_mapping.csv",
        "series_map.csv",
        "series_mapping.csv",
        "code_mapping.json",
        "series_map.json",
        "series_mapping.json",
    ),
) -> Dict[str, Union[str, List[str]]]:
    """
    Load a mapping dict of normalized alias -> series_id (or list of series_ids).
    Looks in the same directory as this script unless an explicit path is provided.
    """
    base_dir = Path(__file__).parent
    candidates: List[Path] = []
    if explicit_path:
        p = Path(explicit_path)
        if not p.is_absolute():
            p = base_dir / p
        candidates.append(p)
    for name in fallback_names:
        candidates.append(base_dir / name)

    for path in candidates:
        if path.exists():
            try:
                if path.suffix.lower() == ".csv":
                    mapping = _read_csv_mapping(path)
                elif path.suffix.lower() == ".json":
                    mapping = _read_json_mapping(path)
                else:
                    continue  # skip unsupported
                if mapping:
                    log.info(f"Loaded code mapping from {path.name} ({len(mapping)} entries)")
                    return mapping
            except Exception as e:
                log.warning(f"Failed to read {path.name}: {e}")
    log.info("No mapping file found; only raw series IDs will be accepted.")
    return {}


def resolve_series_ids(
    codes_or_ids: Iterable[str],
    mapping: Optional[Dict[str, Union[str, List[str]]]] = None,
) -> Tuple[List[str], Dict[str, List[str]]]:
    """
    Maps human-friendly codes to BLS series IDs. Accepts either:
      - direct BLS series IDs (alphanumeric ~ 12-20 chars) — passed through
      - aliases present in `mapping` — replaced with mapped IDs

    Returns (series_ids, reverse_map) where reverse_map maps series_id -> [aliases]
    for traceability in the final DataFrame.
    """
    mapping = mapping or {}
    series_ids: List[str] = []
    reverse_map: Dict[str, List[str]] = {}
    unknown: List[str] = []

    for token in codes_or_ids:
        token = str(token).strip()
        if not token:
            continue
        # Heuristic: treat as series ID if it starts with letters and contains digits
        looks_like_sid = any(ch.isdigit() for ch in token) and any(ch.isalpha() for ch in token)
        key = _norm_key(token)
        if key in mapping:
            mapped = mapping[key]
            sids = [mapped] if isinstance(mapped, str) else list(mapped)
            for sid in sids:
                sid = str(sid).strip()
                series_ids.append(sid)
                reverse_map.setdefault(sid, []).append(token)
        elif looks_like_sid and 8 <= len(token) <= 25:
            series_ids.append(token)
            # keep reverse map empty (no alias)
        else:
            unknown.append(token)

    if unknown:
        raise KeyError(
            "Unknown codes (not BLS series IDs and not found in mapping): "
            + ", ".join(sorted(set(unknown)))
        )

    # de-duplicate preserving order
    seen = set()
    deduped: List[str] = []
    for sid in series_ids:
        if sid not in seen:
            deduped.append(sid)
            seen.add(sid)
    return deduped, reverse_map


# ------------------------------ Client ------------------------------------- #

@dataclass
class BLSClient:
    api_key: Optional[str] = field(default_factory=get_random_bls_key)
    url: str = BLS_V2_URL
    session: requests.Session = field(default_factory=requests.Session)

    series_limit: int = 50   # per request (v2)
    years_limit: int = 20    # per request (v2)

    def __post_init__(self) -> None:
        if self.api_key is None:
            self.api_key = get_random_bls_key()
        # requests retry config
        try:
            from urllib3.util import Retry
            from requests.adapters import HTTPAdapter
            retry = Retry(
                total=5,
                backoff_factor=1.2,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset({"GET", "POST"}),
            )
            adapter = HTTPAdapter(max_retries=retry)
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
        except Exception:
            pass

    def _post(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        headers = {"Content-Type": "application/json"}
        resp = self.session.post(self.url, json=payload, headers=headers, timeout=60)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise RuntimeError(f"BLS API HTTP error: {e} — body: {resp.text[:500]}") from e
        data = resp.json()
        if data.get("status") != "REQUEST_SUCCEEDED":
            raise RuntimeError(f'BLS API returned status={data.get("status")}: {data.get("message")}')
        return data

    def _year_chunks(self, start: int, end: int) -> List[Tuple[int, int]]:
        if start > end:
            start, end = end, start
        years = end - start + 1
        if years <= self.years_limit:
            return [(start, end)]
        chunks = []
        s = start
        while s <= end:
            e = min(s + self.years_limit - 1, end)
            chunks.append((s, e))
            s = e + 1
        return chunks

    def fetch(
        self,
        series_ids: Iterable[str],
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        *,
        catalog: bool = False,
        calculations: bool = False,
        annualaverage: bool = False,
        aspects: bool = False,
    ) -> Dict[str, Any]:
        """
        Calls BLS API, automatically chunking series and years to respect limits.
        Returns merged JSON (Results.series is concatenated across chunks).
        """
        sids = list(series_ids)
        if not sids:
            raise ValueError("No series IDs provided.")

        # Default span: if start/end not given, BLS returns 3 years; here keep None to let API decide
        merged: Dict[str, Any] = {"status": "REQUEST_SUCCEEDED", "Results": {"series": []}, "message": None}

        series_chunks = [sids[i:i + self.series_limit] for i in range(0, len(sids), self.series_limit)]
        year_chunks = self._year_chunks(start_year, end_year) if (start_year and end_year) else [(start_year, end_year)]

        for sc in series_chunks:
            for (ys, ye) in year_chunks:
                payload: Dict[str, Any] = {"seriesid": sc}
                if self.api_key:
                    payload["registrationkey"] = self.api_key
                if ys is not None:
                    payload["startyear"] = int(ys)
                if ye is not None:
                    payload["endyear"] = int(ye)
                if catalog:
                    payload["catalog"] = True
                if calculations:
                    payload["calculations"] = True
                if annualaverage:
                    payload["annualaverage"] = True
                if aspects:
                    payload["aspects"] = True

                data = self._post(payload)
                # merge
                chunk_series = data.get("Results", {}).get("series", [])
                merged["Results"]["series"].extend(chunk_series)
        return merged


def _parse_results_to_df(
    data: Dict[str, Any],
    reverse_map: Optional[Dict[str, List[str]]] = None,
) -> pd.DataFrame:
    reverse_map = reverse_map or {}
    rows: List[Dict[str, Any]] = []

    for s in data.get("Results", {}).get("series", []):
        series_id = s.get("seriesID") or s.get("seriesId") or s.get("series_id")
        cat = s.get("catalog", {}) or {}
        for item in s.get("data", []):
            footnotes = item.get("footnotes", [])
            # footnotes may be list of dicts with 'text'
            fn_text = "; ".join([fn.get("text", "") for fn in footnotes if fn and fn.get("text")]) or None
            rows.append(
                {
                    "series_id": series_id,
                    "alias": "|".join(reverse_map.get(series_id, [])) or None,
                    "year": int(item.get("year")),
                    "period": item.get("period"),
                    "period_name": item.get("periodName"),
                    "value": float(item.get("value")) if item.get("value") not in (None, "") else None,
                    "latest": s.get("latest") if isinstance(s.get("latest"), bool) else None,
                    "seasonality": cat.get("seasonality"),
                    "series_title": cat.get("series_title") or cat.get("seriesTitle"),
                    "survey_name": cat.get("survey_name") or cat.get("surveyName"),
                    "measure_data_type": cat.get("measure_data_type") or cat.get("measureDataType"),
                    "area": cat.get("area"),
                    "item": cat.get("item"),
                    "footnotes": fn_text,
                }
            )

    if not rows:
        return pd.DataFrame(columns=[
            "series_id","alias","year","period","period_name","value","latest","seasonality",
            "series_title","survey_name","measure_data_type","area","item","footnotes"
        ])

    df = pd.DataFrame(rows).sort_values(["series_id", "year", "period"]).reset_index(drop=True)
    return df


def get_bls_data(
    codes_or_ids: Iterable[str],
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
    High-level helper: resolve codes/IDs, call API, return tidy DataFrame.
    Raises KeyError if any codes can't be resolved by mapping and don't look like BLS IDs.
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
    return _parse_results_to_df(data, reverse_map)


# --------------------------------- CLI ------------------------------------- #

def _parse_args(argv: List[str]) -> Any:
    import argparse

    p = argparse.ArgumentParser(description="Fetch BLS time series data (v2 API) with alias mapping.")
    p.add_argument("codes", nargs="+", help="Series IDs or aliases defined in code_mapping.(csv|json)")
    p.add_argument("--start", type=int, dest="start", help="Start year (YYYY)")
    p.add_argument("--end", type=int, dest="end", help="End year (YYYY)")
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
        # Pretty print a small sample to stdout
        with pd.option_context("display.max_columns", None, "display.width", 160):
            print(df.head(25).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
