# mapping_loader.py
"""
This module handles the loading and resolution of series IDs from mapping files and dynamic sources.
It supports both CSV and JSON formats and provides a flexible way to map human-readable aliases to BLS series IDs.
It also integrates with the cu_series_codes module to dynamically fetch CPI series IDs.
"""

# Add project root to the Python path
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import csv
import json
import logging
from collections.abc import Iterable
from typing import Optional, Union

# Assumes the script is run from a context where 'cu_series' is in the Python path.
# This is typically the case if you run from the project root.
from cu_series.cu_series_codes import get_cu_series_codes

log = logging.getLogger("bls")


def _norm_key(s: str) -> str:
    """
    Normalizes a string to be used as a key in the mapping dictionary.
    """
    return (
        s.strip()
        .casefold()
        .replace("-", "")
        .replace("_", "")
        .replace(" ", "")
        .replace(".", "")
        .replace("/", "")
    )


def _read_csv_mapping(path: Path) -> dict[str, Union[str, list[str]]]:
    """
    Reads a CSV file and creates a mapping from aliases to series IDs.
    """
    mapping: dict[str, Union[str, list[str]]] = {}
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"{path.name} has no header row")

        cols = [c.strip().lower() for c in reader.fieldnames]
        alias_col = next(
            (c for c in ("alias", "name", "label", "code") if c in cols), None
        )
        series_col = next(
            (c for c in ("series", "series_id", "seriesid", "seriesId") if c in cols),
            None,
        )

        if not alias_col or not series_col:
            if len(cols) == 2:
                alias_col, series_col = cols
            else:
                raise ValueError(
                    f"{path.name} header must include alias/name/label/code and series/series_id, or be exactly two columns. Found: {cols}"
                )

        for row in reader:
            alias_raw = row.get(alias_col, "")
            sid_raw = row.get(series_col, "")
            if alias_raw and sid_raw:
                alias = _norm_key(str(alias_raw))
                sid = str(sid_raw).strip()
                if alias in mapping:
                    prev = mapping[alias]
                    if isinstance(prev, list):
                        if sid not in prev:
                            prev.append(sid)
                    elif sid != prev:
                        mapping[alias] = [prev, sid]
                else:
                    mapping[alias] = sid
    return mapping


def _read_json_mapping(path: Path) -> dict[str, Union[str, list[str]]]:
    """
    Reads a JSON file and creates a mapping from aliases to series IDs.
    """
    with path.open(encoding="utf-8") as f:
        data = json.load(f)

    mapping: dict[str, Union[str, list[str]]] = {}
    if isinstance(data, dict):
        if "groups" in data and isinstance(data["groups"], list):
            for g in data["groups"]:
                alias_raw = (
                    g.get("alias") or g.get("name") or g.get("label") or g.get("code")
                )
                sid = g.get("series") or g.get("series_id") or g.get("seriesid")
                if alias_raw and sid:
                    mapping[_norm_key(str(alias_raw))] = sid
        else:
            for k, v in data.items():
                mapping[_norm_key(str(k))] = v
    elif isinstance(data, list):
        for g in data:
            if isinstance(g, dict):
                alias_raw = (
                    g.get("alias") or g.get("name") or g.get("label") or g.get("code")
                )
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
) -> dict[str, Union[str, list[str]]]:
    """
    Loads a mapping from a file, searching in default locations if no path is provided.
    """
    base_dir = Path(__file__).parent
    candidates = (
        [Path(explicit_path)]
        if explicit_path
        else [base_dir / name for name in fallback_names]
    )

    for path in candidates:
        if path.exists():
            try:
                if path.suffix.lower() == ".csv":
                    mapping = _read_csv_mapping(path)
                elif path.suffix.lower() == ".json":
                    mapping = _read_json_mapping(path)
                else:
                    continue
                if mapping:
                    log.info(
                        f"Loaded code mapping from {path.name} ({len(mapping)} entries)"
                    )
                    return mapping
            except Exception as e:
                log.warning(f"Failed to read {path.name}: {e}")
    log.info("No mapping file found; only raw series IDs will be accepted.")
    return {}


def _parse_cu_filters(filter_str: str) -> Optional[dict[str, str]]:
    """
    Parses a filter string for CU series codes.
    Example: "area_code=0000,item_code=SA0"
    """
    if not filter_str:
        return None
    try:
        # Simple parser for key=value pairs separated by commas
        filters = dict(item.split("=") for item in filter_str.split(","))
        return filters
    except ValueError:
        log.warning(
            f"Invalid CU filter format: {filter_str}. Expected 'key1=value1,key2=value2'."
        )
        return None


def resolve_series_ids(
    codes_or_ids: Iterable[str],
    mapping: Optional[dict[str, Union[str, list[str]]]] = None,
) -> tuple[list[str], dict[str, list[str]]]:
    """
    Resolves human-friendly codes to BLS series IDs using the provided mapping.
    Also handles dynamic fetching of CU series IDs with a 'CU:' prefix.
    Example prefixes:
    - "CU:" (all CU series)
    - "CU:area_code=0000" (all for U.S. City Average)
    - "CU:area_code=0000,item_code=SA0" (U.S. City Average, All Items)
    """
    mapping = mapping or {}
    series_ids: list[str] = []
    reverse_map: dict[str, list[str]] = {}
    unknown: list[str] = []

    for token in codes_or_ids:
        token = str(token).strip()
        if not token:
            continue

        if token.upper().startswith("CU:"):
            filter_str = token[3:]
            filters = _parse_cu_filters(filter_str)
            try:
                cu_series = get_cu_series_codes(filters)
                if not cu_series:
                    log.warning(f"No CU series found for filter: {filter_str}")
                    continue
                series_ids.extend(cu_series)
                # Map all found series IDs back to the original token for traceability
                for sid in cu_series:
                    reverse_map.setdefault(sid, []).append(token)
                log.info(f"Resolved '{token}' to {len(cu_series)} CU series IDs.")
            except Exception as e:
                log.error(f"Failed to fetch CU series for '{token}': {e}")
            continue

        looks_like_sid = any(ch.isdigit() for ch in token) and any(
            ch.isalpha() for ch in token
        )
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
        else:
            unknown.append(token)

    if unknown:
        raise KeyError(
            f"Unknown codes (not BLS series IDs and not in mapping): {', '.join(sorted(set(unknown)))}"
        )

    # Deduplicate series IDs while preserving order
    seen = set()
    deduped = [sid for sid in series_ids if not (sid in seen or seen.add(sid))]
    return deduped, reverse_map
