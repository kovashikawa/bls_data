"""Human-readable alias → BLS series ID mapping (CSV/JSON) + CU series resolution."""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Optional, Union

log = logging.getLogger(__name__)


def _norm_key(s: str) -> str:
    return s.strip().casefold().replace("-", "").replace("_", "").replace(" ", "").replace(".", "").replace("/", "")


def load_mapping(
    explicit_path: Optional[Union[str, Path]] = None,
    *,
    fallback_names: tuple[str, ...] = (
        "code_mapping.csv", "series_map.csv", "series_mapping.csv",
        "code_mapping.json", "series_map.json", "series_mapping.json",
    ),
) -> dict[str, Union[str, list[str]]]:
    """Load alias→series_id mapping from CSV or JSON, auto-detecting format."""
    base_dir = Path(__file__).parent.parent.parent / "data_extraction"
    candidates = (
        [Path(explicit_path)] if explicit_path
        else [base_dir / name for name in fallback_names]
    )
    for path in candidates:
        if not path.exists():
            continue
        try:
            mapping = _read_csv_mapping(path) if path.suffix.lower() == ".csv" else _read_json_mapping(path)
            if mapping:
                log.info("Loaded mapping from %s (%d entries)", path.name, len(mapping))
                return mapping
        except Exception as e:
            log.warning("Failed to read %s: %s", path.name, e)
    log.info("No mapping file found; only raw series IDs accepted.")
    return {}


def _read_csv_mapping(path: Path) -> dict[str, Union[str, list[str]]]:
    mapping: dict[str, Union[str, list[str]]] = {}
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"{path.name} has no header row")
        cols = [c.strip().lower() for c in reader.fieldnames]
        alias_col = next((c for c in ("alias", "name", "label", "code") if c in cols), None)
        series_col = next((c for c in ("series", "series_id", "seriesid") if c in cols), None)
        if not alias_col or not series_col:
            if len(cols) == 2:
                alias_col, series_col = cols
            else:
                raise ValueError(f"Cannot determine alias/series columns in {path.name}: {cols}")
        for row in reader:
            alias_raw, sid_raw = row.get(alias_col, ""), row.get(series_col, "")
            if alias_raw and sid_raw:
                alias = _norm_key(str(alias_raw))
                sid = str(sid_raw).strip()
                existing = mapping.get(alias)
                if existing is None:
                    mapping[alias] = sid
                elif isinstance(existing, list):
                    if sid not in existing:
                        existing.append(sid)
                elif sid != existing:
                    mapping[alias] = [existing, sid]
    return mapping


def _read_json_mapping(path: Path) -> dict[str, Union[str, list[str]]]:
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    mapping: dict[str, Union[str, list[str]]] = {}
    items = data.get("groups", data) if isinstance(data, dict) else data
    if isinstance(items, dict) and "groups" not in data:
        for k, v in items.items():
            mapping[_norm_key(str(k))] = v
    elif isinstance(items, list):
        for g in items:
            if not isinstance(g, dict):
                continue
            alias_raw = g.get("alias") or g.get("name") or g.get("label") or g.get("code")
            sid = g.get("series") or g.get("series_id") or g.get("seriesid")
            if alias_raw and sid:
                mapping[_norm_key(str(alias_raw))] = sid
    return mapping


def resolve_series_ids(
    codes_or_ids: list[str],
    mapping: Optional[dict[str, Union[str, list[str]]]] = None,
) -> tuple[list[str], dict[str, list[str]]]:
    """Resolve human-readable codes to BLS series IDs, including CU: prefixed patterns."""
    mapping = mapping or {}
    series_ids: list[str] = []
    reverse_map: dict[str, list[str]] = {}
    unknown: list[str] = []

    for token in codes_or_ids:
        token = str(token).strip()
        if not token:
            continue

        # CU: dynamic resolution — e.g. CU:area_code=0000,item_code=SA0
        if token.upper().startswith("CU:"):
            from .cpi import get_cu_series_codes
            filters = _parse_cu_filters(token[3:])
            try:
                cu_ids = get_cu_series_codes(filters)
                series_ids.extend(cu_ids)
                for sid in cu_ids:
                    reverse_map.setdefault(sid, []).append(token)
            except Exception as e:
                log.error("CU resolution failed for '%s': %s", token, e)
            continue

        key = _norm_key(token)
        if key in mapping:
            mapped = mapping[key]
            sids = [mapped] if isinstance(mapped, str) else list(mapped)
            for sid in sids:
                series_ids.append(str(sid).strip())
                reverse_map.setdefault(sid, []).append(token)
        elif any(ch.isdigit() for ch in token) and any(ch.isalpha() for ch in token):
            series_ids.append(token)
        else:
            unknown.append(token)

    if unknown:
        raise KeyError(f"Unknown codes: {', '.join(sorted(set(unknown)))}")

    seen: set[str] = set()
    return [sid for sid in series_ids if not (sid in seen or seen.add(sid))], reverse_map


def _parse_cu_filters(filter_str: str) -> Optional[dict[str, str]]:
    if not filter_str:
        return None
    try:
        return dict(item.split("=") for item in filter_str.split(","))
    except ValueError:
        log.warning("Invalid CU filter format: %s", filter_str)
        return None