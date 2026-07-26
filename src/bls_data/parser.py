"""Parse BLS API JSON responses into pandas DataFrames."""

from typing import Any, Optional

import pandas as pd


def _safe_float(val):
    """Parse a value to float, returning None for non-numeric strings like '-'."""
    if val is None or val == "" or val == "-":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def parse_results_to_df(
    data: dict[str, Any],
    reverse_map: Optional[dict[str, list[str]]] = None,
) -> pd.DataFrame:
    """Convert BLS API JSON response to a tidy pandas DataFrame."""
    reverse_map = reverse_map or {}
    rows: list[dict[str, Any]] = []

    for s in data.get("Results", {}).get("series", []):
        series_id = s.get("seriesID")
        cat = s.get("catalog", {})
        for item in s.get("data", []):
            footnotes = (
                "; ".join(
                    fn.get("text", "")
                    for fn in item.get("footnotes", [])
                    if fn and fn.get("text")
                )
                or None
            )
            rows.append(
                {
                    "series_id": series_id,
                    "alias": "|".join(reverse_map.get(series_id, [])) or None,
                    "year": int(item["year"]),
                    "period": item.get("period"),
                    "period_name": item.get("periodName"),
                    "value": _safe_float(item.get("value")),
                    "latest": s.get("latest"),
                    "series_title": cat.get("series_title"),
                    "survey_name": cat.get("survey_name"),
                    "measure_data_type": cat.get("measure_data_type"),
                    "area": cat.get("area"),
                    "item": cat.get("item"),
                    "seasonality": cat.get("seasonality"),
                    "footnotes": footnotes,
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "series_id", "alias", "year", "period", "period_name",
                "value", "latest", "series_title", "survey_name",
                "measure_data_type", "area", "item", "seasonality", "footnotes",
            ]
        )

    return (
        pd.DataFrame(rows)
        .sort_values(["series_id", "year", "period"])
        .reset_index(drop=True)
    )