# data_parser.py
"""
This module provides functionality for parsing the JSON response from the BLS API
and converting it into a pandas DataFrame for easier analysis and manipulation.
"""
from typing import Any, Dict, List, Optional

import pandas as pd


def parse_results_to_df(
    data: Dict[str, Any],
    reverse_map: Optional[Dict[str, List[str]]] = None,
) -> pd.DataFrame:
    """
    Parses the JSON response from the BLS API and converts it into a pandas DataFrame.
    """
    reverse_map = reverse_map or {}
    rows: List[Dict[str, Any]] = []

    for s in data.get("Results", {}).get("series", []):
        series_id = s.get("seriesID")
        cat = s.get("catalog", {})
        for item in s.get("data", []):
            footnotes = "; ".join(
                fn.get("text", "") for fn in item.get("footnotes", []) if fn and fn.get("text")
            ) or None
            rows.append(
                {
                    "series_id": series_id,
                    "alias": "|".join(reverse_map.get(series_id, [])) or None,
                    "year": int(item.get("year")),
                    "period": item.get("period"),
                    "period_name": item.get("periodName"),
                    "value": float(item.get("value")) if item.get("value") not in (None, "") else None,
                    "latest": s.get("latest"),
                    "seasonality": cat.get("seasonality"),
                    "series_title": cat.get("seriesTitle"),
                    "survey_name": cat.get("surveyName"),
                    "measure_data_type": cat.get("measureDataType"),
                    "area": cat.get("area"),
                    "item": cat.get("item"),
                    "footnotes": footnotes,
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "series_id",
                "alias",
                "year",
                "period",
                "period_name",
                "value",
                "latest",
                "seasonality",
                "series_title",
                "survey_name",
                "measure_data_type",
                "area",
                "item",
                "footnotes",
            ]
        )

    return pd.DataFrame(rows).sort_values(["series_id", "year", "period"]).reset_index(drop=True)
