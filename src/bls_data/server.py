"""FastMCP server for BLS data — tools for querying and analyzing economic time series."""

from typing import Optional
import io
import base64
from datetime import datetime

import pandas as pd
from fastmcp import FastMCP

from bls_data.client import BLSClient
from bls_data.parser import parse_results_to_df
from bls_data.mapping import load_mapping, resolve_series_ids

mcp = FastMCP("bls-data-server")
_client: Optional[BLSClient] = None


def _get_client() -> BLSClient:
    global _client
    if _client is None:
        _client = BLSClient()
    return _client


@mcp.tool()
def get_series(series_id: str, start: Optional[str] = None, end: Optional[str] = None) -> dict:
    """Fetch a BLS data series by ID with optional date range.

    Args:
        series_id: BLS series ID (e.g. 'CUUR0000SA0' for CPI All Items)
        start: Start year (e.g. '2020')
        end: End year (e.g. '2024')
    """
    try:
        client = _get_client()
        data = client.fetch(
            [series_id],
            start_year=int(start) if start else None,
            end_year=int(end) if end else None,
        )
        if not data.get("Results", {}).get("series"):
            return {"error": f"No data found for series {series_id}"}
        df = parse_results_to_df(data)
        return {
            "series_id": series_id,
            "count": len(df),
            "date_range": {"start": str(df["year"].min()), "end": str(df["year"].max())},
            "data": df.to_dict("records"),
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def get_series_info(series_id: str) -> dict:
    """Get catalog metadata for a BLS series.

    Args:
        series_id: BLS series ID
    """
    try:
        client = _get_client()
        data = client.fetch([series_id], catalog=True)
        series_list = data.get("Results", {}).get("series", [])
        if not series_list:
            return {"error": f"No metadata for {series_id}"}
        cat = series_list[0].get("catalog", {})
        return {
            "series_id": series_id,
            "title": cat.get("series_title"),
            "survey": cat.get("survey_name"),
            "measure": cat.get("measure_data_type"),
            "area": cat.get("area"),
            "item": cat.get("item"),
            "seasonality": cat.get("seasonality"),
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def search_series(query: str, limit: int = 10) -> dict:
    """Search BLS series by keyword in the CPI master catalog.

    The CPI catalog contains 8,000+ series with full titles. For broader
    discovery, use list_surveys() and popular_series() first.

    Args:
        query: Search term (e.g. 'food', 'housing', 'energy', 'medical')
        limit: Max results
    """
    try:
        from bls_data.cpi import _MASTER_PATH
        cpi = pd.read_csv(_MASTER_PATH, dtype=str)
        mask = cpi["series_title"].str.contains(query, case=False, na=False)
        results = cpi[mask].head(limit)
        return {
            "query": query,
            "count": len(results),
            "results": results[["series_id", "series_title"]].to_dict("records"),
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def list_surveys() -> dict:
    """List all available BLS surveys with abbreviations and names.

    Use this to discover what economic data categories are available (CPI, employment,
    wages, PPI, productivity, etc.) before searching for specific series.
    """
    try:
        client = _get_client()
        surveys = client.list_surveys()
        return {
            "count": len(surveys),
            "surveys": [
                {"abbreviation": s.get("survey_abbreviation", s.get("surveyAbbreviation")),
                 "name": s.get("survey_name", s.get("surveyName"))}
                for s in surveys
            ],
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def popular_series(survey: Optional[str] = None) -> dict:
    """Return the most-requested BLS series, optionally filtered by survey.

    Args:
        survey: Survey abbreviation (e.g. 'CU' for CPI, 'LN' for employment/unemployment,
                'CE' for Current Employment Statistics). Omit for all popular series.
    """
    try:
        client = _get_client()
        series = client.get_popular_series(survey)
        return {
            "survey": survey or "all",
            "count": len(series),
            "series": [
                {"series_id": s.get("seriesID"), "title": s.get("series_title", "")}
                for s in series
            ],
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def analyze_cpi_seasonality(
    series_id: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> dict:
    """Analyze CPI seasonality with percentile bands and current year comparison.

    Args:
        series_id: BLS series ID
        start: Start year (default: 10 years ago)
        end: End year (default: current year)
    """
    try:
        import matplotlib.pyplot as plt

        start_year = int(start) if start else datetime.now().year - 10
        end_year = int(end) if end else datetime.now().year

        client = _get_client()
        data = client.fetch([series_id], start_year=start_year, end_year=end_year)
        df = parse_results_to_df(data)

        if df.empty:
            return {"error": f"No data for {series_id} in {start_year}-{end_year}"}

        df["month"] = df["period"].str.extract(r"M(\d+)").astype(int)
        df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))
        df = df.sort_values("date").set_index("date")
        df["mom_change"] = df["value"].pct_change() * 100

        last10 = df[df.index >= pd.Timestamp.today() - pd.DateOffset(years=10)].copy()
        if last10.empty:
            return {"error": "Insufficient historical data"}

        last10["month"] = last10.index.month
        percentiles = last10.groupby("month")["mom_change"].quantile([0.25, 0.5, 0.75]).unstack()

        current = df[df.index.year == datetime.now().year].copy()
        current["month"] = current.index.month
        current_vals = current.set_index("month")["mom_change"]

        month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        table = []
        for m in range(1, 13):
            table.append({
                "month": m, "name": month_names[m-1],
                "p25": round(percentiles.loc[m, 0.25], 3) if m in percentiles.index else None,
                "p50": round(percentiles.loc[m, 0.5], 3) if m in percentiles.index else None,
                "p75": round(percentiles.loc[m, 0.75], 3) if m in percentiles.index else None,
                "current": round(current_vals[m], 3) if m in current_vals.index else None,
            })

        fig, ax = plt.subplots(figsize=(12, 6))
        if not percentiles.empty:
            ax.plot(percentiles.index, percentiles[0.25], "--", color="#b8c9da", label="25th")
            ax.plot(percentiles.index, percentiles[0.5], linewidth=2, color="#3a5068", label="Median")
            ax.plot(percentiles.index, percentiles[0.75], "--", color="#b8c9da", label="75th")
            ax.fill_between(percentiles.index, percentiles[0.25], percentiles[0.75], alpha=0.15, color="#3a5068")
        if not current_vals.empty:
            ax.plot(current_vals.index, current_vals.values, "o-", color="#1E1E1E", linewidth=2, markersize=6, label=str(datetime.now().year))
        ax.set_xticks(range(1, 13))
        ax.set_xticklabels(month_names)
        ax.set_xlabel("Month")
        ax.set_ylabel("MoM Change (%)")
        ax.set_title(f"CPI Seasonality: {series_id}")
        ax.legend()
        ax.grid(True, alpha=0.3)

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
        buf.seek(0)
        import base64
        plot_b64 = base64.b64encode(buf.read()).decode()
        plt.close(fig)

        return {
            "series_id": series_id,
            "table": table,
            "plot_base64": plot_b64,
            "stats": {
                "period": f"{start_year}-{end_year}",
                "historical_points": len(last10),
                "avg_mom": round(last10["mom_change"].mean(), 3),
            },
        }
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    mcp.run()