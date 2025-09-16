#!/usr/bin/env python3
"""
MCP Server for BLS Data Repository

A local Multi-Command Protocol (MCP) server that provides wrapper tools for
existing BLS data API functions, facilitating local debugging and experimentation.
"""

from fastmcp import FastMCP
from data_extraction.main import get_bls_data
from data_extraction.bls_client import BLSClient
import bls_api

# Create MCP server instance
mcp = FastMCP("bls-data-server")


@mcp.tool()
def get_series(series_id: str, start: str = None, end: str = None):
    """
    Fetch a BLS data series by its ID, optionally specifying start/end dates.
    
    Args:
        series_id: The BLS series ID to fetch (e.g., 'CUUR0000SA0')
        start: Start year as string (e.g., '2020')
        end: End year as string (e.g., '2023')
    
    Returns:
        pandas DataFrame with the series data
    """
    try:
        # Convert string years to integers if provided
        start_year = int(start) if start else None
        end_year = int(end) if end else None
        
        # Fetch data using the main API function
        df = get_bls_data(
            codes_or_ids=[series_id],
            start_year=start_year,
            end_year=end_year
        )
        
        # Convert DataFrame to dict for JSON serialization
        return {
            "series_id": series_id,
            "data": df.to_dict('records'),
            "shape": df.shape,
            "columns": df.columns.tolist()
        }
    except Exception as e:
        return {"error": f"Failed to fetch series {series_id}: {str(e)}"}


@mcp.tool()
def list_endpoints():
    """
    Return a list of available API functions/endpoints.
    
    Returns:
        Dictionary with available functions and endpoints
    """
    try:
        # Get available functions from the main data extraction module
        data_extraction_functions = [
            f for f in dir(get_bls_data.__module__) 
            if not f.startswith("_") and callable(getattr(get_bls_data.__module__, f))
        ]
        
        # Get available endpoints from the FastAPI app
        api_endpoints = []
        if hasattr(bls_api, 'app'):
            for route in bls_api.app.routes:
                if hasattr(route, 'methods') and hasattr(route, 'path'):
                    api_endpoints.append({
                        "path": route.path,
                        "methods": list(route.methods),
                        "name": getattr(route, 'name', 'unnamed')
                    })
        
        return {
            "data_extraction_functions": data_extraction_functions,
            "api_endpoints": api_endpoints,
            "main_functions": [
                "get_bls_data - Main data fetching function",
                "BLSClient - Direct API client",
                "get_series - MCP wrapper for single series",
                "list_endpoints - MCP wrapper for function discovery"
            ]
        }
    except Exception as e:
        return {"error": f"Failed to list endpoints: {str(e)}"}


@mcp.tool()
def get_series_info(series_id: str):
    """
    Get metadata information about a BLS series.
    
    Args:
        series_id: The BLS series ID to get info for
    
    Returns:
        Dictionary with series metadata
    """
    try:
        # Create a BLS client to get series information
        client = BLSClient()
        
        # Fetch just the series metadata (catalog=True)
        df = get_bls_data(
            codes_or_ids=[series_id],
            catalog=True
        )
        
        if df.empty:
            return {"error": f"No information found for series {series_id}"}
        
        # Extract metadata from the first row
        metadata = df.iloc[0].to_dict()
        
        return {
            "series_id": series_id,
            "metadata": metadata,
            "available_columns": df.columns.tolist()
        }
    except Exception as e:
        return {"error": f"Failed to get series info for {series_id}: {str(e)}"}


@mcp.tool()
def search_series(search_term: str, limit: int = 10):
    """
    Search for BLS series by title or description.
    
    Args:
        search_term: Term to search for in series titles
        limit: Maximum number of results to return
    
    Returns:
        List of matching series with their metadata
    """
    try:
        # This is a simplified search - in a real implementation,
        # you might want to use the database or a more sophisticated search
        client = BLSClient()
        
        # For now, return a message about available search methods
        return {
            "message": "Series search functionality would require database integration",
            "suggested_approach": "Use the database API endpoints for series search",
            "available_endpoints": [
                "GET /bls_series - List all series with filtering",
                "GET /bls_series/{series_id} - Get specific series info"
            ],
            "search_term": search_term,
            "limit": limit
        }
    except Exception as e:
        return {"error": f"Search failed: {str(e)}"}


@mcp.tool()
def analyze_cpi_seasonality(series_id: str, start: str = None, end: str = None):
    """
    Analyze CPI seasonality with percentile bands and current year comparison.
    Returns JSON table + base64 plot.
    
    Args:
        series_id: The BLS series ID to analyze (e.g., 'CUUR0000SA0')
        start: Start year as string (default: last 10 years)
        end: End year as string (default: current year)
    
    Returns:
        Dictionary with seasonality analysis table and plot
    """
    try:
        import pandas as pd
        import io
        import base64
        import matplotlib.pyplot as plt
        from datetime import datetime
        
        # Set default date range if not provided
        if not start:
            start_year = datetime.now().year - 10
        else:
            start_year = int(start)
            
        if not end:
            end_year = datetime.now().year
        else:
            end_year = int(end)
        
        # Fetch data using the main API function
        df = get_bls_data(
            codes_or_ids=[series_id],
            start_year=start_year,
            end_year=end_year
        )
        
        if df.empty:
            return {"error": f"No data found for series {series_id} in the specified range"}
        
        # Convert to proper datetime and sort
        # Handle BLS period format (M01, M02, etc.)
        df['month'] = df['period'].str.extract(r'M(\d+)').astype(int)
        df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
        df = df.sort_values('date').set_index('date')
        
        # Compute MoM changes (month-over-month percentage change)
        df['mom_change'] = df['value'].pct_change() * 100
        
        # Filter last 10 years for historical analysis
        cutoff_date = pd.Timestamp.today() - pd.DateOffset(years=10)
        last10 = df[df.index >= cutoff_date].copy()
        
        if last10.empty:
            return {"error": "Insufficient historical data for seasonality analysis"}
        
        # Group by month for percentile calculation
        last10['month'] = last10.index.month
        percentiles = last10.groupby('month')['mom_change'].quantile([0.25, 0.5, 0.75]).unstack()
        
        # Current year data
        current_year = df[df.index.year == datetime.now().year].copy()
        current_year['month'] = current_year.index.month
        current_vals = current_year.set_index('month')['mom_change']
        
        # Build seasonality table
        table = []
        month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        
        for m in range(1, 13):
            table.append({
                "month": m,
                "month_name": month_names[m-1],
                "p25": round(percentiles.loc[m, 0.25], 3) if m in percentiles.index else None,
                "p50": round(percentiles.loc[m, 0.5], 3) if m in percentiles.index else None,
                "p75": round(percentiles.loc[m, 0.75], 3) if m in percentiles.index else None,
                "current": round(current_vals[m], 3) if m in current_vals.index else None
            })
        
        # Create seasonality plot
        plt.style.use('default')
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Plot historical percentiles
        if not percentiles.empty:
            ax.plot(percentiles.index, percentiles[0.25], label="25th percentile", 
                   linestyle="--", color="lightblue", alpha=0.8)
            ax.plot(percentiles.index, percentiles[0.5], label="Median (50th percentile)", 
                   linewidth=2, color="blue")
            ax.plot(percentiles.index, percentiles[0.75], label="75th percentile", 
                   linestyle="--", color="lightblue", alpha=0.8)
            
            # Fill between percentiles for better visualization
            ax.fill_between(percentiles.index, percentiles[0.25], percentiles[0.75], 
                           alpha=0.2, color="lightblue", label="25th-75th percentile range")
        
        # Plot current year data
        if not current_vals.empty:
            ax.plot(current_vals.index, current_vals.values, label=f"Current Year ({datetime.now().year})", 
                   marker="o", color="red", linewidth=2, markersize=6)
        
        # Formatting
        ax.set_xticks(range(1, 13))
        ax.set_xticklabels(month_names)
        ax.set_xlabel("Month")
        ax.set_ylabel("Month-over-Month Change (%)")
        ax.set_title(f"CPI Seasonality Analysis: {series_id}\nHistorical Percentiles vs Current Year")
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Save plot to base64
        buf = io.BytesIO()
        plt.tight_layout()
        fig.savefig(buf, format="png", dpi=150, bbox_inches='tight')
        buf.seek(0)
        image_base64 = base64.b64encode(buf.read()).decode("utf-8")
        plt.close(fig)
        
        # Calculate summary statistics
        summary_stats = {
            "analysis_period": f"{start_year}-{end_year}",
            "historical_data_points": len(last10),
            "current_year_data_points": len(current_year),
            "avg_historical_mom": round(last10['mom_change'].mean(), 3),
            "std_historical_mom": round(last10['mom_change'].std(), 3)
        }
        
        return {
            "series_id": series_id,
            "table": table,
            "image_base64": image_base64,
            "summary_stats": summary_stats,
            "description": f"Seasonality analysis of {series_id}: historical percentiles (last 10 years) vs current year month-over-month changes."
        }
        
    except Exception as e:
        return {"error": f"Failed to analyze seasonality for {series_id}: {str(e)}"}


if __name__ == "__main__":
    # Run the MCP server via stdio
    mcp.run()





