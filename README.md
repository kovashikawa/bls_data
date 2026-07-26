# bls_data

Clean Python toolkit for U.S. Bureau of Labor Statistics (BLS) time-series data — API client, parser, series mapping, and MCP server.

## Install

```bash
pip install -e .
# or with dev deps
pip install -e ".[dev]"
# with plotting support
pip install -e ".[plot]"
```

## Usage

### As a library

```python
from bls_data import BLSClient, fetch_bls_data
from bls_data.parser import parse_results_to_df

client = BLSClient()
data = client.fetch(["CUUR0000SA0"], start_year=2020, end_year=2024)
df = parse_results_to_df(data)
print(df.head())
```

### MCP server

```bash
# configure .env with BLS_API_KEY_0=your_key
python -m bls_data.server
```

**Available tools:** `get_series`, `get_series_info`, `search_series`, `analyze_cpi_seasonality`

### CLI (legacy)

```bash
python -m data_extraction.main CUUR0000SA0 --start 2020 --end 2024
```

## API key

Register at https://www.bls.gov/developers/api_signature_v2.htm. Add to `.env`:

```
BLS_API_KEY_0=your_key_here
BLS_API_KEY_1=another_key  # optional — keys rotate randomly
```

## Structure

```
bls_data/
├── src/bls_data/
│   ├── client.py    # BLS API v2 client — chunking, retries, key rotation
│   ├── parser.py    # JSON → pandas DataFrame
│   ├── mapping.py   # Human-readable aliases → series IDs
│   ├── cpi.py       # CPI series code helpers
│   ├── api_key.py   # Random key rotation from .env
│   └── server.py    # FastMCP server
├── tests/
├── cu_series/       # CPI master list (CSV)
├── scripts/         # CPI extraction utilities
└── pyproject.toml
```

## License

MIT