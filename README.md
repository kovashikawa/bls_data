# bls_data

> A clean, modular Python toolkit for downloading, mapping and analysing U.S. Bureau of Labor Statistics (BLS) time‑series data

The bls_data repository provides a set of scripts and Python modules to streamline working with the BLS Public Data API. It handles API authentication, request chunking, error‑handling, series‑ID mapping, CPI metadata download/parsing and tidy conversion of the results into pandas DataFrames.

The project currently focuses on time‑series data from the v2 BLS timeseries API and includes helpers to retrieve Consumer Price Index (CPI) series codes and build a master list of CPI series. It can be used as a library inside your own projects or via its command‑line interface.

## Key features

- **BLS API client with retries** – The BLSClient class encapsulates API calls, setting sensible request limits (50 series per call, 20 years per call), handling retries on HTTP 429/5xx errors and automatically adding your API key. The client exposes a fetch() method that assembles payloads and merges results across chunks.

- **Random API key loading** – The api_key.py module loads environment variables from a .env file and picks one of the keys starting with BLS_API_KEY_ at random. This allows rotation among multiple API keys without changing the code (see get_random_bls_key() in api_key.py).

- **Human‑friendly series mapping** – The mapping_loader.py module can read CSV or JSON mapping files to translate human‑readable aliases (e.g. cpi_all_items) into official BLS series IDs. It normalises keys to be case‑ and punctuation‑insensitive and supports one‑to‑many mappings. The helper resolve_series_ids() also recognises dynamic CU prefixes (e.g. CU:area_code=0000) for CPI series.

- **Tidy DataFrame parsing** – The data_parser.py module converts the JSON API response into a tidy pandas DataFrame. It flattens nested series, adds optional catalogue fields (survey name, measure, area, item), joins any aliases and extracts footnotes.

- **High‑level convenience function** – get_bls_data() wraps series resolution, API calls and parsing into a single function. You pass a list of aliases or series IDs along with optional date range and flags for metadata, calculations or annual averages; it returns a ready‑to‑use DataFrame.

- **Command‑line interface** – Running python -m bls_data.data_extraction.main exposes a CLI that accepts series codes/aliases and options such as --start, --end, --catalog, --calculations, --annualaverage, --aspects and --out. The CLI prints a sample of the resulting DataFrame or saves it to CSV.

- **Example script** – example.py demonstrates fetching multiple series (CPI All Items, CES All Employees, Unemployment Rate) by alias and displaying the first and last rows. It shows how to handle missing mappings and print the resulting table.

- **CPI master list creation** – The cu_series/cu_download.py script downloads the CPI metadata files (cu.series, cu.item, cu.area), merges them and outputs a master CSV called cpi_series_master_list.csv. It uses the curl command to avoid TLS issues and logs progress.

- **CPI series code helper** – The cu_series/cu_series_codes.py module provides a function get_cu_series_codes() that reads the master CSV and returns CPI series IDs filtered by area or item code. For example, you can retrieve all series for the U.S. city average or All Items by passing filters such as {"area_code": "0000", "item_code": "SA0"}.

## Installation

1. Clone this repository:
```bash
git clone https://github.com/kovashikawa/bls_data.git
cd bls_data
```

2. Create a virtual environment (recommended) and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows use 'venv\Scripts\activate'
```

3. Install dependencies from requirements.txt:
```bash
pip install -r requirements.txt
```

### Configure API keys

Create a `.env` file in the project root with one or more BLS API keys. Keys must be prefixed with `BLS_API_KEY_` and can be numbered arbitrarily, for example:

```env
BLS_API_KEY_0=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
BLS_API_KEY_1=yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy
```

The library will load all such keys and pick one at random for each session.

## Usage

### From Python

Import the `get_bls_data` function and pass one or more series IDs or aliases:

```python
from bls_data.data_extraction.main import get_bls_data

# Fetch CPI (All Items), CES (All Employees) and the Unemployment Rate
# between 2018 and 2023, including catalogue metadata
aliases = [
    "cpi_all_items",      # Consumer Price Index for All Urban Consumers
    "ces_all_employees",  # All Employees, Total Nonfarm
    "unemployment_rate"   # Civilian Unemployment Rate
]

df = get_bls_data(
    codes_or_ids=aliases,
    start_year=2018,
    end_year=2023,
    catalog=True
)

print(df.head())
```

By default the helper will look for a `code_mapping.csv/.json` file in the data_extraction folder to translate aliases to series IDs. You can override the mapping path with the `mapping_path` argument or pass series IDs directly.

### Command line

To fetch data directly from the terminal you can run the main script. Here is an example fetching the same three series and saving the output to a CSV file:

```bash
python -m bls_data.data_extraction.main \
  cpi_all_items ces_all_employees unemployment_rate \
  --start 2018 --end 2023 \
  --catalog \
  --out data/bls_data.csv
```

This will create `data/bls_data.csv` containing a tidy table with columns such as `series_id`, `alias`, `year`, `period`, `period_name`, `value`, `seasonality`, `series_title`, `survey_name`, `area`, `item`, and `footnotes`.

### Building a CPI series master list

If you need to explore the available CPI series, run the download script:

```bash
python -m bls_data.cu_series.cu_download
```

This will download the BLS CPI metadata files, merge them and write `cpi_series_master_list.csv` in the current directory. You can then query this file via `cu_series_codes.get_cu_series_codes()` to retrieve series IDs matching certain area and item codes.

## Repository structure

```
bls_data/
├── cu_series/
│   ├── cu_download.py      # download & merge CPI metadata into master CSV
│   ├── cu_series_codes.py  # helper to filter CPI series IDs
│   ├── cpi_series_master_list.csv (generated)
│   └── __init__.py
├── data_extraction/
│   ├── api_key.py          # loads random BLS API key
│   ├── bls_client.py       # BLS API client with retries
│   ├── data_parser.py      # converts JSON responses to DataFrames
│   ├── mapping_loader.py   # load & resolve alias mappings
│   ├── main.py             # high‑level `get_bls_data()` & CLI
│   ├── example.py          # demonstration script
│   ├── code_mapping.csv    # sample alias → series mapping
│   └── __init__.py
├── LICENSE
├── README.md (this file)
└── requirements.txt
```

## Contributing

Contributions are welcome! If you'd like to report a bug, request a feature or submit a pull request:

1. Fork this repository and create a new branch.
2. Make your changes following the existing code style and add tests/examples where appropriate.
3. Submit a pull request describing your changes and referencing any issues.

If you encounter issues with the BLS API or need help finding series IDs, feel free to open an issue.

## License

This project is licensed under the MIT License.
