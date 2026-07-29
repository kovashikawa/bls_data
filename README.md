# bls_data

Python toolkit for U.S. Bureau of Labor Statistics time-series data — API client,
parser, MCP server, and a fine-tuned local model that turns questions into tool
calls.

## Install

```bash
pip install -e .
pip install -e ".[dev]"     # dev dependencies
pip install -e ".[plot]"    # plotting (needed by analyze_cpi_seasonality)
```

## API key

Register at <https://www.bls.gov/developers/api_signature_v2.htm>, then add to `.env`:

```
BLS_API_KEY_0=your_key_here
BLS_API_KEY_1=another_key   # optional — any BLS_API_KEY_* is used, chosen at random
```

## Usage

### MCP server

```bash
python -m bls_data.server
```

To register with Claude Code (MCP servers start in an arbitrary working directory,
so pass the key explicitly):

```bash
claude mcp add bls -e BLS_API_KEY_0=<your_key> \
  -- /path/to/bls_data/.venv/bin/python -m bls_data.server
```

**Tools:** `get_series`, `get_series_info`, `search_series`, `list_surveys`,
`popular_series`, `analyze_cpi_seasonality`.

`get_series`, `get_series_info` and `analyze_cpi_seasonality` take an everyday
**item name**. You never need a series ID:

```python
get_series(item="groceries", start="2024")   # → CUUR0000SAF11  Food at home
get_series(item="healthcare")                # → CUUR0000SAM    Medical care
get_series(item="public transport")          # → did_you_mean: [Public transportation, …]
get_series(series_id="CUUR0000SA0")          # raw IDs still accepted
```

Resolution is exact-match first, then an alias table for everyday vocabulary
(`groceries`, `gas`, `core CPI`, `airfare`, `jobs`), then a whole-word prefix
preference that favours the general category (`tobacco` → *Tobacco and smoking
products*). An ambiguous name returns **ranked candidates rather than a guess** —
fetching a sibling series would return plausible numbers that are wrong. A name
matching nothing says so.

`search_series` ranks the 400 US-city-average, not-seasonally-adjusted items by
BM25 and returns names usable directly as `item=`. Pass `scope="all"` to scan the
full ~8,100-row catalog instead, including regional and seasonally-adjusted series.

### As a library

```python
from dotenv import load_dotenv; load_dotenv(".env")
from bls_data.server import get_series, search_series
from bls_data.items import resolve_item, search_items

get_series(item="Food at home", start="2023", end="2024")
resolve_item("healthcare")        # → 'CUUR0000SAM'
search_items("gasoline", limit=3) # → ranked Candidate(series_id, item_name, score)
```

Lower level, if you want the raw client:

```python
from bls_data import BLSClient
from bls_data.parser import parse_results_to_df

df = parse_results_to_df(BLSClient().fetch(["CUUR0000SA0"], start_year=2020, end_year=2024))
```

### CLI (legacy)

```bash
python -m data_extraction.main CUUR0000SA0 --start 2020 --end 2024
```

## Fine-tuned agent

A 67 MB LoRA adapter over Qwen3-1.7B that maps a natural-language question to one
tool call. Runs on Apple Silicon via MLX. Optional — the MCP server works fine
driven by any model.

```bash
python build_dataset.py                        # seeds → splits + mlx_data_clean/
python train.py --seed 0                       # ~12 min on an M4
python score.py --adapter models/bls-agent-v10
```

Example output:

```
"How have grocery prices moved since 2022?"  →  get_series(item="Food at home", start="2022")
"Analyze seasonality in gas prices."         →  analyze_cpi_seasonality(item="Gasoline (all types)")
```

### Results

43 held-out phrasings — wordings absent from training, though every concept they
reference appears in it. Mean over **5 training seeds**, ± one sd across seeds.

| | tool | entity | exact |
|---|---|---|---|
| **fine-tuned** | **99.1% ±1.3** | **94.4% ±1.3** | **94.4% ±1.3** |
| fine-tuned, with resolver alias table | 99.1% ±1.3 | 96.7% ±1.3 | 96.7% ±1.3 |
| base Qwen3-1.7B | 72.1% | 9.3% | 9.3% |
| BM25 over 400 item names, no model | — | — | 84.4% |

- **tool** — correct tool chosen, out of six.
- **entity** — plus the load-bearing argument (item / query / survey), ignoring dates.
- **exact** — every argument identical.

Quote **94.4%** as the model's figure. The alias row is the same five adapters
with a better resolver, and its +2.3pp is a single test item whose alias was
written knowing that item failed; the rest of the alias table moves this eval by
zero. The last row is the baseline the model has to beat, and does.

### Design

**Targets are item names, not series IDs.** `get_series(item="Food at home")`, with
`bls_data.items.resolve_item` mapping to the ID. The catalog is ~400 distinct items
repeated across area and seasonal-adjustment combinations; restricted to US city
average NSA, the item name is a unique key. Naming the item makes near-misses
semantically distinct rather than one character apart, and lets a wrong answer fail
loudly instead of silently fetching a sibling.

**The split holds out phrasings, not concepts.** This is a lookup task — `"medical
care" → CUUR0000SAM` can only be recalled, not derived — so every concept
contributes at least one phrasing to training and the remaining phrasings are held
out. `seed_dataset.py` is concept tables: 205 seeds over 82 concepts, ≥2 phrasings
each, giving 229 / 50 / 76 rows.

**The build fails rather than producing wrong data.** It refuses to write if any
series ID is absent from the bundled catalog, any example appears in two splits,
any concept is held out entirely, or any label carries dates its question never
mentions.

**Training config that matters:** `mask_prompt: true` (the data is short-completion
— 134 prompt tokens against 19 completion, with an identical system prompt every
row, so unmasked loss is ~88% preamble), LoRA rank 16 across all 28 layers, cosine
schedule with warmup, 800 iterations, checkpoint selected on val accuracy.

### Running it yourself

- **Use ≥3 seeds for any comparison.** Run-to-run sd is ~1.3pp. `train.py --seed`.
- **Score on an otherwise-idle machine.** Evaluating while a training job holds the
  GPU returns different numbers for identical weights; isolated runs are
  byte-identical.
- **`train.py` refuses to run** if `mlx_data_clean/` is older than
  `seed_dataset.py` or `build_dataset.py`.
- **MLX / Apple Silicon only.** GGUF export needs a fused model (`mlx_lm.fuse`) and
  is not wired up.

### Limitations

- **One tool call per turn, by design.** An MCP host runs the call → result → call
  loop, so the useful contract is picking the right single call. Multi-step is not
  trained.
- **35 concepts were taught.** Anything outside them depends on the resolver and
  `search_series`, not on the model.
- **The model can hallucinate an item name** that resolves to the wrong series.
  Known case: "OER" → *Education and communication*. No resolver can repair that.
- **The expander is capped at 2 rows per seed.** Beyond that it mostly emits rows
  echoing an ID already in the question, which teaches nothing.

### Next

1. **Enumerate the catalog in context** — all 400 item names fit in ~2,274 tokens,
   removing recall from the problem. Needs `max_seq_length` above 2048.
2. **Extend the alias table** from domain vocabulary, validated on val.
3. **Retrieval / two-step calling** only if the catalog outgrows the context window.

Judge anything new against both 84.4% (no model) and 94.4% (current).

## Layout

```
bls_data/
├── src/bls_data/
│   ├── client.py     # BLS API v2 — chunking, retries, key rotation
│   ├── parser.py     # JSON → pandas DataFrame
│   ├── items.py      # item name → series ID; aliases, BM25 search
│   ├── mapping.py    # human-readable aliases → series IDs
│   ├── cpi.py        # CPI series code helpers
│   ├── api_key.py    # random key rotation from .env
│   └── server.py     # FastMCP server, 6 tools
├── seed_dataset.py   # concept tables → seeds
├── build_dataset.py  # splits, validation, mlx_data_clean/
├── train.py          # MLX LoRA + checkpoint selection
├── score.py          # held-out evaluation
├── experiments_retrieval.py  # retrieval feasibility study (not in serving path)
├── models/           # current adapter only; superseded ones are deleted
├── cu_series/        # CPI master list (CSV)
├── scripts/          # CPI extraction utilities
└── tests/
```

## License

MIT
