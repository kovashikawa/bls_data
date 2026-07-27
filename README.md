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

**Available tools:** `get_series`, `get_series_info`, `search_series`, `list_surveys`,
`popular_series`, `analyze_cpi_seasonality`

Note that `search_series` searches the bundled CPI master catalog only (~8,100 series),
not all of BLS. Use `list_surveys` and `popular_series` for broader discovery.

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

## Fine-tuned agent (distillation)

A LoRA adapter over Qwen3-1.7B that maps a natural-language question to the right
MCP tool call. Runs on Apple Silicon via MLX.

```bash
python build_dataset.py                       # seeds -> splits + mlx_data_clean/
python train.py                               # train + select checkpoint on val
python score.py --adapter models/bls-agent-v7 # report held-out accuracy
```

### Results

Measured on 43 held-out *phrasings* — wordings absent from training, though every
concept they reference is present (see "How the split works").

| | tool | entity | exact |
|---|---|---|---|
| fine-tuned (600 iters) | 100% | 93.0% | 90.7% |
| base Qwen3-1.7B | 72.1% | 9.3% | 9.3% |

- **tool** — correct tool chosen (6-way). The easy part.
- **entity** — tool + the load-bearing argument (`series_id`/`query`/`survey`), ignoring dates.
- **exact** — every argument identical; spurious `start`/`end` count as wrong.

Read the headline as roughly **86–91%**: binomial se at n=43 is ±4.5pt, and
run-to-run variance is ~5pt (an identically-configured rerun scored 86.0%).

### How the split works

This is a **lookup** task — `"medical care" -> CUUR0000SAM` cannot be derived, only
recalled. So the split holds out *phrasings*, not concepts:

- `seed_dataset.py` is concept tables: 205 seeds over 82 concepts, ≥2 phrasings each.
- Every concept contributes at least one phrasing to train; the rest go to val/test.
- The build **fails** if any concept is held out entirely, if any series ID is absent
  from the CPI catalog, or if any example appears in more than one split.

Earlier versions held out whole seeds, which put whole concepts in test — 4 of 11
scored items asked for a series ID that appeared nowhere in training and were
unanswerable by construction.

### Two non-obvious findings

**Do not early-stop on val loss.** It bottoms near iter 250 and rises, while task
accuracy keeps climbing to ~600. Stopping at the val-loss minimum costs ~50 points
of exact match:

| iter | 200 | 400 | **600** | 800 | 1000 | 1200 | 1400 |
|---|---|---|---|---|---|---|---|
| val loss | .135 | .135 | .147 | .156 | .169 | .168 | .169 |
| exact | 37.2% | 65.1% | **90.7%** | 81.4% | 88.4% | 83.7% | 74.4% |

`train.py` therefore selects checkpoints on val **accuracy**. It selects on val, not
test — choosing by test accuracy is selection on the set you then report.

**Expansion has sharply diminishing returns.** The synthetic expander mostly emits
`"Show me data for series CUUR0000SAF1."` rows that echo an ID already present in the
question, teaching nothing about concept→ID. It is capped at 2 per seed so real
phrasings dominate.

### Not supported: multi-step calls (deliberate)

The model emits exactly one tool call per turn. This is a decision, not an
oversight: an MCP host already runs the call → result → call loop, so the useful
contract is "pick the right single call given the conversation so far".

An earlier version had six compound seeds ("search for rent series, **then** get
data for the first result"). They were removed because they were mislabeled by
construction — the training target hardcoded `series_id="CUUR0000SEHA01"` as the
"first result", which the model cannot know without seeing the search output. It
taught guessing a plausible ID rather than reading one. (They were also inert:
the formatter only ever emitted the first call, so the second was silently
discarded.)

Adding real multi-step means one of two things:

- **Independent calls only** — compound questions whose calls are all knowable
  upfront ("CPI and unemployment for 2024"). Needs an output format for N calls
  and a set-comparison metric in `score.py`. Tractable.
- **Dependent steps** — requires actual tool results in the training context,
  which means capturing live BLS responses and a strategy for truncating large
  `get_series` payloads. A separate project.

### Files

| file | role |
|---|---|
| `seed_dataset.py` | concept tables → `SEED_DATA` (205 seeds / 82 concepts) |
| `build_dataset.py` | split, expand, validate; writes `*_clean.jsonl` + `mlx_data_clean/` |
| `train.py` | wrapper over `mlx_lm.lora` + val-accuracy checkpoint selection |
| `score.py` | tool/entity/exact metrics; `--split {test,val}` |
| `models/bls-agent-v7/` | the adapter (`adapter_config.json` records which checkpoint and why) |

`models/` holds only the current adapter. Superseded ones are deleted rather than
kept — everything is reproducible from the committed pipeline, and the older
adapters were trained on data with known-wrong labels. Intermediate LoRA
checkpoints are gitignored; `train.py` selects one and promotes it to
`adapters.safetensors`.

`train.py` refuses to run if `mlx_data_clean/` is older than `seed_dataset.py` or
`build_dataset.py` — training on stale data otherwise fails silently.

Only the MLX/Apple-Silicon path is supported. A previous Unsloth/CUDA path was
removed rather than left untested. GGUF export needs a fused model (`mlx_lm.fuse`)
and is not wired up.

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
├── seed_dataset.py  # distillation: concept tables
├── build_dataset.py # distillation: splits + validation
├── train.py         # distillation: MLX LoRA + checkpoint selection
├── score.py         # distillation: held-out evaluation
├── models/          # LoRA adapters
└── pyproject.toml
```

## License

MIT