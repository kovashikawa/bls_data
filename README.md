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
python score.py --adapter models/bls-agent-v10 # report held-out accuracy
```

### Results

Measured on 43 held-out *phrasings* — wordings absent from training, though every
concept they reference is present (see "How the split works").

Mean over **5 training seeds** ± one sd across seeds. Single-run numbers are not
meaningful here — see "Report distributions" below.

| | tool | entity | exact |
|---|---|---|---|
| **fine-tuned, emits item names (5-seed mean)** | 99.1% ±1.3 | **94.4% ±1.3** | **94.4% ±1.3** |
| earlier version, emitted raw series IDs | 99.1% ±1.3 | 89.8% ±2.1 | 88.8% ±3.0 |
| base Qwen3-1.7B | 72.1% | 9.3% | 9.3% |
| BM25 over 400 item names, *no model* | — | — | 84.4% |

- **tool** — correct tool chosen (6-way). The easy part.
- **entity** — tool + the load-bearing argument (`series_id`/`query`/`survey`), ignoring dates.
- **exact** — every argument identical; spurious `start`/`end` count as wrong.

The model emits **item names**, not series IDs — `get_series(item="Food at home")`
— and `bls_data.items.resolve_item` maps back to the ID. That change alone is
worth +5.6pp (t=3.8, p≈0.005) and more than halves seed variance. See "Why item
names".

The last row is the baseline this project has to justify itself against.

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

### Non-obvious findings

**Mask the prompt.** This data is heavily short-completion: mean 134 prompt tokens
vs 19 completion tokens (generation ratio 0.141), with an identical system prompt
in every row. With `mask_prompt: false`, **87.7% of the loss was the model
re-predicting a fixed preamble** — which is why train loss looked implausibly low
and why val loss appeared to decouple from accuracy:

| | unmasked | masked |
|---|---|---|
| pick checkpoint by val-loss minimum | 37.2% | 86.8% |
| best checkpoint available | 90.7% | 88.4% |
| **penalty for trusting val loss** | **~50pt** | **1.6pt** |

An earlier version of this README claimed "do not early-stop on val loss" as a
property of structured-output tasks. That was wrong — it was this config bug, and
it's a documented short-completion SFT failure mode (Huerta-Enochian & Ko, EMNLP
2024). Once the loss measures the completion, standard practice works.

`train.py` still selects on val **accuracy** rather than val loss, which costs
little and is robust either way. It selects on val, not test — choosing by test
accuracy is selection on the set you then report.

**Report distributions, not runs.** Run-to-run sd is ~1.3pp now; it was ~3pp
before item-name targets and ~6pp before prompt masking. A 14-point spread across seeds previously led to diagnosing a
14-point "regression" from a change that was actually correct. `train.py --seed`
exists for this; use ≥3.

**Score on a quiet machine.** Evaluating concurrently with a training job on the
same GPU returns *different numbers for identical weights*. Two isolated runs are
byte-identical; under Metal memory pressure results silently change.

**Expansion has sharply diminishing returns.** The synthetic expander mostly emits
`"Show me data for series CUUR0000SAF1."` rows that echo an ID already present in the
question, teaching nothing about concept→ID. It is capped at 2 per seed so real
phrasings dominate.

### Why item names

Asking a 1.7B model to recall `CUUR0000SAF11` makes every residual error a
one-character sibling confusion — `SAF11` (food at home) vs `SAF1` (food), `SAM`
(medical care) vs `SEMD` (hospital services). No training config fixes that; the
output format was wrong.

The catalogue is not 8,103 independent series. It is ~400 distinct items repeated
across area and seasonal-adjustment combinations, and restricted to US city
average NSA the item name is a **unique key** over 400 rows. So the model can name
the item and code can resolve it:

| target format | exact (5 seeds) |
|---|---|
| `get_series(series_id="CUUR0000SAF11")` | 88.8% ±3.0 |
| `get_series(item="Food at home")` | 92.1% ±1.3 |
| + hierarchy-aware resolver | **94.4% ±1.3** |

`resolve_item` prefers the general category when a bare term is a whole-word
prefix of several ("tobacco" → *Tobacco and smoking products*, not *Tobacco
products other than cigarettes*), and returns `None` rather than guessing between
unrelated items — a loud failure beats silently fetching a sibling series.

Remaining errors are two vocabulary gaps, consistent across every seed:
"healthcare" → *Medical care*, "OER" → *Owners' equivalent rent*. An alias table
would fix both. It is deliberately **not** added: those two are known only from
inspecting test failures, so adding them would be fitting the test set.

### Where this should go

Measured on the same 43 held-out questions, retrieving over those 400 item names:

| query source | recall@1 | recall@5 |
|---|---|---|
| oracle (gold item name) | 100% | 100% |
| raw user question, no model | **84.4%** | 93.8% |
| base Qwen3-1.7B rewrites it | 75.0% | 84.4% |
| this fine-tuned model rewrites it | 62.5% | 71.9% |

Two conclusions. The retriever has no ceiling problem, and a zero-model BM25
baseline nearly matches the fine-tune. But every model in the chain currently
makes retrieval *worse* — this adapter emits series IDs when asked for a search
phrase, having specialized away its ability to paraphrase. So retrieval cannot be
bolted on; it needs training from base on two-step traces.

Next steps, in order:

1. **Enumerate the catalogue in context.** All 400 item names fit in ~2,274
   tokens. That removes recall entirely — the model selects from a visible list
   rather than remembering. Needs `max_seq_length` raised from 2048.
2. **A general alias table** for user vocabulary → catalogue vocabulary, built
   from domain knowledge rather than from test failures, and validated on val.
3. **BM25 / retrieval** only if the catalogue outgrows context. It is the answer
   to "the list does not fit", which is not currently the problem — and it
   *requires* the query-formulation step measured above as harmful.

Judge anything new against **both** 84.4% (no model at all) and 94.4% (current).

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
| `models/bls-agent-v10/` | the adapter (`adapter_config.json` records which checkpoint and why) |

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