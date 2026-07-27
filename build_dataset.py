"""
Fixed data pipeline — proper seed-lineage train/val/test split.
Fixes all 6 issues from the adversarial review.

Usage:
    python build_dataset.py
    # Generates: train_clean.jsonl, val_clean.jsonl, test_clean.jsonl
"""

import json
import random
import os
from pathlib import Path

# Load seed data
from seed_dataset import SEED_DATA

random.seed(42)

# ── Step 1: Split seeds BEFORE expansion (fixes data leakage) ──
# Hold out 15 seeds for testing — these NEVER enter training
indices = list(range(len(SEED_DATA)))
random.shuffle(indices)

test_indices = set(indices[:15])      # 15 seeds → test (never seen)
val_indices = set(indices[15:25])     # 10 seeds → validation
train_indices = set(indices[25:])     # 45 seeds → training

test_seeds = [SEED_DATA[i] for i in sorted(test_indices)]
val_seeds = [SEED_DATA[i] for i in sorted(val_indices)]
train_seeds = [SEED_DATA[i] for i in sorted(train_indices)]

print(f"Split: {len(train_seeds)} train / {len(val_seeds)} val / {len(test_seeds)} test")

# ── Step 2: Expand only training seeds ──
# Generate variations from the 45 training seeds only
VALID_SERIES_IDS = [
    "CUUR0000SA0", "CUUR0000SAF1", "CUUR0000SA0E", "CUUR0000SAH",
    "CUUR0000SAM", "CUUR0000SAT", "CUUR0000SAA", "CUUR0000SA0L1E",
    "CUUR0000SAE", "CUUR0000SAR", "CUUR0000SAC", "CUUR0000SAS",
    "CUUR0000SAF11", "CUUR0000SEFV", "CUUR0000SETA01", "CUUR0000SETA02",
    "CUUR0000SETB01", "CUUR0000SEHF01", "CUUR0000SEHA", "CUUR0000SEHC01",
    "CUUR0000SETG01",  # Airline fares (a real series — just not "energy")
    "LNS14000000", "LNS11300000", "LNS12300000",
    "CES0000000001", "CES0500000003",
]


def _assert_ids_exist():
    """Fail the build if any CPI id is absent from the bundled master catalog.

    The V2 data shipped four ids that exist in no BLS catalog (SAR1/SAC1/SAS1/
    SEHA01) and several that named the wrong concept. Checking here means a bad
    id can never reach a training file again.
    """
    import sys
    sys.path.insert(0, "src")
    import pandas as pd
    from bls_data.cpi import _MASTER_PATH

    catalog = set(pd.read_csv(_MASTER_PATH, dtype=str)["series_id"])
    seed_ids = {
        sid
        for seed in SEED_DATA
        for key in ("arguments", "next_arguments")
        if (sid := seed.get(key, {}).get("series_id"))
    }
    missing = sorted(
        s for s in set(VALID_SERIES_IDS) | seed_ids
        if s.startswith("CU") and s not in catalog
    )
    if missing:
        raise SystemExit(f"Series ids not in CPI catalog: {missing}")
    print(f"✓ {len(seed_ids)} seed ids + {len(VALID_SERIES_IDS)} pool ids validated against catalog")


_assert_ids_exist()
VALID_SURVEYS = ["CU", "CE", "LN", "PC", "PR", "JT", "LE", "EN", "OE", "SM", "LA"]
SEARCH_TERMS = [
    "dairy", "airline", "prescription", "tuition", "beef", "alcoholic beverages",
    "internet", "hotel", "tobacco", "baby food", "coffee", "bread", "eggs",
    "milk", "butter", "chicken", "fish", "cereal", "wine", "beer",
    "rent", "mortgage", "utilities", "phone", "cable", "streaming",
    "furniture", "appliances", "clothing", "shoes", "jewelry", "fuel oil",
]
LIST_SURVEYS_VARIANTS = [
    "What surveys are available from the BLS?",
    "List all economic data programs at the Bureau of Labor Statistics.",
    "What BLS data categories exist?",
    "Show me all BLS survey abbreviations.",
    "What types of economic data does BLS track?",
]


# ── Step 1b: Partition the expansion pools per split ──
# Splitting seeds alone was not enough. expand_seed used to draw from these
# module-level pools regardless of which split the seed belonged to, so a train
# seed and a test seed could emit byte-identical rows ("Search for BLS series
# about furniture." => search_series(query="furniture")). That put 4/30 test and
# 13/38 val rows verbatim into train. Giving each split its own disjoint slice
# makes an identical row impossible to construct.

def _partition(items, salt):
    """Deterministically cut a pool into disjoint train/val/test slices.

    Proportions track the seed split (45/10/15 → 64/14/21%). `salt` gives each
    pool an independent shuffle so the same index doesn't land in the same split
    across every pool. Uses a private Random so it cannot perturb the global
    stream that drives the seed shuffle above.
    """
    items = sorted(items)
    random.Random(salt).shuffle(items)
    n_test = max(1, round(len(items) * 0.21))
    n_val = max(1, round(len(items) * 0.14))
    return {
        "test": items[:n_test],
        "val": items[n_test:n_test + n_val],
        "train": items[n_test + n_val:],
    }


_SERIES = _partition(VALID_SERIES_IDS, "series")
_SURVEYS = _partition(VALID_SURVEYS, "surveys")
_TERMS = _partition(SEARCH_TERMS, "terms")
_VARIANTS = _partition(LIST_SURVEYS_VARIANTS, "variants")

POOLS = {
    split: {
        "series_ids": _SERIES[split],
        "surveys": _SURVEYS[split],
        "search_terms": _TERMS[split],
        "list_variants": _VARIANTS[split],
    }
    for split in ("train", "val", "test")
}

for _s in ("train", "val", "test"):
    _p = POOLS[_s]
    print(f"  pool[{_s:5s}] {len(_p['series_ids'])} ids, {len(_p['surveys'])} surveys, "
          f"{len(_p['search_terms'])} terms, {len(_p['list_variants'])} phrasings")

# Qwen3 non-thinking system prompt (FIX: suppresses thinking mode)
SYSTEM_PROMPT = """You are a BLS economic data assistant.
Available tools:
- get_series(series_id, start?, end?) — Fetch time-series data
- list_surveys() — List all BLS surveys
- popular_series(survey?) — Popular series for a survey
- search_series(query, limit?) — Search CPI catalog
- get_series_info(series_id) — Series metadata
- analyze_cpi_seasonality(series_id, start?, end?) — Seasonality analysis
Respond ONLY with the correct tool call. Do NOT use thinking or reasoning."""


def expand_seed(seed, target_count_per_seed=6, pool=None):
    """Generate variations of a seed example, drawing only from `pool`.

    `pool` is this split's disjoint slice of the expansion pools. Passing the
    module-level pools here is what caused the cross-split duplicates.
    """
    pool = pool or POOLS["train"]
    valid_series_ids = pool["series_ids"]
    examples = [dict(seed)]  # Include the original
    tool = seed["tool"]
    args = dict(seed["arguments"])

    if tool == "get_series":
        years = [("2018", "2022"), ("2019", "2023"), ("2020", "2025"),
                 ("2021", "2024"), ("2022", "2025")]
        for start, end in random.sample(years, min(3, len(years))):
            if start >= end:
                continue
            new_args = {**args, "start": start, "end": end}
            new_q = seed["question"].replace(
                args.get("start", "2020"), start
            ).replace(args.get("end", "2024"), end)
            examples.append({"question": new_q, "tool": tool, "arguments": new_args})

        # Variations with different but related series IDs
        current_sid = args.get("series_id", "")
        related = [s for s in valid_series_ids
                   if s.startswith(current_sid[:2]) and s != current_sid]
        for sid in random.sample(related, min(2, len(related))):
            examples.append({
                "question": f"Show me data for series {sid}.",
                "tool": tool,
                "arguments": {**args, "series_id": sid},
            })

    elif tool == "popular_series":
        surveys = random.sample(pool["surveys"], min(4, len(pool["surveys"])))
        for s in surveys:
            examples.append({
                "question": f"What are the popular series in the {s} survey?",
                "tool": tool,
                "arguments": {"survey": s},
            })

    elif tool == "search_series":
        terms = random.sample(pool["search_terms"], min(4, len(pool["search_terms"])))
        for term in terms:
            examples.append({
                "question": f"Search for BLS series about {term}.",
                "tool": tool,
                "arguments": {"query": term},
            })

    elif tool == "get_series_info":
        current = args.get("series_id", "")
        others = [s for s in valid_series_ids if s != current]
        for sid in random.sample(others, min(3, len(others))):
            examples.append({
                "question": f"What information do you have on series {sid}?",
                "tool": tool,
                "arguments": {"series_id": sid},
            })

    elif tool == "analyze_cpi_seasonality":
        cpi_ids = [s for s in valid_series_ids if s.startswith("CU")]
        current = args.get("series_id", "")
        others = [s for s in cpi_ids if s != current]
        for sid in random.sample(others, min(3, len(others))):
            examples.append({
                "question": f"Analyze the seasonal pattern of series {sid}.",
                "tool": tool,
                "arguments": {"series_id": sid},
            })

    elif tool == "list_surveys":
        # list_surveys takes no arguments, so the only thing that can vary is the
        # phrasing — which makes it the pool most prone to cross-split collision.
        variants = pool["list_variants"]
        for q in random.sample(variants, min(3, len(variants))):
            examples.append({"question": q, "tool": tool, "arguments": {}})

    return examples[:target_count_per_seed]


def format_training_example(ex):
    """Format a seed example as a ChatML training string with /no_think."""
    user_msg = ex["question"]
    tool = ex["tool"]
    args = ex["arguments"]
    args_str = ", ".join(f'{k}="{v}"' if isinstance(v, str) else f"{k}={v}"
                         for k, v in args.items())
    assistant_msg = f"{tool}({args_str})"

    return {
        "text": (
            f"<|im_start|>system\n{SYSTEM_PROMPT}<|im_end|>\n"
            f"<|im_start|>user\n{user_msg}<|im_end|>\n"
            f"<|im_start|>assistant\n"
            f" {assistant_msg}<|im_end|>"
        )
    }


# ── Step 3: Build datasets ──
def build_split(seeds, name, split, target=300):
    """Build a dataset split from seeds, expanding only from `split`'s pools."""
    examples = []
    per_seed = max(1, target // len(seeds)) if seeds else 1
    for seed in seeds:
        examples.extend(expand_seed(seed, per_seed, pool=POOLS[split]))
    random.shuffle(examples)

    # Deduplicate. Shared pools plus random.sample let one split re-emit the same
    # (question, call) many times over — train was 46/223 duplicates. Dedup here
    # keeps the reported example count honest about how much distinct signal
    # there actually is.
    formatted, seen = [], set()
    for ex in examples:
        item = format_training_example(ex)
        if item["text"] in seen:
            continue
        seen.add(item["text"])
        formatted.append(item)
        if len(formatted) == target:
            break

    path = f"{name}.jsonl"
    with open(path, "w") as f:
        for item in formatted:
            f.write(json.dumps(item) + "\n")
    print(f"  {name}: {len(formatted)} unique examples ({len(examples)} generated) → {path}")
    return formatted


print("\nBuilding datasets...")
train = build_split(train_seeds, "train_clean", "train", target=300)
val = build_split(val_seeds, "val_clean", "val", target=40)
test = build_split(test_seeds, "test_clean", "test", target=40)

# ── Step 3b: Assert the split actually holds ──
_sets = {n: {i["text"] for i in s} for n, s in [("train", train), ("val", val), ("test", test)]}
for _a, _b in [("test", "train"), ("val", "train"), ("test", "val")]:
    if overlap := _sets[_a] & _sets[_b]:
        raise SystemExit(f"{len(overlap)} rows shared between {_a} and {_b}:\n  " +
                         "\n  ".join(sorted(overlap)[:3]))
print("✓ verified: no example appears in more than one split")

# ── Step 4: Save raw seed splits for evaluation ──
for name, seeds in [("test_seeds", test_seeds), ("val_seeds", val_seeds)]:
    with open(f"{name}.json", "w") as f:
        json.dump(seeds, f, indent=2)
    print(f"  {name}: {len(seeds)} seeds → {name}.json")

print(f"\n✓ Dataset built. Training: {len(train)}, Val: {len(val)}, Test: {len(test)}")
print(f"✓ Test SEEDS never appear in training data (seed-lineage split)")
print(f"✓ Expansion POOLS are partitioned per split, so no expanded example can")
print(f"  be shared either — asserted above, not just intended.")