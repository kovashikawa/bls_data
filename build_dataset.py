"""
Fixed data pipeline — proper seed-lineage train/val/test split.
Fixes all 6 issues from the adversarial review.

Usage:
    python build_dataset.py
    # Generates: train_clean.jsonl, val_clean.jsonl, test_clean.jsonl
"""

import json
import random
import re
from pathlib import Path
from collections import defaultdict

# Load seed data
from seed_dataset import SEED_DATA


def split_seeds():
    """Split seeds by PHRASING, stratified by concept.

    The previous split held out whole seeds, which meant whole *concepts* landed
    in test: 4 of 11 scored test items asked for a series id that appeared
    nowhere in training. No model can answer those — "medical care" ->
    CUUR0000SAM is a lookup, not something derivable. Every concept now
    contributes at least one phrasing to train; the remaining phrasings are held
    out. So test measures "does it recall the mapping from wording it hasn't
    seen", which is the actual deployed task.
    """
    random.seed(42)

    by_concept = defaultdict(list)
    for seed in SEED_DATA:
        by_concept[seed["concept"]].append(seed)

    train, extras = [], []
    for concept in sorted(by_concept):
        group = list(by_concept[concept])
        random.shuffle(group)
        train.append(group[0])   # every concept is represented in train
        extras.extend(group[1:])

    random.shuffle(extras)
    n_val = round(len(SEED_DATA) * 0.14)
    n_test = round(len(SEED_DATA) * 0.21)
    val, test = extras[:n_val], extras[n_val:n_val + n_test]
    train += extras[n_val + n_test:]

    orphans = {s["concept"] for s in val + test} - {s["concept"] for s in train}
    if orphans:
        raise SystemExit(f"concepts held out entirely (unanswerable): {sorted(orphans)}")

    print(f"Split: {len(train)} train / {len(val)} val / {len(test)} test "
          f"({len(by_concept)} concepts, all present in train)")
    return train, val, test


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


VALID_SURVEYS = ["CU", "CE", "LN", "PC", "PR", "JT", "LE", "EN", "OE", "SM", "LA"]
SEARCH_TERMS = [
    "dairy", "airline", "prescription", "tuition", "beef", "alcoholic beverages",
    "internet", "hotel", "tobacco", "baby food", "coffee", "bread", "eggs",
    "milk", "butter", "chicken", "fish", "cereal", "wine", "beer",
    "rent", "mortgage", "utilities", "phone", "cable", "streaming",
    "furniture", "appliances", "clothing", "shoes", "jewelry", "fuel oil",
]
# list_surveys has no arguments, so the only thing that could vary is phrasing —
# and the phrasings now live in seed_dataset.LIST_SURVEYS, split by concept like
# everything else. Keeping a second copy here re-introduced exactly the bug this
# module is meant to prevent: a val seed's expansion collided with a test seed's
# original. There is nothing left for the expander to add.


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

POOLS = {
    split: {
        "series_ids": _SERIES[split],
        "surveys": _SURVEYS[split],
        "search_terms": _TERMS[split],
    }
    for split in ("train", "val", "test")
}



def _print_pools():
    for s in ("train", "val", "test"):
        p = POOLS[s]
        print(f"  pool[{s:5s}] {len(p['series_ids'])} ids, {len(p['surveys'])} surveys, "
              f"{len(p['search_terms'])} terms")

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
        # Only rewrite years the question actually names.
        #
        # This used to do question.replace(args.get("start", "2020"), start) and
        # then write start/end into the arguments regardless. When the question
        # named no years the replace was a no-op but the arguments still gained
        # dates, producing labels that contradict their own question:
        #   "Pull the all-items consumer price index." -> start=2022, end=2025
        # and open-ended questions gained an end they never asked for:
        #   "Show me services inflation since 2020." -> start=2020, end=2025
        # That taught the model to invent ranges and then penalised it at eval
        # for doing so — 12 of 17 expanded-test failures were the model giving
        # the *more faithful* answer than the label.
        question = seed["question"]
        seed_start, seed_end = args.get("start"), args.get("end")
        vary_start = bool(seed_start) and seed_start in question
        vary_end = bool(seed_end) and seed_end in question

        if vary_start and vary_end and seed_start == seed_end:
            # Single-year question ("...in 2023?"). Keep it a single year.
            for year in random.sample(["2019", "2020", "2021", "2022", "2023"], 3):
                if year == seed_start:
                    continue
                examples.append({
                    "question": question.replace(seed_start, year),
                    "tool": tool,
                    "arguments": {**args, "start": year, "end": year},
                })
        elif vary_start and vary_end:
            ranges = [("2018", "2022"), ("2019", "2023"), ("2020", "2025"),
                      ("2021", "2024"), ("2022", "2025")]
            for start, end in random.sample(ranges, min(3, len(ranges))):
                new_q = question.replace(seed_start, start).replace(seed_end, end)
                if new_q == question:
                    continue
                examples.append({"question": new_q, "tool": tool,
                                 "arguments": {**args, "start": start, "end": end}})
        elif vary_start:
            # Open-ended ("since 2021"). Vary the start; never invent an end.
            for start in random.sample(["2018", "2019", "2020", "2021", "2022"], 3):
                if start == seed_start:
                    continue
                examples.append({
                    "question": question.replace(seed_start, start),
                    "tool": tool,
                    "arguments": {**args, "start": start},
                })
        # else: the question names no years — leave it alone rather than
        # attaching a range it never mentioned.

        # Variations with different but related series IDs. The generated
        # question names no dates, so the arguments must not carry the parent
        # seed's start/end either.
        current_sid = args.get("series_id", "")
        related = [s for s in valid_series_ids
                   if s.startswith(current_sid[:2]) and s != current_sid]
        for sid in random.sample(related, min(2, len(related))):
            examples.append({
                "question": f"Show me data for series {sid}.",
                "tool": tool,
                "arguments": {"series_id": sid},
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

    # list_surveys: no expansion. Nothing can vary but the wording, and the
    # wordings are seeds now.

    return examples[:target_count_per_seed]


def render_call(ex):
    """Render a seed example's tool call, e.g. get_series(series_id="X")."""
    args_str = ", ".join(f'{k}="{v}"' if isinstance(v, str) else f"{k}={v}"
                         for k, v in ex["arguments"].items())
    return f'{ex["tool"]}({args_str})'


def format_training_example(ex):
    """Emit chat-format rows so mlx_lm can mask the prompt.

    This used to hand-roll a ChatML string under a "text" key. Three problems,
    all fixed by letting the tokenizer's own template do it:

    1. mlx_lm cannot mask the prompt on a "text" dataset — it has no way to tell
       prompt from completion. Our data is heavily short-completion (134 prompt
       tokens vs 19 completion), so 87.7% of the loss was the model re-predicting
       a system prompt that is identical in every row.
    2. The hand-rolled string omitted Qwen3's empty `<think></think>` block,
       which is the actual mechanism for non-thinking mode. The system prompt
       asked for no reasoning in English instead, which is not the same thing.
    3. It emitted a stray leading space before the call ("assistant\\n get_series"),
       a different token than the un-spaced form any chat-template path produces.
    """
    return {
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": ex["question"]},
            {"role": "assistant", "content": render_call(ex)},
        ]
    }


def example_key(item):
    """Dedup/overlap key: the (question, call) pair this row teaches."""
    m = item["messages"]
    return (m[1]["content"], m[2]["content"])


# ── Step 3: Build datasets ──
def build_split(seeds, name, split, target=600, per_seed=2):
    """Build a dataset split from seeds, expanding only from `split`'s pools.

    per_seed defaults to 2 (the seed itself plus one synthetic variation). It was
    effectively 6, which made 56% of train "Show me data for series CUUR0000SAF1."
    style rows that just echo an id already present in the question. Those teach
    nothing about concept->id, which is the mapping the model actually needs, so
    the real phrasings now dominate.
    """
    examples = []
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
        if (k := example_key(item)) in seen:
            continue
        seen.add(k)
        formatted.append(item)
        if len(formatted) == target:
            break

    path = f"{name}.jsonl"
    with open(path, "w") as f:
        for item in formatted:
            f.write(json.dumps(item) + "\n")
    print(f"  {name}: {len(formatted)} unique examples ({len(examples)} generated) → {path}")
    return formatted


MLX_DIR = Path("mlx_data_clean")


def main():
    """Build every artifact. Kept out of module scope so that `import
    build_dataset` is free of side effects — importing it used to run the entire
    build, which is why score.py had to recover SYSTEM_PROMPT by parsing a
    training file instead of just importing it."""
    train_seeds, val_seeds, test_seeds = split_seeds()
    _assert_ids_exist()
    _print_pools()

    print("\nBuilding datasets...")
    train = build_split(train_seeds, "train_clean", "train")
    val = build_split(val_seeds, "val_clean", "val")
    test = build_split(test_seeds, "test_clean", "test")

    # ── Write the MLX data directory ──
    # mlx_lm.lora requires a directory holding train.jsonl / valid.jsonl /
    # test.jsonl under those exact names. This used to be a manual `cp` after
    # every rebuild — forget it once and you train on stale data with no error,
    # the same silent-staleness class of bug that produced the bogus 93%.
    MLX_DIR.mkdir(exist_ok=True)
    for split_name, rows in [("train", train), ("valid", val), ("test", test)]:
        with open(MLX_DIR / f"{split_name}.jsonl", "w") as f:
            for item in rows:
                f.write(json.dumps(item) + "\n")
    print(f"  mlx data dir: {MLX_DIR}/{{train,valid,test}}.jsonl")

    # ── Assert the split actually holds ──
    sets = {n: {example_key(i) for i in s}
            for n, s in [("train", train), ("val", val), ("test", test)]}
    for a, b in [("test", "train"), ("val", "train"), ("test", "val")]:
        if overlap := sets[a] & sets[b]:
            raise SystemExit(f"{len(overlap)} rows shared between {a} and {b}:\n  " +
                             "\n  ".join(str(o) for o in sorted(overlap)[:3]))
    print("✓ verified: no example appears in more than one split")

    # ── Assert no label contradicts its own question ──
    # The expander used to attach date ranges to questions that named no years.
    bad = [(q, c) for q, c in sets["train"] | sets["val"] | sets["test"]
           if c.startswith("get_series(")
           and re.search(r"(?:19|20)\d\d", c)
           and not re.search(r"(?:19|20)\d\d", q)]
    if bad:
        raise SystemExit(f"{len(bad)} rows carry dates their question never mentions:\n  "
                         + "\n  ".join(str(b) for b in sorted(bad)[:3]))
    print("✓ verified: no label carries dates absent from its question")

    # ── Save raw seed splits for evaluation ──
    for name, seeds in [("test_seeds", test_seeds), ("val_seeds", val_seeds)]:
        with open(f"{name}.json", "w") as f:
            json.dump(seeds, f, indent=2)
        print(f"  {name}: {len(seeds)} seeds → {name}.json")

    print(f"\n✓ Dataset built. Training: {len(train)}, Val: {len(val)}, Test: {len(test)}")
    print(f"✓ Test SEEDS never appear in training data (seed-lineage split)")
    print(f"✓ Expansion POOLS are partitioned per split, so no expanded example can")
    print(f"  be shared either — asserted above, not just intended.")


if __name__ == "__main__":
    main()