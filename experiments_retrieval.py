"""Can BM25 over 400 item names replace memorising series IDs?

Answers three questions:
  1. Retriever ceiling: given the gold item's own name (oracle), does BM25
     return the right series ID? Must be ~100% or the index is broken.
  2. Raw question baseline: no model, no training, no GPU — throw the user's
     raw question at BM25 and measure recall of the correct series ID.
  3. Comparison: how close is (2) to the fine-tuned model's 94.4%?

The index covers the 400 US-city-average NSA item names (canonical_names),
NOT the full 8,103 series_title strings. The 400-item framing comes from the
observation that the catalogue is ~400 distinct items repeated across area
and seasonal-adjustment combinations, and item name is a unique key in the
CUUR0000 namespace.

Only CPI-series seeds are scorable by BM25 — non-CPI seeds (list_surveys,
popular_series with a survey, search_series with a query) have no item name
to look up. The reported numbers state the denominator explicitly.
"""

import json
import math
import re
import sys
from collections import Counter

sys.path.insert(0, "src")
from bls_data.items import canonical_names

# ── Build BM25 over the 400 item names ──
ID_TO_NAME = canonical_names()                         # {series_id: item_name}
NAME_TO_ID = {v: k for k, v in ID_TO_NAME.items()}     # {item_name: series_id}

TOK = re.compile(r"[a-z0-9]+")


def tok(s):
    return TOK.findall(s.lower())


_names = list(NAME_TO_ID)
_docs = [tok(n) for n in _names]
_N = len(_docs)
_avgdl = sum(len(d) for d in _docs) / _N
_df = Counter()
for d in _docs:
    for w in set(d):
        _df[w] += 1
_idf = {w: math.log(1 + (_N - n + 0.5) / (n + 0.5)) for w, n in _df.items()}
_tfs = [Counter(d) for d in _docs]


def bm25(query, k=10, k1=1.5, b=0.75):
    """Rank item names against a free-text query, return top-k series IDs."""
    q = tok(query)
    if not q:
        return []
    scores = []
    for i, tf in enumerate(_tfs):
        dl = len(_docs[i]) or 1
        s = sum(
            _idf.get(w, 0) * f * (k1 + 1) / (f + k1 * (1 - b + b * dl / _avgdl))
            for w in q
            if (f := tf.get(w, 0))
        )
        if s:
            scores.append((s, i))
    scores.sort(reverse=True)
    return [NAME_TO_ID[_names[i]] for _, i in scores[:k]]


# ── Load test seeds ──
seeds = json.load(open("test_seeds.json"))


def gold_item_name(seed):
    """Return the official item name for a CPI-series seed, or None."""
    sid = seed["arguments"].get("series_id", "")
    if sid.startswith("CU"):
        return ID_TO_NAME.get(sid)
    return None


# Partition: CPI seeds (can be scored by BM25) vs non-CPI seeds (cannot).
cpi_seeds = [(s["question"], s["arguments"]["series_id"])
             for s in seeds
             if gold_item_name(s)]
non_cpi = [(s["question"], s["tool"], s["arguments"])
           for s in seeds
           if not gold_item_name(s)]


# ── Evaluate ──
def evaluate(name, queries, k_values=(1, 5, 10)):
    """Report recall@k for a set of (question, gold_series_id) pairs."""
    recalls = {k: 0 for k in k_values}
    misses = []
    for q, gold in queries:
        got = bm25(queries[(q, gold)])
        for k in k_values:
            if gold in got[:k]:
                recalls[k] += 1
        if gold not in got[:5]:
            misses.append((queries[(q, gold)], gold, ID_TO_NAME.get(gold, "?")))
    n = len(queries)
    parts = [f"recall@1 {recalls[1]/n:5.1%}"]
    for k in k_values[1:]:
        parts.append(f"@{k} {recalls[k]/n:5.1%}")
    print(f"  {name:40s} {'  '.join(parts)}   (n={n})")
    return misses


# ── Oracle: the gold item's own name ──
oracle_queries = {(q, g): ID_TO_NAME.get(g, "") for q, g in cpi_seeds}
# ── Raw: the user's question, no model ──
raw_queries = {(q, g): q for q, g in cpi_seeds}

print(f"BM25 index: {_N} item names (US city average, NSA + labour-force)\n")
print(f"Test seeds: {len(seeds)} total — {len(cpi_seeds)} CPI, {len(non_cpi)} non-CPI "
      f"(list_surveys/popular_series/search_series)\n")

print("CPI seeds only (BM25 can score these):")
print("  ORACLE query (the gold item's own name) — retriever ceiling:")
m_or = evaluate("oracle", oracle_queries)

print("  RAW question as query — no model, zero-effort baseline:")
m_raw = evaluate("raw question", raw_queries)

# ── Full-set score (non-CPI seeds are misses by construction) ──
n_total = len(seeds)
# CPI results from raw queries
raw_recall_1 = sum(
    1 for q, g in cpi_seeds if g in bm25(raw_queries[(q, g)])[:1]
)
print(f"\nFull 43-seed set (non-CPI seeds scored as misses):")
print(f"  {'raw question recall@1':40s} {raw_recall_1/n_total:5.1%}   (n={n_total})")

print(f"\nNon-CPI seeds not scoreable by BM25 ({len(non_cpi)}):")
for q, tool, args in sorted(non_cpi, key=lambda x: x[1]):
    print(f"  [{tool}] {q}")

print(f"\nBM25 misses on CPI seeds (recall@5):")
for query_text, gold, item in m_raw:
    print(f"    {gold:16s} {item!r}")
    print(f"        query: {query_text!r}")
    # Show what BM25 returned instead
    got = bm25(query_text, k=5)
    for rank, sid in enumerate(got, 1):
        name = ID_TO_NAME.get(sid, "?")
        marker = " ✓" if sid == gold else ""
        print(f"        #{rank}: {sid} {name!r}{marker}")
