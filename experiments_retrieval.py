"""Feasibility study: can retrieval replace memorisation for BLS series ids?

Three questions, in order of how badly a 'no' kills the idea:
  1. Is the RETRIEVER capable at all? Give it an oracle query (the gold series'
     own item name) and see if the gold id comes back in the top k.
  2. Does the CURRENT retriever (substring + .head(), no ranking) suffice, or
     does it need real ranking (BM25)?
  3. Could a naive query (the raw user question) work without any model at all?

Only if 1 and 2 look good is it worth asking whether a 1.7B model can formulate
the query — which is the risk Anthropic flags for small models.
"""
import json, re, sys, math
from collections import Counter

sys.path.insert(0, "src")
import pandas as pd
from bls_data.cpi import _MASTER_PATH

cat = pd.read_csv(_MASTER_PATH, dtype=str)
titles = cat["series_title"].fillna("").tolist()
ids = cat["series_id"].tolist()
items = cat["item_name"].fillna("").tolist()
id_to_item = dict(zip(ids, items))

TOK = re.compile(r"[a-z0-9]+")
def tok(s): return TOK.findall(s.lower())

# ── BM25 over series titles ──
docs = [tok(t) for t in titles]
N, avgdl = len(docs), sum(len(d) for d in docs) / len(docs)
df = Counter()
for d in docs:
    for w in set(d):
        df[w] += 1
idf = {w: math.log(1 + (N - n + 0.5) / (n + 0.5)) for w, n in df.items()}
tfs = [Counter(d) for d in docs]

def bm25(query, k=10, k1=1.5, b=0.75):
    q = tok(query)
    scores = []
    for i, tf in enumerate(tfs):
        dl = len(docs[i]) or 1
        s = 0.0
        for w in q:
            if (f := tf.get(w, 0)):
                s += idf.get(w, 0) * f * (k1 + 1) / (f + k1 * (1 - b + b * dl / avgdl))
        if s:
            scores.append((s, i))
    scores.sort(reverse=True)
    return [ids[i] for _, i in scores[:k]]

def substring(query, k=10):
    """Exactly what search_series does today: substring match, no ranking."""
    m = cat["series_title"].str.contains(re.escape(query), case=False, na=False)
    return cat[m].head(k)["series_id"].tolist()

# ── evaluate ──
seeds = json.load(open("test_seeds.json"))
cases = [(s["question"], s["arguments"]["series_id"])
         for s in seeds if s["arguments"].get("series_id", "").startswith("CU")]

def report(name, queries, fn, k=10):
    r1 = r5 = r10 = 0
    misses = []
    for (q, gold) in cases:
        got = fn(queries[(q, gold)], k)
        if gold in got[:1]: r1 += 1
        if gold in got[:5]: r5 += 1
        if gold in got[:10]: r10 += 1
        else: misses.append((queries[(q, gold)], gold, id_to_item.get(gold, "?")))
    n = len(cases)
    print(f"  {name:34s} recall@1 {r1/n:5.1%}  @5 {r5/n:5.1%}  @10 {r10/n:5.1%}")
    return misses

oracle = {(q, g): id_to_item.get(g, "") for q, g in cases}
raw    = {(q, g): q for q, g in cases}

print(f"{len(cases)} test questions with a CPI series id; corpus = {N} titles\n")
print("ORACLE query (the gold series' own item name) — retriever ceiling:")
report("  substring (current impl)", oracle, substring)
m_or = report("  BM25 (proposed)", oracle, bm25)

print("\nRAW question as query — no model, zero-effort baseline:")
report("  substring (current impl)", raw, substring)
m_raw = report("  BM25 (proposed)", raw, bm25)

print("\nOracle-query misses under BM25 (retriever genuinely cannot find these):")
for q, gold, item in m_or[:6]:
    print(f"    {gold:16s} {item!r}  <- query {q!r}")
