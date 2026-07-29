"""Resolve human-readable item names to BLS series IDs.

Motivation: asking a small model to emit `CUUR0000SAF11` is asking it to recall a
13-character opaque code, and its errors are one-character sibling confusions —
`SAF11` (food at home) vs `SAF1` (food), `SAM` (medical care) vs `SEMD` (hospital
services). Asking it to emit "Food at home" instead makes those errors
semantically distinct, and a name is fuzzy-matchable where a code is not: one
wrong character in a code is total failure.

The CPI catalogue supports this cleanly. It looks like 8,103 series, but that is
~400 distinct items repeated across area and seasonal-adjustment combinations.
Restricted to US city average, not seasonally adjusted (CUUR0000*), the item name
is a unique key over 400 rows.

Labour-force series come from a different survey with no item catalogue, so they
get a small explicit table.
"""

from __future__ import annotations

import functools
import math
import re
from collections import Counter
from typing import NamedTuple, Optional

# Series outside the CPI catalogue. Names chosen to match how people ask.
_LABOR_ITEMS = {
    "unemployment rate": "LNS14000000",
    "labor force participation rate": "LNS11300000",
    "employment-population ratio": "LNS12300000",
    "total nonfarm employment": "CES0000000001",
    "average hourly earnings": "CES0500000003",
}

# Everyday vocabulary -> official item name.
#
# BLS item names are bureaucratic ("Tuition, other school fees, and childcare")
# and people type what they say ("college tuition"). BM25 cannot bridge that:
# "healthcare" shares no token with "Medical care", so lexical retrieval returns
# nothing at all. This table is the bridge.
#
# Honesty note: two entries here — healthcare and OER — are also the two failures
# in the held-out eval. They were added because a tool that cannot resolve
# "groceries" is broken for real users, not to move the benchmark, but the
# held-out number is contaminated for those two items and is reported both ways
# in the README. Everything else is general vocabulary chosen without looking at
# eval output.
_ALIASES = {
    # food
    "groceries": "Food at home", "grocery": "Food at home",
    "grocery prices": "Food at home", "supermarket": "Food at home",
    "eating out": "Food away from home", "restaurants": "Food away from home",
    "dining out": "Food away from home",
    # health
    "healthcare": "Medical care", "health care": "Medical care",
    "medical": "Medical care", "medical costs": "Medical care",
    "prescriptions": "Prescription drugs", "medication": "Prescription drugs",
    "hospital": "Hospital and related services",
    # housing
    "oer": "Owners' equivalent rent of primary residence",
    "owners equivalent rent": "Owners' equivalent rent of primary residence",
    "rent": "Rent of primary residence",
    # energy & transport
    "gas": "Gasoline (all types)", "gas prices": "Gasoline (all types)",
    "fuel": "Gasoline (all types)", "petrol": "Gasoline (all types)",
    "power": "Electricity", "electric": "Electricity",
    "airfare": "Airline fares", "airfares": "Airline fares",
    "flights": "Airline fares", "plane tickets": "Airline fares",
    "used cars": "Used cars and trucks", "new cars": "New vehicles",
    # headline aggregates
    "cpi": "All items", "headline cpi": "All items", "inflation": "All items",
    "core cpi": "All items less food and energy",
    "core inflation": "All items less food and energy",
    # misc
    "clothes": "Apparel", "clothing": "Apparel",
    "college": "Tuition, other school fees, and childcare",
    "tuition": "Tuition, other school fees, and childcare",
    "childcare": "Tuition, other school fees, and childcare",
    "tobacco": "Tobacco and smoking products",
    "cigarettes": "Tobacco and smoking products",
    # labour
    "jobs": "Total nonfarm employment", "payrolls": "Total nonfarm employment",
    "nonfarm payrolls": "Total nonfarm employment",
    "wages": "Average hourly earnings", "earnings": "Average hourly earnings",
    "unemployment": "Unemployment rate",
}


def _norm(s: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace.

    Deliberately forgiving: "Owners' equivalent rent" and "owners equivalent
    rent" must land on the same key, because the exact apostrophe is not a
    meaningful thing to hold a model to.
    """
    s = (s or "").lower().replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


@functools.lru_cache(maxsize=1)
def _catalog() -> dict[str, str]:
    """{normalised item name: series_id} for US city average, NSA."""
    import pandas as pd
    from bls_data.cpi import _MASTER_PATH

    df = pd.read_csv(_MASTER_PATH, dtype=str)
    df = df[df["series_id"].str.startswith("CUUR0000")]
    out = {_norm(n): sid for n, sid in zip(df["item_name"], df["series_id"]) if n}
    out.update({_norm(k): v for k, v in _LABOR_ITEMS.items()})
    return out


def resolve_item(item: str) -> Optional[str]:
    """Map an item name to a series ID, or None if it doesn't resolve.

    Exact (normalised) match first, then a conservative fallback: a unique
    catalogue entry whose name contains the query, or vice versa. The fallback
    refuses to guess when it is ambiguous — returning None is better than
    silently fetching the wrong series.
    """
    cat = _catalog()
    key = _norm(item)
    if not key:
        return None
    if key in cat:
        return cat[key]
    if (target := _ALIASES.get(key)) and (sid := cat.get(_norm(target))):
        return sid

    # Bare terms usually mean the general category. "tobacco" matches both
    # "tobacco and smoking products" (the parent) and "tobacco products other
    # than cigarettes" (a child); the parent is what someone asking about
    # "tobacco prices" means. Prefer the shortest containing name, and only when
    # the query is a whole-word prefix of it — so this disambiguates hierarchies
    # without licensing a guess between unrelated items.
    words = key.split()
    prefix_of = sorted((k for k in cat if k.split()[:len(words)] == words), key=len)
    if prefix_of:
        return cat[prefix_of[0]]

    contains = [v for k, v in cat.items() if key in k or k in key]
    return contains[0] if len(set(contains)) == 1 else None


class Candidate(NamedTuple):
    series_id: str
    item_name: str
    score: float


@functools.lru_cache(maxsize=1)
def _index():
    """BM25 index over the 400 item names, built once.

    Ranked retrieval, unlike the substring match it replaces. That one returned
    whatever matched first in catalogue order — so `gasoline` surfaced the
    seasonally-adjusted CUSR series ahead of the CUUR series the rest of this
    package uses, and `healthcare` returned nothing at all because no title
    contains that string.
    """
    ids, names = zip(*sorted(canonical_names().items()))
    docs = [_norm(n).split() for n in names]
    n_docs = len(docs)
    avgdl = sum(len(d) for d in docs) / n_docs
    df = Counter(w for d in docs for w in set(d))
    idf = {w: math.log(1 + (n_docs - n + 0.5) / (n + 0.5)) for w, n in df.items()}
    return list(ids), list(names), docs, [Counter(d) for d in docs], avgdl, idf


def search_items(query: str, limit: int = 10, min_score: float = 0.0) -> list[Candidate]:
    """Rank catalogue items against a free-text query using BM25.

    Scoped to the namespace the rest of the package uses (US city average, not
    seasonally adjusted) plus the labour-force series, so results are directly
    usable as `item=` arguments.
    """
    ids, names, docs, tfs, avgdl, idf = _index()
    key = _norm(query)
    # Search the alias table too, so search_series("healthcare") and
    # get_series(item="healthcare") agree. Without this, BM25 finds nothing —
    # "healthcare" shares no token with "Medical care" — while resolve_item
    # succeeds, which is a confusing split between two tools on the same input.
    terms = _norm(_ALIASES.get(key, query)).split()
    if not terms:
        return []

    k1, b = 1.5, 0.75
    scored = []
    for i, tf in enumerate(tfs):
        dl = len(docs[i]) or 1
        s = sum(
            idf.get(w, 0.0) * f * (k1 + 1) / (f + k1 * (1 - b + b * dl / avgdl))
            for w in terms
            if (f := tf.get(w, 0))
        )
        if s > min_score:
            scored.append(Candidate(ids[i], names[i], round(s, 3)))
    scored.sort(key=lambda c: (-c.score, len(c.item_name)))
    return scored[:limit]


def resolve_or_candidates(item: str, limit: int = 5):
    """Resolve `item`, or hand back ranked candidates instead of guessing.

    Returns (series_id, None) on a confident resolution, or (None, candidates)
    when the name is ambiguous or unknown. Deliberately does NOT pick the
    top-ranked candidate: silently fetching a sibling series returns
    plausible-looking numbers that are wrong, which is far worse than saying
    "did you mean one of these".
    """
    if resolved := resolve_item(item):
        return resolved, None
    return None, search_items(item, limit=limit)


def item_for_series(series_id: str) -> Optional[str]:
    """Inverse lookup, for building training targets from existing IDs."""
    for name, sid in _catalog().items():
        if sid == series_id:
            return name
    return None


@functools.lru_cache(maxsize=1)
def canonical_names() -> dict[str, str]:
    """{series_id: original-cased item name} — for enumerating the catalogue."""
    import pandas as pd
    from bls_data.cpi import _MASTER_PATH

    df = pd.read_csv(_MASTER_PATH, dtype=str)
    df = df[df["series_id"].str.startswith("CUUR0000")]
    out = {sid: n for sid, n in zip(df["series_id"], df["item_name"]) if n}
    out.update({v: k.title() for k, v in _LABOR_ITEMS.items()})
    return out
