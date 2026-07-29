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
import re
from typing import Optional

# Series outside the CPI catalogue. Names chosen to match how people ask.
_LABOR_ITEMS = {
    "unemployment rate": "LNS14000000",
    "labor force participation rate": "LNS11300000",
    "employment-population ratio": "LNS12300000",
    "total nonfarm employment": "CES0000000001",
    "average hourly earnings": "CES0500000003",
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
