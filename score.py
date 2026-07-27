"""
Score a trained adapter on the real held-out test set.

Replaces the ad-hoc eval that produced the "93% on 15 held-out" figure. That
number was measured against heldout_test.json, which was the *V1* holdout — the
V2 rebuild reshuffled the split and put all 15 of those seeds into training.

Two eval sets are reported:
  test_seeds.json  — 15 natural questions, seed-disjoint AND verbatim-disjoint
                     from train. This is the honest number.
  test_clean.jsonl — 30 expanded rows; 4 are verbatim-identical to train rows
                     (shared expansion pools), so they are excluded by default.

Usage:
    python score.py --adapter models/bls-agent-v3
    python score.py --adapter models/bls-agent-v3 --compare models/bls-agent-v2
    python score.py --base-only
"""

import argparse
import ast
import json
import re
from pathlib import Path

MODEL = "Qwen/Qwen3-1.7B"

# Recovered from the training file itself rather than imported from
# build_dataset (which runs the whole build at module import). This guarantees
# the eval prompt is byte-identical to what the adapter was trained on.
def _system_prompt(path="train_clean.jsonl"):
    first = json.loads(open(path).readline())["text"]
    return first.split("<|im_start|>system\n")[1].split("<|im_end|>")[0]


SYSTEM_PROMPT = _system_prompt()


def parse_call(text: str) -> tuple[str, dict] | None:
    """Extract (tool, args) from model output.

    Uses balanced-paren scanning + ast rather than a greedy regex and a naive
    split(","), both of which mis-parse quoted values containing commas and any
    output that wraps the call in prose or a <think> block.
    """
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.S)
    m = re.search(r"([A-Za-z_]\w*)\s*\(", text)
    if not m:
        return None
    name, i, depth = m.group(1), m.end() - 1, 0
    for j in range(i, len(text)):
        depth += (text[j] == "(") - (text[j] == ")")
        if depth == 0:
            break
    else:
        return None
    try:
        node = ast.parse(text[m.start():j + 1], mode="eval").body
        if not isinstance(node, ast.Call):
            return None
        args = {kw.arg: ast.literal_eval(kw.value) for kw in node.keywords}
    except (SyntaxError, ValueError):
        return None
    return name, args


def load_seed_set(path="test_seeds.json"):
    return [(s["question"], s["tool"], s["arguments"]) for s in json.load(open(path))]


def load_jsonl_set(path="test_clean.jsonl", exclude_train_dupes=True):
    rows = [json.loads(l)["text"] for l in open(path)]
    if exclude_train_dupes:
        train = {json.loads(l)["text"] for l in open("train_clean.jsonl")}
        rows = [r for r in rows if r not in train]
    out = []
    for t in rows:
        q = t.split("user\n")[1].split("<|im_end|>")[0]
        gold = t.split("assistant\n")[1].strip().replace("<|im_end|>", "")
        parsed = parse_call(gold)
        if parsed:
            out.append((q, parsed[0], parsed[1]))
    return out


def run(model, tokenizer, questions, max_tokens=64):
    from mlx_lm import generate
    from mlx_lm.sample_utils import make_sampler

    sampler = make_sampler(temp=0.0)
    outs = []
    for q in questions:
        prompt = (
            f"<|im_start|>system\n{SYSTEM_PROMPT}<|im_end|>\n"
            f"<|im_start|>user\n{q}<|im_end|>\n<|im_start|>assistant\n"
        )
        outs.append(generate(model, tokenizer, prompt=prompt,
                             max_tokens=max_tokens, sampler=sampler, verbose=False))
    return outs


KEY_ARG = {"get_series": "series_id", "get_series_info": "series_id",
           "analyze_cpi_seasonality": "series_id", "search_series": "query",
           "popular_series": "survey", "list_surveys": None}


def score(outputs, gold, label):
    """Three metrics, because they fail for different reasons.

    tool   — 6-way tool choice. The easy part; this is what the old "93%" measured.
    entity — tool + the semantically load-bearing argument (series_id/query/survey),
             ignoring start/end. Isolates "does it know which series this is?"
    exact  — every argument identical. Extra hallucinated start/end count as wrong.
    """
    n = len(gold)
    tool_ok = entity_ok = exact = 0
    failures = []
    for out, (q, gt_tool, gt_args) in zip(outputs, gold):
        parsed = parse_call(out)
        if parsed is None:
            failures.append((q, gt_tool, gt_args, out.strip()[:80], "unparseable"))
            continue
        pt, pa = parsed
        t_ok = pt == gt_tool
        k = KEY_ARG.get(gt_tool)
        e_ok = t_ok and (k is None or str(pa.get(k)) == str(gt_args.get(k)))
        a_ok = t_ok and {k2: str(v) for k2, v in pa.items()} == {k2: str(v) for k2, v in gt_args.items()}
        tool_ok += t_ok
        entity_ok += e_ok
        exact += a_ok
        if not a_ok:
            call = f"{pt}({', '.join(f'{k2}={v!r}' for k2, v in pa.items())})"
            why = "wrong tool" if not t_ok else ("wrong entity" if not e_ok else "spurious dates")
            failures.append((q, gt_tool, gt_args, call, why))
    return {"label": label, "n": n, "tool": tool_ok / n, "entity": entity_ok / n,
            "exact": exact / n, "failures": failures}


def report(r):
    print(f"\n{r['label']}  (n={r['n']})")
    print(f"  tool accuracy  : {r['tool']:.1%}  ({round(r['tool']*r['n'])}/{r['n']})")
    print(f"  entity accuracy: {r['entity']:.1%}  ({round(r['entity']*r['n'])}/{r['n']})")
    print(f"  exact match    : {r['exact']:.1%}  ({round(r['exact']*r['n'])}/{r['n']})")
    if r["failures"]:
        print(f"  failures ({len(r['failures'])}):")
        for q, gtt, gta, got, why in r["failures"]:
            want = f"{gtt}({', '.join(f'{k}={v!r}' for k, v in gta.items())})"
            print(f"    [{why}] {q}")
            print(f"        want: {want}")
            print(f"        got : {got}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--adapter", default="models/bls-agent-v3")
    p.add_argument("--compare", help="second adapter to score alongside")
    p.add_argument("--base-only", action="store_true")
    p.add_argument("--no-base", action="store_true",
                   help="skip the base-model run (for checkpoint sweeps, where it "
                        "would be re-scored identically for every checkpoint)")
    p.add_argument("--split", choices=["test", "val"], default="test",
                   help="which split to score. Use 'val' to CHOOSE a checkpoint; "
                        "picking one by test accuracy is selection on the test set "
                        "and inflates the reported number.")
    a = p.parse_args()

    from mlx_lm import load

    seeds_path = "test_seeds.json" if a.split == "test" else "val_seeds.json"
    rows_path = "test_clean.jsonl" if a.split == "test" else "val_clean.jsonl"
    seed_set = load_seed_set(seeds_path)
    clean_set = load_jsonl_set(rows_path)
    print(f"Eval sets [{a.split}]: {len(seed_set)} natural held-out seeds | "
          f"{len(clean_set)} expanded rows (train-duplicates removed)")

    targets = [] if a.base_only else [("fine-tuned " + Path(a.adapter).name, a.adapter)]
    if a.compare:
        targets.append(("fine-tuned " + Path(a.compare).name, a.compare))
    if not a.no_base:
        targets.append(("base Qwen3-1.7B", None))

    for label, adapter in targets:
        model, tok = load(MODEL, adapter_path=adapter) if adapter else load(MODEL)
        for name, data in [(f"{a.split} seeds", seed_set), (f"{a.split} expanded", clean_set)]:
            qs = [q for q, _, _ in data]
            report(score(run(model, tok, qs), data, f"{label} — {name}"))


if __name__ == "__main__":
    main()
