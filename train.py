"""
Fine-tune the BLS Data Agent (Qwen3-1.7B + LoRA on Apple Silicon via MLX).

This is a thin wrapper around `python -m mlx_lm.lora`, which is what actually
produced every model in models/. The previous version of this file was dead
code: it filtered rows on a "messages" key that the data has never had (so it
silently loaded 0 examples and trained on nothing), and imported a `LoRA` symbol
that does not exist in mlx_lm. It also carried an Unsloth/CUDA path that could
not be tested on this machine. Both are gone — what remains is the pipeline that
is actually run and verified here.

Two findings are encoded below because neither is guessable:

  * ~600 iterations. Val LOSS bottoms around iter 250 and rises monotonically,
    but task accuracy keeps climbing to ~600 (37% -> 91% exact match). Early
    stopping on val loss costs ~50 points. Cross-entropy punishes confident
    near-misses while the emitted tool call keeps getting more correct.
  * Checkpoints are selected on val ACCURACY, not val loss and not test
    accuracy. Selecting on test would inflate the number you then report.

Usage:
    python build_dataset.py          # must run first — writes mlx_data_clean/
    python train.py                  # train + select checkpoint
    python train.py --iters 900      # sweep further out
    python train.py --no-select      # keep final weights, skip the sweep
    python score.py --adapter models/bls-agent-v7
"""

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

MODEL = "Qwen/Qwen3-1.7B"
DATA_DIR = Path("mlx_data_clean")
SOURCES = [Path("seed_dataset.py"), Path("build_dataset.py")]

# mask_prompt is the single most important setting here. Our data is heavily
# short-completion — mean 134 prompt tokens vs 19 completion tokens (generation
# ratio 0.141), and the system prompt is byte-identical in every row. With
# mask_prompt off, 87.7% of the loss was the model re-predicting that fixed
# preamble. That is why train loss looked implausibly low (0.04) and why val
# loss decoupled from task accuracy: ~88% of val loss was measuring preamble
# reproduction, not tool-call quality. This is a documented failure mode for
# short-completion SFT (Huerta-Enochian & Ko, EMNLP 2024), not a property of
# tool calling.
#
# num_layers -1 adapts all 28 transformer blocks; 16 left the first 12 frozen.
DEFAULTS = dict(iters=800, batch_size=2, num_layers=-1, learning_rate="5e-5",
                save_every=200, steps_per_eval=200, val_batches=25, seed=0)

# rank 16 (was 8) for 82 concepts to memorise; cosine decay with warmup rather
# than a constant LR, which left the run thrashing at the end.
def _tuning_config(iters):
    return {
        "mask_prompt": True,
        "lora_parameters": {"rank": 16, "dropout": 0.0, "scale": 20.0},
        "lr_schedule": {
            "name": "cosine_decay",
            "warmup": max(10, iters // 20),      # ~5% warmup
            "warmup_init": 1e-7,
            "arguments": [5e-5, iters, 1e-6],    # [init_lr, decay_steps, end_lr]
        },
    }


def check_data_fresh():
    """Refuse to train on data older than the code that generates it."""
    needed = [DATA_DIR / f"{n}.jsonl" for n in ("train", "valid", "test")]
    missing = [p for p in needed if not p.exists()]
    if missing:
        raise SystemExit(f"missing {[str(p) for p in missing]} — run: python build_dataset.py")

    newest_src = max(p.stat().st_mtime for p in SOURCES if p.exists())
    oldest_data = min(p.stat().st_mtime for p in needed)
    if newest_src > oldest_data:
        raise SystemExit(
            f"{DATA_DIR}/ is older than seed_dataset.py/build_dataset.py.\n"
            "Training on stale data fails silently — run: python build_dataset.py"
        )
    n = sum(1 for _ in open(needed[0]))
    print(f"✓ {DATA_DIR}/ is current ({n} train rows)")


def train(adapter_path: Path, cfg: dict):
    # rank / lr_schedule / mask_prompt have no CLI flags, so they go via YAML.
    import yaml
    conf_path = adapter_path / "_tuning.yaml"
    conf_path.write_text(yaml.safe_dump(_tuning_config(cfg["iters"])))

    cmd = [
        # `-m mlx_lm.lora` still works but warns it is deprecated.
        sys.executable, "-m", "mlx_lm", "lora",
        "--model", MODEL, "--train", "--data", str(DATA_DIR),
        "--adapter-path", str(adapter_path),
        "--config", str(conf_path),
        "--iters", str(cfg["iters"]),
        "--batch-size", str(cfg["batch_size"]),
        "--num-layers", str(cfg["num_layers"]),
        "--learning-rate", str(cfg["learning_rate"]),
        "--save-every", str(cfg["save_every"]),
        "--steps-per-eval", str(cfg["steps_per_eval"]),
        "--val-batches", str(cfg["val_batches"]),
        "--seed", str(cfg["seed"]),
    ]
    print("+ " + " ".join(cmd))
    subprocess.run(cmd, check=True)


def select_checkpoint(adapter_path: Path):
    """Pick the checkpoint with the best exact-match on the VAL split.

    Deliberately not val loss (it disagrees with accuracy here) and deliberately
    not test (that is selection on the set you report).
    """
    import score

    ckpts = sorted(adapter_path.glob("[0-9]*_adapters.safetensors"))
    if not ckpts:
        print("no intermediate checkpoints; keeping final weights")
        return None

    from mlx_lm import load
    val_seeds = score.load_seed_set("val_seeds.json")
    val_rows = score.load_jsonl_set("val_clean.jsonl")
    combined = val_seeds + val_rows
    print(f"\nSelecting checkpoint on val ({len(combined)} items)")

    staging = adapter_path / "_staging"
    results = []
    for ckpt in ckpts:
        staging.mkdir(exist_ok=True)
        shutil.copy(adapter_path / "adapter_config.json", staging / "adapter_config.json")
        shutil.copy(ckpt, staging / "adapters.safetensors")
        model, tok = load(MODEL, adapter_path=str(staging))
        outs = score.run(model, tok, [q for q, _, _ in combined])
        r = score.score(outs, combined, ckpt.name)
        results.append((r["exact"], r["entity"], ckpt))
        print(f"  {ckpt.name}: val exact {r['exact']:.1%}  entity {r['entity']:.1%}")
        del model
    shutil.rmtree(staging, ignore_errors=True)

    best_exact, best_entity, best = max(results, key=lambda t: (t[0], t[1]))
    shutil.copy(best, adapter_path / "adapters.safetensors")

    cfg_path = adapter_path / "adapter_config.json"
    cfg = json.load(open(cfg_path))
    cfg["_selected_checkpoint"] = best.name
    cfg["_selection"] = (
        f"adapters.safetensors is {best.name}, chosen by best exact-match on the "
        f"val split ({best_exact:.1%}). Not chosen on val loss — val loss bottoms "
        f"near iter 250 while accuracy keeps improving to ~600. Not chosen on "
        f"test, which would inflate the reported number."
    )
    json.dump(cfg, open(cfg_path, "w"), indent=4)
    print(f"\n✓ selected {best.name} (val exact {best_exact:.1%}) -> adapters.safetensors")
    return best


def main():
    p = argparse.ArgumentParser(description="Fine-tune the BLS Data Agent (MLX LoRA)")
    p.add_argument("--output", default="models/bls-agent-v7", help="adapter output dir")
    p.add_argument("--iters", type=int, default=DEFAULTS["iters"])
    p.add_argument("--seed", type=int, default=DEFAULTS["seed"],
                   help="training seed; run-to-run sd is ~6pp, so compare means over >=3 seeds")
    p.add_argument("--no-select", action="store_true",
                   help="skip the val-accuracy checkpoint sweep, keep final weights")
    a = p.parse_args()

    check_data_fresh()
    adapter_path = Path(a.output)
    adapter_path.mkdir(parents=True, exist_ok=True)

    cfg = {**DEFAULTS, "iters": a.iters, "seed": a.seed}
    train(adapter_path, cfg)
    if not a.no_select:
        select_checkpoint(adapter_path)

    print(f"\nNext: python score.py --adapter {adapter_path}")


if __name__ == "__main__":
    main()
