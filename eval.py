"""
Evaluate the fine-tuned BLS Data Agent against the base model and teacher.

Compares:
  - Base model (zero-shot) performance
  - Fine-tuned model performance
  - Teacher baseline (if available)

Metrics:
  - Tool selection accuracy (did it pick the right tool?)
  - Argument accuracy (did it pick the right series_id/query?)
  - Exact match (did it produce the exact correct call?)
"""

import argparse
import json
import re
from pathlib import Path
from typing import Optional


def parse_tool_call(text: str) -> Optional[tuple[str, dict]]:
    """Extract tool name and arguments from model output."""
    # Match: tool_name(arg1="val1", arg2="val2")
    pattern = r'(\w+)\((.*)\)'
    match = re.search(pattern, text)
    if not match:
        return None
    tool = match.group(1)
    args_str = match.group(2)

    # Parse arguments
    args = {}
    for pair in args_str.split(","):
        pair = pair.strip()
        if "=" in pair:
            key, val = pair.split("=", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            try:
                val = json.loads(val)
            except (json.JSONDecodeError, ValueError):
                pass
            args[key] = val

    return tool, args


def evaluate(
    model_outputs: list[str],
    ground_truth: list[dict],
    model_name: str = "model",
) -> dict:
    """Evaluate model outputs against ground truth."""
    correct_tool = 0
    correct_args = 0
    exact_match = 0
    total = len(ground_truth)

    for i, (output, gt) in enumerate(zip(model_outputs, ground_truth)):
        gt_tool = gt["tool"]
        gt_args = gt["arguments"]

        parsed = parse_tool_call(output)
        if parsed is None:
            continue

        pred_tool, pred_args = parsed

        if pred_tool == gt_tool:
            correct_tool += 1

            # Check argument accuracy
            arg_ok = True
            for key, val in gt_args.items():
                if key not in pred_args or str(pred_args[key]) != str(val):
                    arg_ok = False
                    break
            if arg_ok:
                correct_args += 1

                # Exact match: tool + all args match
                if set(pred_args.keys()) == set(gt_args.keys()):
                    exact_match += 1

    return {
        "model": model_name,
        "total": total,
        "tool_accuracy": correct_tool / total if total else 0,
        "argument_accuracy": correct_args / total if total else 0,
        "exact_match": exact_match / total if total else 0,
    }


def evaluate_base_model(model_name: str, test_data: list[dict]) -> dict:
    """Evaluate a base model (load from HuggingFace, run inference)."""
    from transformers import AutoModelForCausalLM, AutoTokenizer
    import torch

    print(f"Loading {model_name}...")
    tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(
        model_name,
        torch_dtype=torch.float16,
        device_map="auto",
        trust_remote_code=True,
    )

    outputs = []
    system_prompt = test_data[0]["messages"][0]["content"]

    for example in test_data[:50]:  # Evaluate on first 50
        user_msg = example["messages"][1]["content"]
        prompt = f"<|im_start|>system\n{system_prompt}<|im_end|>\n<|im_start|>user\n{user_msg}<|im_end|>\n<|im_start|>assistant\n"

        inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
        with torch.no_grad():
            generated = model.generate(
                **inputs,
                max_new_tokens=128,
                temperature=0.0,
                do_sample=False,
            )
        response = tokenizer.decode(generated[0][inputs.input_ids.shape[1]:], skip_special_tokens=True)
        outputs.append(response.strip())

    ground_truth = [
        {"tool": ex["messages"][2]["content"].split("(")[0],
         "arguments": _parse_args_from_tool_call(ex["messages"][2]["content"])}
        for ex in test_data[:50]
    ]

    return evaluate(outputs, ground_truth, model_name)


def _parse_args_from_tool_call(text: str) -> dict:
    """Parse arguments from a tool call string."""
    _, args = parse_tool_call(text) or ("", {})
    return args


def evaluate_from_jsonl(test_path: str, predictions_path: str) -> dict:
    """Evaluate from a JSONL file of predictions."""
    with open(test_path) as f:
        test_data = [json.loads(line) for line in f]

    with open(predictions_path) as f:
        predictions = [line.strip() for line in f]

    ground_truth = [
        {"tool": ex["messages"][2]["content"].split("(")[0],
         "arguments": _parse_args_from_tool_call(ex["messages"][2]["content"])}
        for ex in test_data
    ]

    return evaluate(predictions, ground_truth, "fine-tuned")


def main():
    parser = argparse.ArgumentParser(description="Evaluate BLS Data Agent")
    parser.add_argument("--test-data", default="train.jsonl", help="Test data JSONL")
    parser.add_argument("--predictions", help="Model predictions (one per line)")
    parser.add_argument("--base-model", help="Evaluate base model (HuggingFace name)")
    args = parser.parse_args()

    if args.base_model:
        with open(args.test_data) as f:
            test_data = [json.loads(line) for line in f]
        results = evaluate_base_model(args.base_model, test_data)
    elif args.predictions:
        results = evaluate_from_jsonl(args.test_data, args.predictions)
    else:
        print("Provide --predictions or --base-model")
        return

    print("\n" + "=" * 50)
    print(f"Model: {results['model']}")
    print(f"Examples: {results['total']}")
    print(f"Tool accuracy: {results['tool_accuracy']:.1%}")
    print(f"Argument accuracy: {results['argument_accuracy']:.1%}")
    print(f"Exact match: {results['exact_match']:.1%}")
    print("=" * 50)


if __name__ == "__main__":
    main()