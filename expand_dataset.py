"""
Synthetic data expansion for BLS Data Agent distillation.

Takes the seed dataset (70 examples) and uses a teacher LLM to generate
diverse variations, producing a 5K-10K example training set in ShareGPT format.

Usage:
    python expand_dataset.py --seed seed_dataset.py --output train.jsonl --count 5000
"""

import json
import argparse
import os
import random
import time
import sys
from pathlib import Path
from typing import Any

# ── Prompt templates for the teacher model ──

SYSTEM_PROMPT = """You are a BLS economic data assistant. You have access to these tools:

- get_series(series_id, start?, end?) — Fetch time-series data for a BLS series by ID. 
  Example: get_series("CUUR0000SA0", "2020", "2024") gets CPI All Items for 2020-2024.

- list_surveys() — List all available BLS surveys (CPI, employment, wages, PPI, etc.)

- popular_series(survey?) — Get the most-requested series for a survey.
  Example: popular_series("CU") gets popular CPI series.

- search_series(query, limit?) — Search the CPI catalog by keyword.
  Example: search_series("food", 10) finds series about food prices.

- get_series_info(series_id) — Get metadata about a series (title, survey, seasonality).

- analyze_cpi_seasonality(series_id, start?, end?) — Analyze month-over-month seasonal patterns
  with percentile bands and current year comparison.

Always respond with the correct tool call. ONLY use tools listed above."""

VARIATION_PROMPTS = [
    "Rephrase this question differently. Return ONLY a JSON object with 'question', 'tool', 'arguments'.",
    "Ask this question in a more conversational way. Return ONLY JSON.",
    "Ask this question as if you're an economics student. Return ONLY JSON.",
    "Ask this as a short, direct query. Return ONLY JSON.",
    "Ask this as a detailed analytical request. Return ONLY JSON.",
    "Generate a similar but distinct question about a related economic topic. Return ONLY JSON.",
    "Ask this question using different economic terminology. Return ONLY JSON.",
    "Ask this question in a more technical/professional tone. Return ONLY JSON.",
]

COMPOUND_PROMPT = """Generate a compound question that requires two tool calls. 
The first call discovers or searches, the second fetches data or analyzes.
Return ONLY JSON with 'question', 'tool', 'arguments', 'next_tool', 'next_arguments'."""

VALID_SERIES_IDS = [
    "CUUR0000SA0", "CUUR0000SAF1", "CUUR0000SETG01", "CUUR0000SAH1",
    "CUUR0000SAM1", "CUUR0000SAT1", "CUUR0000SAA1", "CUUR0000SA0L1E",
    "CUUR0000SAE1", "CUUR0000SAR1", "CUUR0000SAC1", "CUUR0000SAS1",
    "CUUR0000SAF11", "CUUR0000SEFV01", "CUUR0000SETA01", "CUUR0000SETA02",
    "CUUR0000SETB01", "CUUR0000SEHF01", "CUUR0000SEHA01", "CUUR0000SEHC01",
    "LNS14000000", "LNS11300000", "LNS12300000",
    "CES0000000001", "CES0500000003",
]

VALID_SURVEYS = ["CU", "CE", "LN", "PC", "PR", "JT", "LE", "EN", "OE", "SM", "LA"]
VALID_TOOLS = ["get_series", "list_surveys", "popular_series", "search_series",
               "get_series_info", "analyze_cpi_seasonality"]

CP_SEARCH_TERMS = [
    "dairy", "airline", "prescription", "tuition", "beef", "alcoholic beverages",
    "internet", "hotel", "tobacco", "baby food", "coffee", "bread", "eggs",
    "milk", "butter", "chicken", "fish", "cereal", "wine", "beer",
    "rent", "mortgage", "utilities", "phone", "cable", "streaming",
    "furniture", "appliances", "clothing", "shoes", "jewelry",
    "car insurance", "health insurance", "life insurance",
    "dentist", "hospital", "nursing home", "eyeglasses",
    "pet food", "veterinary", "haircut", "laundry", "dry cleaning",
]


def validate_example(example: dict) -> bool:
    """Validate that an example has correct structure."""
    if not all(k in example for k in ("question", "tool", "arguments")):
        return False
    if example["tool"] not in VALID_TOOLS:
        return False

    args = example["arguments"]
    tool = example["tool"]

    if tool == "get_series":
        if "series_id" not in args:
            return False
        sid = args["series_id"]
        if not (sid.startswith(("CU", "LN", "CE")) and len(sid) > 6):
            return False
        for key in args:
            if key not in ("series_id", "start", "end"):
                return False

    elif tool == "list_surveys":
        if args != {}:
            return False

    elif tool == "popular_series":
        for key in args:
            if key != "survey":
                return False
        if "survey" in args and args["survey"] not in VALID_SURVEYS:
            return False

    elif tool == "search_series":
        if "query" not in args:
            return False
        for key in args:
            if key not in ("query", "limit"):
                return False

    elif tool == "get_series_info":
        if "series_id" not in args:
            return False

    elif tool == "analyze_cpi_seasonality":
        if "series_id" not in args:
            return False
        for key in args:
            if key not in ("series_id", "start", "end"):
                return False

    return True


def format_for_training(example: dict) -> dict:
    """Format a validated example into ShareGPT training format."""
    user_msg = example["question"]
    tool = example["tool"]
    args = example["arguments"]

    # Format the assistant response as a tool call
    args_str = ", ".join(f"{k}={json.dumps(v)}" for k, v in args.items())
    assistant_msg = f"{tool}({args_str})"

    # Build multi-turn if compound
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_msg},
        {"role": "assistant", "content": assistant_msg},
    ]

    return {"messages": messages}


def generate_synthetic_examples(seed_data: list, count: int) -> list:
    """
    Generate synthetic examples from seed data using rule-based variations.
    In production, this would call a teacher LLM API.

    This rule-based version generates ~8 variations per seed + random combinations.
    """
    examples = []
    seen_questions = set()

    for seed in seed_data:
        # Add the original
        q = seed["question"]
        if q not in seen_questions:
            seen_questions.add(q)
            examples.append(dict(seed))

        # Generate variations by swapping series IDs, dates, survey codes
        tool = seed["tool"]
        args = dict(seed["arguments"])

        if tool == "get_series":
            # Generate variations with different date ranges
            for start in ["2018", "2019", "2020", "2021", "2022"]:
                for end in ["2023", "2024", "2025"]:
                    if start >= end:
                        continue
                    new_args = {**args, "start": start, "end": end}
                    new_q = seed["question"].replace(
                        args.get("start", "2020"), start
                    ).replace(
                        args.get("end", "2024"), end
                    )
                    if new_q not in seen_questions:
                        seen_questions.add(new_q)
                        examples.append({
                            "question": new_q, "tool": tool, "arguments": new_args
                        })

            # Generate variations with different series IDs
            for sid in random.sample(VALID_SERIES_IDS, min(3, len(VALID_SERIES_IDS))):
                if sid == args.get("series_id"):
                    continue
                new_args = {**args, "series_id": sid}
                new_q = f"Show me data for series {sid}."
                if new_q not in seen_questions:
                    seen_questions.add(new_q)
                    examples.append({
                        "question": new_q, "tool": tool, "arguments": new_args
                    })

        elif tool == "popular_series":
            for survey in VALID_SURVEYS:
                new_args = {"survey": survey}
                new_q = f"What are the popular series in the {survey} survey?"
                if new_q not in seen_questions:
                    seen_questions.add(new_q)
                    examples.append({
                        "question": new_q, "tool": tool, "arguments": new_args
                    })

        elif tool == "search_series":
            for term in random.sample(CP_SEARCH_TERMS, min(5, len(CP_SEARCH_TERMS))):
                new_args = {"query": term}
                new_q = f"Search for BLS series about {term}."
                if new_q not in seen_questions:
                    seen_questions.add(new_q)
                    examples.append({
                        "question": new_q, "tool": tool, "arguments": new_args
                    })

        elif tool == "get_series_info":
            for sid in random.sample(VALID_SERIES_IDS, min(3, len(VALID_SERIES_IDS))):
                new_args = {"series_id": sid}
                new_q = f"What information do you have on series {sid}?"
                if new_q not in seen_questions:
                    seen_questions.add(new_q)
                    examples.append({
                        "question": new_q, "tool": tool, "arguments": new_args
                    })

        elif tool == "analyze_cpi_seasonality":
            for sid in random.sample(
                [s for s in VALID_SERIES_IDS if s.startswith("CU")], min(3, 5)
            ):
                new_args = {"series_id": sid}
                new_q = f"Analyze the seasonal pattern of series {sid}."
                if new_q not in seen_questions:
                    seen_questions.add(new_q)
                    examples.append({
                        "question": new_q, "tool": tool, "arguments": new_args
                    })

    # Shuffle and limit
    random.shuffle(examples)
    return examples[:count]


def main():
    parser = argparse.ArgumentParser(description="Expand BLS seed dataset")
    parser.add_argument("--seed", default="seed_dataset.py", help="Path to seed dataset")
    parser.add_argument("--output", default="train.jsonl", help="Output JSONL path")
    parser.add_argument("--count", type=int, default=5000, help="Target example count")
    args = parser.parse_args()

    # Load seed data
    seed_path = Path(args.seed)
    if seed_path.suffix == ".py":
        import importlib.util
        spec = importlib.util.spec_from_file_location("seed", seed_path)
        seed_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(seed_module)
        seed_data = seed_module.SEED_DATA
    else:
        with open(seed_path) as f:
            seed_data = json.load(f)

    print(f"Loaded {len(seed_data)} seed examples")

    # Generate synthetic examples
    examples = generate_synthetic_examples(seed_data, args.count)

    # Validate and format
    valid = []
    rejected = 0
    for ex in examples:
        if validate_example(ex):
            valid.append(format_for_training(ex))
        else:
            rejected += 1

    # Write output
    with open(args.output, "w") as f:
        for ex in valid:
            f.write(json.dumps(ex) + "\n")

    print(f"Generated: {len(valid)} valid training examples")
    print(f"Rejected: {rejected}")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()