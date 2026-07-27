"""
Fine-tune a small language model for BLS data tool calling.

Two paths:
  A) Apple Silicon (M4) → MLX LoRA  (fastest, native)
  B) CUDA GPU (Kaggle T4) → Unsloth QLoRA (battle-tested)

Usage:
  # Path A — M4 Mac
  uv run python train.py --backend mlx --model Qwen/Qwen3-1.7B

  # Path B — Kaggle/Colab GPU
  uv run python train.py --backend unsloth --model Qwen/Qwen3-1.7B
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Optional


def load_dataset(path: str) -> list[dict]:
    """Load ShareGPT-format training data."""
    data = []
    with open(path) as f:
        for line in f:
            ex = json.loads(line)
            if "messages" in ex:
                data.append(ex["messages"])
    print(f"Loaded {len(data)} training examples from {path}")
    return data


# ═══════════════════════════════════════════════
# Path A: MLX (Apple Silicon)
# ═══════════════════════════════════════════════

def train_mlx(model_name: str, data: list, output_dir: str, epochs: int = 4):
    """Fine-tune using MLX LoRA (Apple Silicon native)."""
    import mlx.core as mx
    from mlx_lm import load, generate
    from mlx_lm.lora import LoRA, train_model

    print(f"╔══ MLX LoRA training ══╗")
    print(f"Model: {model_name}")
    print(f"Examples: {len(data)}")
    print(f"Epochs: {epochs}")
    print(f"Device: MPS (Apple Silicon)")

    # Convert messages to instruction format
    train_data = []
    for msgs in data:
        system = msgs[0]["content"]
        user = msgs[1]["content"]
        assistant = msgs[2]["content"]
        train_data.append({
            "text": f"<|im_start|>system\n{system}<|im_end|>\n<|im_start|>user\n{user}<|im_end|>\n<|im_start|>assistant\n{assistant}<|im_end|>"
        })

    # Load model and apply LoRA
    model, tokenizer = load(model_name)

    lora = LoRA(
        model=model,
        r=16,
        alpha=32,
        dropout=0.05,
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj"],
    )

    train_model(
        model,
        tokenizer,
        train_data,
        lora_adapters=lora,
        learning_rate=5e-5,
        epochs=epochs,
        batch_size=2,
        grad_accum=4,
        output_dir=output_dir,
        val_split=0.1,
        save_every=100,
    )

    print(f"Model saved to {output_dir}")
    return output_dir


# ═══════════════════════════════════════════════
# Path B: Unsloth QLoRA (CUDA GPU)
# ═══════════════════════════════════════════════

def train_unsloth(model_name: str, data: list, output_dir: str, epochs: int = 4):
    """Fine-tune using Unsloth QLoRA (CUDA GPU)."""
    from unsloth import FastLanguageModel
    from unsloth.chat_templates import get_chat_template, standardize_sharegpt
    import torch
    from datasets import Dataset
    from transformers import TrainingArguments
    from trl import SFTTrainer

    print(f"╔══ Unsloth QLoRA training ══╗")
    print(f"Model: {model_name}")
    print(f"Examples: {len(data)}")
    print(f"Epochs: {epochs}")
    print(f"Device: {torch.cuda.get_device_name(0)}")

    # Load 4-bit model
    model, tokenizer = FastLanguageModel.from_pretrained(
        model_name=model_name,
        max_seq_length=2048,
        load_in_4bit=True,
        fast_inference=False,
    )

    # Apply LoRA
    model = FastLanguageModel.get_peft_model(
        model,
        r=16,
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj",
                        "gate_proj", "up_proj", "down_proj"],
        lora_alpha=32,
        lora_dropout=0.05,
        bias="none",
        use_gradient_checkpointing=True,
    )

    # Convert to HF dataset
    tokenizer = get_chat_template(tokenizer, chat_template="chatml")
    hf_data = Dataset.from_list([{"messages": m} for m in data])
    hf_data = standardize_sharegpt(hf_data)

    def formatting_func(examples):
        return tokenizer.apply_chat_template(
            examples["messages"], tokenize=False, add_generation_prompt=False
        )

    trainer = SFTTrainer(
        model=model,
        tokenizer=tokenizer,
        train_dataset=hf_data,
        formatting_func=formatting_func,
        args=TrainingArguments(
            output_dir=output_dir,
            per_device_train_batch_size=2,
            gradient_accumulation_steps=4,
            num_train_epochs=epochs,
            learning_rate=5e-5,
            lr_scheduler_type="cosine",
            warmup_ratio=0.05,
            logging_steps=10,
            save_strategy="epoch",
            bf16=True,
            optim="adamw_8bit",
            report_to="none",
        ),
    )

    trainer.train()

    # Save LoRA adapter
    model.save_pretrained(output_dir)
    tokenizer.save_pretrained(output_dir)
    print(f"Adapter saved to {output_dir}")

    # Merge and save full model
    print("Merging LoRA weights...")
    model = model.merge_and_unload()
    merged_dir = os.path.join(output_dir, "merged")
    model.save_pretrained(merged_dir)
    tokenizer.save_pretrained(merged_dir)
    print(f"Merged model saved to {merged_dir}")

    return merged_dir


# ═══════════════════════════════════════════════
# GGUF Export
# ═══════════════════════════════════════════════

def export_gguf(model_dir: str, output_path: str, quant: str = "q4_k_m"):
    """Export merged model to GGUF format using llama.cpp."""
    import subprocess

    llama_cpp_dir = os.path.expanduser("~/llama.cpp")
    convert_script = os.path.join(llama_cpp_dir, "convert_hf_to_gguf.py")

    if not os.path.exists(convert_script):
        print("llama.cpp not found. Install: git clone https://github.com/ggerganov/llama.cpp ~/llama.cpp")
        print("Then: cd ~/llama.cpp && make")
        return

    # Convert to GGUF FP16
    fp16_path = output_path.replace(".gguf", "_fp16.gguf")
    subprocess.run([
        sys.executable, convert_script, model_dir,
        "--outfile", fp16_path,
        "--outtype", "f16",
    ], check=True)

    # Quantize
    quantize_bin = os.path.join(llama_cpp_dir, "llama-quantize")
    subprocess.run([
        quantize_bin, fp16_path, output_path, quant.upper()
    ], check=True)

    # Clean up FP16
    os.remove(fp16_path)

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"GGUF exported: {output_path} ({size_mb:.0f} MB, {quant})")


# ═══════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Fine-tune BLS Data Agent")
    parser.add_argument("--backend", choices=["mlx", "unsloth"], default="unsloth",
                        help="Training backend: mlx (Apple Silicon) or unsloth (CUDA)")
    parser.add_argument("--model", default="Qwen/Qwen3-1.7B",
                        help="Base model name or path")
    parser.add_argument("--data", default="train.jsonl",
                        help="Path to training data (JSONL)")
    parser.add_argument("--epochs", type=int, default=4,
                        help="Training epochs")
    parser.add_argument("--output", default="models/bls-agent",
                        help="Output directory")
    parser.add_argument("--export-gguf", action="store_true",
                        help="Export to GGUF after training")
    parser.add_argument("--quant", default="q4_k_m",
                        choices=["q4_k_m", "q4_k_s", "q8_0", "f16"],
                        help="GGUF quantization level")
    args = parser.parse_args()

    data = load_dataset(args.data)
    os.makedirs(args.output, exist_ok=True)

    if args.backend == "mlx":
        model_dir = train_mlx(args.model, data, args.output, args.epochs)
    else:
        model_dir = train_unsloth(args.model, data, args.output, args.epochs)

    if args.export_gguf:
        gguf_path = os.path.join(args.output, f"bls-agent-{args.quant}.gguf")
        export_gguf(model_dir, gguf_path, args.quant)

    print(f"\n✓ Training complete. Model: {model_dir}")


if __name__ == "__main__":
    main()