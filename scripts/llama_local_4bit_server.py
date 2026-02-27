"""
Example local 4-bit Llama inference server for AutoEditor.

WARNING:
- 405B is extremely large (800+ GB weights) and not practical on most single-node setups.
- Use hosted Hugging Face inference for 405B in production.
- For local testing, prefer 70B/8B quantized variants.

Usage:
  pip install torch transformers accelerate bitsandbytes fastapi uvicorn
  set HF_TOKEN=...
  set LOCAL_LLAMA_MODEL=meta-llama/Meta-Llama-3.1-70B-Instruct
  python scripts/llama_local_4bit_server.py
"""

import os
from typing import Any, Dict, List, Optional

import torch
from fastapi import FastAPI
from pydantic import BaseModel
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig

MODEL_ID = os.getenv("LOCAL_LLAMA_MODEL", "meta-llama/Meta-Llama-3.1-70B-Instruct")
HF_TOKEN = os.getenv("HF_TOKEN", "").strip() or None
MAX_INPUT_TOKENS = int(os.getenv("LOCAL_LLAMA_MAX_INPUT_TOKENS", "4096"))

bnb_config = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_use_double_quant=True,
    bnb_4bit_quant_type="nf4",
    bnb_4bit_compute_dtype=torch.float16,
)

tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, token=HF_TOKEN)
model = AutoModelForCausalLM.from_pretrained(
    MODEL_ID,
    token=HF_TOKEN,
    quantization_config=bnb_config,
    torch_dtype=torch.float16,
    device_map="auto",
)
model.eval()

app = FastAPI(title="AutoEditor Local Llama 4-bit Server")


class Message(BaseModel):
    role: str
    content: str


class CompletionRequest(BaseModel):
    model: Optional[str] = None
    prompt: Optional[str] = None
    inputs: Optional[str] = None
    messages: Optional[List[Message]] = None
    max_tokens: Optional[int] = 512
    max_new_tokens: Optional[int] = None
    temperature: Optional[float] = 0.2
    top_p: Optional[float] = 0.9


def resolve_prompt(req: CompletionRequest) -> str:
    if req.prompt:
        return req.prompt
    if req.inputs:
        return req.inputs
    if req.messages:
        merged = "\n".join(msg.content for msg in req.messages if msg.content)
        if merged.strip():
            return merged.strip()
    return ""


@app.post("/v1/completions")
def completions(req: CompletionRequest) -> Dict[str, Any]:
    prompt = resolve_prompt(req)
    if not prompt:
        return {"error": "empty_prompt"}

    max_new_tokens = int(req.max_new_tokens or req.max_tokens or 512)
    max_new_tokens = max(32, min(max_new_tokens, 1200))
    temperature = float(req.temperature or 0.2)
    top_p = float(req.top_p or 0.9)

    encoded = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=MAX_INPUT_TOKENS).to(model.device)
    with torch.inference_mode():
        generated = model.generate(
            **encoded,
            max_new_tokens=max_new_tokens,
            do_sample=temperature > 0,
            temperature=max(0.0, min(temperature, 1.2)),
            top_p=max(0.1, min(top_p, 1.0)),
            eos_token_id=tokenizer.eos_token_id,
            pad_token_id=tokenizer.eos_token_id,
        )
    completion_ids = generated[0][encoded["input_ids"].shape[1]:]
    text = tokenizer.decode(completion_ids, skip_special_tokens=True).strip()

    return {
        "model": MODEL_ID,
        "generated_text": text,
        "text": text,
        "choices": [{"text": text, "message": {"role": "assistant", "content": text}}],
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
