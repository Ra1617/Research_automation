import requests
from config import HUGGINGFACE_API_KEY, HUGGINGFACE_MODEL

def get_api_url():
    if HUGGINGFACE_MODEL.startswith("http"):
        return HUGGINGFACE_MODEL
    return f"https://api-inference.huggingface.co/models/{HUGGINGFACE_MODEL}/v1/chat/completions"

def query_huggingface(prompt: str) -> str:
    response = requests.post(
        get_api_url(),
        headers={
            "Authorization": f"Bearer {HUGGINGFACE_API_KEY}",
            "Content-Type": "application/json"
        },
        json={
            "model": HUGGINGFACE_MODEL,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a strict JSON API for company research. "
                        "Return ONLY a raw JSON object — no markdown, no backticks, no explanation, no preamble. "
                        "Every key requested must appear in the output. Use null for unknown values."
                    )
                },
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.1,
            "max_tokens": 8000
        },
        timeout=120
    )
    
    # Allow 429/500/etc to raise exception so orchestrator can retry
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"]
