import requests
from config import MISTRAL_API_KEY, MISTRAL_MODEL

API_URL = "https://api.mistral.ai/v1/chat/completions"

def query_mistral(prompt: str) -> str:
    response = requests.post(
        API_URL,
        headers={
            "Authorization": f"Bearer {MISTRAL_API_KEY}",
            "Content-Type": "application/json"
        },
        json={
            "model": MISTRAL_MODEL,
            "response_format": {"type": "json_object"},
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
            "temperature": 0
        },
        timeout=60
    )
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"]
