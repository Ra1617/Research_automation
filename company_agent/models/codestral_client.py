import requests
from config import CODESTRAL_API_KEY, CODESTRAL_MODEL

API_URL = "https://codestral.mistral.ai/v1/chat/completions"

def query_codestral(prompt: str) -> str:
    response = requests.post(
        API_URL,
        headers={
            "Authorization": f"Bearer {CODESTRAL_API_KEY}",
            "Content-Type": "application/json"
        },
        json={
            "model": CODESTRAL_MODEL,
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
