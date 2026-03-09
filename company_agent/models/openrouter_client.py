import requests
from config import OPENROUTER_API_KEY, OPENROUTER_MODEL

API_URL = "https://openrouter.ai/api/v1/chat/completions"

def query_openrouter(prompt: str) -> str:
    response = requests.post(
        API_URL,
        headers={
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://company-agent.local",
            "X-Title": "Company Research Agent"
        },
        json={
            "model": OPENROUTER_MODEL,
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
