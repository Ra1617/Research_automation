from groq import Groq
from config import GROQ_API_KEY, GROQ_MODEL

client = Groq(api_key=GROQ_API_KEY)

def query_groq(prompt: str) -> str:
    response = client.chat.completions.create(
        messages=[
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
        model=GROQ_MODEL,
        temperature=0,
        max_tokens=8000
    )
    return response.choices[0].message.content
