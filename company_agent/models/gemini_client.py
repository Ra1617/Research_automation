import google.generativeai as genai
from config import GEMINI_API_KEY, GEMINI_MODEL

genai.configure(api_key=GEMINI_API_KEY)

SYSTEM_INSTRUCTION = (
    "You are a strict JSON API for company research. "
    "Return ONLY a raw JSON object — no markdown, no backticks, no explanation, no preamble. "
    "Every key requested must appear in the output. Use null for unknown values."
)

def query_gemini(prompt: str) -> str:
    model = genai.GenerativeModel(
        model_name=GEMINI_MODEL,
        system_instruction=SYSTEM_INSTRUCTION,
        generation_config={"temperature": 0, "max_output_tokens": 8192}
    )
    response = model.generate_content(prompt)
    return response.text
