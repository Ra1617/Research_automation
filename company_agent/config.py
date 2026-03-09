import os
from dotenv import load_dotenv

load_dotenv(override=True)


CODESTRAL_API_KEY = os.getenv("CODESTRAL_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")

CODESTRAL_MODEL = os.getenv("CODESTRAL_MODEL", "codestral-latest")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-large-latest")

CHUNK_SIZE = 20         # Parameters per LLM call
MAX_RETRIES = 3         # Retry failed LLM calls
OUTPUT_FILE = "output/company_data.xlsx"
