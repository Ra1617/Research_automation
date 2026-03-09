"""
LLM Factory — v2
==================
Provides lazy-loaded LLM instances for the 3 providers:
- NVIDIA (Llama 4 Maverick via NVIDIA's OpenAI-compatible endpoint)
- Groq (Llama 3.3 70b)
- Mistral (Mistral Large)

Each function returns a LangChain chat model instance.
Uses API keys from .env.
"""

import os
import random
from typing import Optional
from dotenv import load_dotenv

load_dotenv(override=True)


def get_nvidia():
    """Build NVIDIA LLM via OpenAI-compatible endpoint."""
    from langchain_openai import ChatOpenAI

    api_key = os.getenv("NVIDIA_API_KEY")
    if not api_key:
        return None

    return ChatOpenAI(
        api_key=api_key,
        base_url="https://integrate.api.nvidia.com/v1",
        model=os.getenv("NVIDIA_MODEL", "meta/llama-4-maverick-17b-128e-instruct"),
        temperature=0.2,
        max_retries=1,
    )


def get_groq():
    """Build Groq LLM instance."""
    from langchain_groq import ChatGroq

    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return None

    return ChatGroq(
        api_key=api_key,
        model=os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"),
        temperature=0.2,
        max_retries=1,
    )


def get_mistral():
    """Build Mistral LLM instance."""
    from langchain_mistralai.chat_models import ChatMistralAI

    api_key = os.getenv("MISTRAL_API_KEY")
    if not api_key:
        return None

    return ChatMistralAI(
        api_key=api_key,
        model=os.getenv("MISTRAL_MODEL", "mistral-large-latest"),
        temperature=0.2,
        max_retries=1,
    )


def get_random_llm():
    """Return a randomly selected available LLM instance and its name.

    Returns:
        tuple: (llm_instance, provider_name) or (None, None) if none available
    """
    providers = [
        ("nvidia", get_nvidia),
        ("groq", get_groq),
        ("mistral", get_mistral),
    ]

    # Shuffle to randomize selection
    random.shuffle(providers)

    for name, factory in providers:
        try:
            llm = factory()
            if llm is not None:
                return llm, name
        except Exception as e:
            print(f"[LLM] Failed to initialize {name}: {e}")
            continue

    return None, None


def get_consolidation_llm():
    """Return an LLM for consolidation (prefers Mistral for quality)."""
    # Try Mistral first (best for structured consolidation)
    llm = get_mistral()
    if llm:
        return llm

    # Fallback to Groq
    llm = get_groq()
    if llm:
        return llm

    # Last resort: NVIDIA
    llm = get_nvidia()
    if llm:
        return llm

    raise RuntimeError("No LLM available for consolidation. Check API keys in .env")

