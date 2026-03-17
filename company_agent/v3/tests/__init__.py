"""Tests for v3 validators — runs without LLM calls."""
import sys
from pathlib import Path

# Ensure company_agent is on path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
