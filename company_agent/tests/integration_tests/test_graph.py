import os
import pytest

from services import graph

pytestmark = pytest.mark.anyio


@pytest.mark.langsmith
@pytest.mark.skipif(
    not os.getenv("GEMINI_API_KEY") and not os.getenv("GROQ_API_KEY") and not os.getenv("OPENROUTER_API_KEY"),
    reason="Requires at least one LLM API key to be configured"
)
async def test_agent_with_real_llm() -> None:
    """Integration test with real LLM providers (requires API keys)."""
    inputs = {"company_name": "OpenAI"}
    res = await graph.ainvoke(inputs)
    assert res is not None
    assert "result" in res
