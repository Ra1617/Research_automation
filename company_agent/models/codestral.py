from typing import Any, Dict
from langchain_openai import ChatOpenAI
from langchain_core.runnables import RunnableLambda

def build_codestral_llm(api_key: str, model: str) -> ChatOpenAI:
    """Build a Codestral LLM instance using OpenAI adapter."""
    return ChatOpenAI(
        api_key=api_key,
        base_url="https://codestral.mistral.ai/v1",
        model=model,
        temperature=0.2,
        max_retries=1,
    )

def build_codestral_chain(api_key: str, model: str, make_chain_fn) -> RunnableLambda:
    """Build a Codestral chain with safe invocation."""
    llm = build_codestral_llm(api_key, model)
    chain = make_chain_fn(llm)
    
    def safe_invoke(input_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return chain.invoke(input_data)
        except Exception as exc:
            return {
                "_error": f"{type(exc).__name__}: {exc}",
            }
    
    return RunnableLambda(safe_invoke)
