from typing import Any, Dict
from langchain_groq import ChatGroq
from langchain_core.runnables import RunnableLambda

def build_groq_llm(api_key: str, model: str) -> ChatGroq:
    """Build a Groq LLM instance."""
    return ChatGroq(
        api_key=api_key,
        model=model,
        temperature=0.2,
        max_retries=1,
    )

def build_groq_chain(api_key: str, model: str, make_chain_fn) -> RunnableLambda:
    """Build a Groq chain with safe invocation."""
    llm = build_groq_llm(api_key, model)
    chain = make_chain_fn(llm)
    
    def safe_invoke(input_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return chain.invoke(input_data)
        except Exception as exc:
            return {
                "_error": f"{type(exc).__name__}: {exc}",
            }
    
    return RunnableLambda(safe_invoke)
