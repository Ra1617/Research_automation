from typing import Any, Dict
from langchain_mistralai.chat_models import ChatMistralAI
from langchain_core.runnables import RunnableLambda

def build_mistral_llm(api_key: str, model: str) -> ChatMistralAI:
    """Build a Mistral LLM instance."""
    return ChatMistralAI(
        api_key=api_key,
        model=model,
        temperature=0.2,
        max_retries=1,
    )

def build_mistral_chain(api_key: str, model: str, make_chain_fn) -> RunnableLambda:
    """Build a Mistral chain with safe invocation."""
    llm = build_mistral_llm(api_key, model)
    chain = make_chain_fn(llm)
    
    def safe_invoke(input_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return chain.invoke(input_data)
        except Exception as exc:
            return {
                "_error": f"{type(exc).__name__}: {exc}",
            }
    
    return RunnableLambda(safe_invoke)
