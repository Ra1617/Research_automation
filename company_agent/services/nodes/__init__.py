"""Nodes for the Company Intelligence Aggregator LangGraph.

Each node is a modular building block in the multi-provider orchestration pipeline:

1. Generation nodes (per provider): LLM inference with retry support
2. Validation node: Field-by-field checks against metadata rules
3. Consolidation node: Score and select best values across providers
4. Router node: Conditional retry logic

Nodes are created via factory functions for dependency injection and lazy loading.
"""

from services.nodes.generation import create_generation_node
from services.nodes.validation import create_validation_node
from services.nodes.consolidation import create_consolidation_node
from services.nodes.router import should_retry

__all__ = [
    "create_generation_node",
    "create_validation_node",
    "create_consolidation_node",
    "should_retry",
]
