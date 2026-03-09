from langgraph.graph.state import CompiledStateGraph

from services.graph import graph
from main import get_graph


def test_graph_compiles_successfully() -> None:
    """Test that the graph compiles without errors."""
    assert isinstance(graph, CompiledStateGraph)


def test_get_graph_returns_cached_instance() -> None:
    """Test that get_graph returns the same instance on multiple calls."""
    graph1 = get_graph()
    graph2 = get_graph()
    assert graph1 is graph2
