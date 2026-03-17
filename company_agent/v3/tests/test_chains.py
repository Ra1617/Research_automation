"""
Tests for LangChain chain objects and their use by agent nodes.
These are lightweight and avoid real API calls by monkeypatching the chains.
"""

from __future__ import annotations

from typing import Any, Dict

import pytest

from v3 import chains, agents


class DummyChain:
    def __init__(self, output: Dict[str, Any]):
        self.output = output

    def invoke(self, _input: Dict[str, Any]) -> Dict[str, Any]:
        return self.output


def test_chain_objects_exist():
    # basic sanity: each research chain is defined (may be Runnable or None)
    assert hasattr(chains, "groq_chain")
    assert hasattr(chains, "mistral_chain")
    assert hasattr(chains, "nvidia_chain")
    # consolidation chain can be None if HF_TOKEN unset
    assert hasattr(chains, "hf_consolidation_chain")


def test_workers_use_chains(monkeypatch):
    dummy = DummyChain({"name": "TestCo"})
    # patch the traceable run helpers instead of chains themselves
    monkeypatch.setattr(agents, "run_groq_chain", lambda x: dummy.invoke(x))
    monkeypatch.setattr(agents, "run_mistral_chain", lambda x: dummy.invoke(x))
    monkeypatch.setattr(agents, "run_nvidia_chain", lambda x: dummy.invoke(x))

    for func in (agents.worker_groq, agents.worker_mistral, agents.worker_nvidia):
        res = func({"input": {"company_name": "X"}})
        assert res["llm_outputs"][0]["raw"]["name"] == "TestCo"
        assert res["llm_outputs"][0]["error"] is None


def test_hf_consolidation_chain_invoked(monkeypatch):
    # simulate presence of consolidation chain
    dummy = DummyChain({"merged": True})
    monkeypatch.setattr(agents, "run_hf_consolidation", lambda x: dummy.invoke(x))

    outputs = [
        {"provider": "a", "raw": {"name": "A"}, "error": None},
        {"provider": "b", "raw": {"name": "B"}, "error": None},
        {"provider": "c", "raw": {"name": "C"}, "error": None},
    ]
    result = agents._hf_consolidate(outputs, "Foo")
    assert result == {"merged": True}

    # absent chain/helper should return None
    monkeypatch.setattr(agents, "hf_consolidation_chain", None)
    monkeypatch.setattr(agents, "run_hf_consolidation", None)
    assert agents._hf_consolidate(outputs, "Foo") is None
