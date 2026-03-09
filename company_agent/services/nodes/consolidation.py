"""Consolidation node to score and select best values across providers.

The consolidation node:
1. Loads ConsolidationRuleSet from schema file or derives from metadata
2. For each field, scores each provider's value using field-specific rules
3. Selects the highest-scoring value and records attribution
4. Compiles final result with transparency (shows all scores)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

from langchain_core.runnables import RunnableLambda
from services.consolidation_rules import (
    load_rules_from_metadata_file,
    load_rules_from_schema_file,
)
from services.state import GraphState


def fallback_value_score(value: Any) -> int:
    """Fallback scoring when schema rules are not available.
    
    Simple heuristic based on content length and type.
    """
    if value is None:
        return 0
    
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return 0
        if cleaned.lower() in {"unknown", "n/a", "na", "none", "tbd", "todo"}:
            return 5  # Penalize placeholder values
        return min(len(cleaned), 100)  # Cap score at 100
    
    if isinstance(value, list):
        if len(value) == 0:
            return 0
        return min(len(value) * 5 + 30, 100)  # Score based on list size
    
    if isinstance(value, (int, float)):
        return 70  # Numeric values get solid score
    
    if isinstance(value, dict):
        return 60 if value else 0
    
    return 30


def create_consolidation_node(
    expected_fields: list[str] | None = None,
    schema_path: str | Path | None = None,
    metadata_path: str | Path | None = None,
) -> RunnableLambda:
    """Factory function to create a consolidation node.
    
    Args:
        expected_fields: List of expected field names (optional)
        schema_path: Path to schema_validation.json (optional, will try metadata_path)
        metadata_path: Path to metadata file (fallback for schema derivation)
        
    Returns:
        RunnableLambda that consolidates provider records into final result
    """
    
    def invoke_consolidation(state: GraphState) -> Dict[str, Any]:
        """Consolidate all provider records into final result."""
        records = state.get("records", {})
        consolidated: Dict[str, Any] = {}
        selected_by_field: Dict[str, Dict[str, Any]] = {}  # field -> {model, value, score}
        errors: Dict[str, str] = {}
        
        # ===== LOAD CONSOLIDATION RULES =====
        schema_file = Path(schema_path).expanduser() if schema_path else None
        metadata_file = Path(metadata_path).expanduser() if metadata_path else None
        
        rule_set = None
        try:
            if schema_file and schema_file.exists():
                rule_set = load_rules_from_schema_file(str(schema_file))
                print("[consolidation] Loaded rules from schema file")
            elif metadata_file and metadata_file.exists():
                rule_set = load_rules_from_metadata_file(str(metadata_file))
                print("[consolidation] Loaded rules from metadata file (fallback)")
            else:
                print("[consolidation] No schema or metadata file found - using fallback scoring")
                rule_set = None
        except Exception as e:
            print(f"[consolidation] Warning: Could not load schema rules: {e}. Using fallback scoring.")
            rule_set = None
        
        # ===== DISCOVER ALL FIELDS =====
        # Start with expected fields, then discover from records
        all_fields = set(expected_fields or [])
        for provider_name, record in records.items():
            # Track provider errors in consolidated output
            if record.get("_error"):
                errors[provider_name] = str(record.get("_error"))
            
            # Discover all data fields (exclude internal metadata fields starting with _)
            for field in record.keys():
                if not field.startswith("_"):
                    all_fields.add(field)
        
        # ===== SCORE AND CONSOLIDATE =====
        # For each field, pick the best value from all providers
        for field in sorted(all_fields):
            best_provider = None
            best_value = None
            best_score = -1
            score_details = {}  # Track scores from each provider for transparency
            
            # Score each provider's value for this field
            for provider_name, record in records.items():
                # Skip providers that had hard failures (auth, init errors) AND have no data
                provider_error = record.get("_error", "")
                has_hard_error = provider_error and "Provider chain" in str(provider_error)
                has_auth_error = provider_error and ("401" in str(provider_error) or "Unauthorized" in str(provider_error) or "Invalid API Key" in str(provider_error))
                
                if (has_hard_error or has_auth_error) and field not in record:
                    continue
                if field not in record:
                    continue
                
                value = record.get(field)
                
                # Score using rule set or fallback heuristic
                if rule_set:
                    score = rule_set.score_field(field, value)
                else:
                    score = fallback_value_score(value)
                
                score_details[provider_name] = {
                    "value": value,
                    "score": score,
                }
                
                # Update best if this score is higher
                if score > best_score:
                    best_score = score
                    best_provider = provider_name
                    best_value = value
            
            # Store consolidated value with attribution and all scores
            consolidated[field] = best_value
            selected_by_field[field] = {
                "provider": best_provider,
                "value": best_value,
                "score": best_score,
                "scores_by_provider": score_details,
            }
        
        # ===== BUILD RESULT =====
        result = {
            "consolidated": consolidated,
            "consolidated_field_count": len([v for v in consolidated.values() if v is not None]),
            "selected_by_field": selected_by_field,
            "provider_records": records,
            "errors": errors,
        }
        
        print(
            f"[consolidation] Consolidated {len(consolidated)} total fields, "
            f"{result['consolidated_field_count']} with values"
        )
        
        return {"result": result}
    
    return RunnableLambda(invoke_consolidation)
