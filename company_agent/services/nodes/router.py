"""Router node for conditional retry logic.

The retry router makes a binary decision:
- If ANY provider has _retry_pending=True → route back to GENERATION nodes
- Otherwise → route to CONSOLIDATION
"""

from __future__ import annotations

from services.state import GraphState


def should_retry(state: GraphState) -> str:
    """Determine if any provider should retry.
    
    Returns:
        "generate_gemini" if retries needed (routes back to generation)
        "consolidate" if no retries needed (final consolidation)
    """
    records = state.get("records", {})
    
    # Check if ANY provider has retry pending
    for provider_name, record in records.items():
        if record.get("_retry_pending") is True:
            print(f"[router] Routing back to generation (retry needed for {provider_name})")
            return "retry_generation"  # Triggers parallel generation nodes
    
    # No retries needed - move to consolidation
    print("[router] No retries pending - routing to consolidation")
    return "consolidate"
