import json
import os
from datetime import datetime


def save_audit_log(company_name: str, result: dict, raw_outputs: list):
    """Save full audit trail: raw LLM outputs + consensus decisions."""
    os.makedirs("output/audit", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = company_name.replace(" ", "_").replace("/", "-")
    
    audit = {
        "company": company_name,
        "timestamp": ts,
        "total_params": len(result),
        "high_confidence": sum(1 for v in result.values() if v.get("score", 0) >= 80),
        "low_confidence": sum(1 for v in result.values() if 0 < v.get("score", 0) < 80),
        "no_data": sum(1 for v in result.values() if v.get("score", 0) == 0),
        "raw_chunks": raw_outputs,
        "final_result": result
    }
    
    path = f"output/audit/{safe_name}_{ts}.json"
    with open(path, "w") as f:
        json.dump(audit, f, indent=2, default=str)
    
    print(f"   Audit log saved: {path}")
    return path
