from collections import Counter
from utils.json_utils import safe_parse


def consolidate(outputs: dict, param_chunk: list) -> dict:
    """
    For each parameter, pick value by majority vote across 3 models.
    If 2+ models agree → confidence: high
    If all differ     → confidence: low  (first non-null wins)
    If all null       → confidence: none
    
    Returns: { "param_key": {"value": ..., "confidence": "high"|"low"|"none", "votes": {...}} }
    """
    parsed = {model: safe_parse(raw) for model, raw in outputs.items()}
    keys = [p["key"] for p in param_chunk]
    result = {}

    for key in keys:
        model_values = {}
        for model_name, data in parsed.items():
            val = data.get(key)
            if val is not None and str(val).strip().lower() not in ("null", "none", "", "nan"):
                model_values[model_name] = val

        if not model_values:
            result[key] = {"value": None, "confidence": "none", "votes": {}}
            continue

        # Normalize values to strings for comparison
        str_vals = [str(v).strip() for v in model_values.values()]
        count = Counter(str_vals)
        top_val_str, top_count = count.most_common(1)[0]

        # Get actual typed value for the winning string
        winning_value = next(
            v for v in model_values.values() if str(v).strip() == top_val_str
        )

        confidence = "high" if top_count >= 2 else "low"
        result[key] = {
            "value": winning_value,
            "confidence": confidence,
            "votes": model_values
        }

    return result
