import json


def load_params(path: str = "schema/parameters.json") -> list:
    with open(path) as f:
        return json.load(f)


def validate_result(result: dict, all_params: list) -> dict:
    """
    Check the consolidated result against schema rules.
    Returns a validation report.
    """
    errors = []
    warnings = []
    required_keys = {p["key"] for p in all_params if p["nullability"] == "Not Null"}

    for param in all_params:
        key = param["key"]
        entry = result.get(key)

        # Missing key entirely
        if entry is None:
            errors.append(f"MISSING KEY: '{key}' not in result")
            continue

        value = entry.get("value")
        score = entry.get("score", 0)
        confidence = "high" if score >= 80 else "low" if score > 0 else "none"

        # Required field is null
        if key in required_keys and value is None:
            errors.append(f"NULL REQUIRED: '{key}' is required (Not Null) but has no value")

        # Low confidence on required fields
        if key in required_keys and confidence == "low":
            warnings.append(f"LOW CONFIDENCE on required field: '{key}' = {value}")

        # No data found at all for required field
        if key in required_keys and confidence == "none":
            errors.append(f"NO DATA: Required field '{key}' returned null from all models")

    report = {
        "total_params": len(all_params),
        "valid": len(errors) == 0,
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings
    }
    return report
