"""Parameter-specific consolidation rules and scoring system.

Each parameter has custom rules that define how to score LLM responses.
Higher scores indicate better data quality and rule compliance.
"""

from typing import Any, Dict, Optional, List
import re
import json


class ParameterRule:
    """Represents consolidation rules for a single parameter."""
    
    def __init__(self, param_name: str, schema_def: Dict[str, Any]):
        """Initialize rule from schema definition.
        
        Args:
            param_name: Parameter name
            schema_def: Parameter definition from schema
        """
        self.param_name = param_name
        self.schema = schema_def
        self.title = schema_def.get("title", param_name)
        self.description = schema_def.get("description", "")
        self.param_type = schema_def.get("type")
        self.required = False
        
    def score(self, value: Any) -> int:
        """Score a value based on rule compliance.
        
        Returns score 0-100 where higher is better compliance.
        
        Args:
            value: The value to score
            
        Returns:
            Score from 0-100
        """
        if value is None or value == "":
            return 0
        
        score = 50  # Base score for having a value
        
        # Type validation
        if self._validate_type(value):
            score += 10
        else:
            return 0  # Fail if wrong type
        
        # Format validation
        if self._validate_format(value):
            score += 10
        
        # Pattern/Regex validation
        if self._validate_pattern(value):
            score += 10
        
        # Enum validation
        if self._validate_enum(value):
            score += 10
        
        # Length validation
        length_score = self._validate_length(value)
        score += length_score
        
        # Value range validation (for numbers)
        if self._validate_range(value):
            score += 5
        
        # Content quality
        quality_score = self._evaluate_content_quality(value)
        score += quality_score
        
        return min(score, 100)  # Cap at 100
    
    def _validate_type(self, value: Any) -> bool:
        """Check if value matches expected type."""
        param_type = self.schema.get("type")
        
        if param_type is None:
            return True
        
        # Handle unions like ["string", "null"]
        if isinstance(param_type, list):
            return any(self._check_single_type(value, t) for t in param_type)
        
        return self._check_single_type(value, param_type)
    
    def _check_single_type(self, value: Any, expected_type: str) -> bool:
        """Check value against single type."""
        type_map = {
            "string": str,
            "integer": int,
            "number": (int, float),
            "boolean": bool,
            "array": list,
            "object": dict,
            "null": type(None),
        }
        
        if expected_type in type_map:
            return isinstance(value, type_map[expected_type])
        
        return True
    
    def _validate_format(self, value: Any) -> bool:
        """Check if value matches format requirement."""
        fmt = self.schema.get("format")
        
        if not fmt or not isinstance(value, str):
            return True
        
        format_validators = {
            "uri": self._is_valid_uri,
            "email": self._is_valid_email,
            "date": self._is_valid_date,
            "uuid": self._is_valid_uuid,
        }
        
        if fmt in format_validators:
            return format_validators[fmt](value)
        
        return True
    
    def _is_valid_uri(self, value: str) -> bool:
        """Validate URI format."""
        return value.startswith(("http://", "https://"))
    
    def _is_valid_email(self, value: str) -> bool:
        """Validate email format."""
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        return bool(re.match(pattern, value))
    
    def _is_valid_date(self, value: str) -> bool:
        """Validate ISO date format."""
        pattern = r"^\d{4}-\d{2}-\d{2}"
        return bool(re.match(pattern, value))
    
    def _is_valid_uuid(self, value: str) -> bool:
        """Validate UUID format."""
        pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        return bool(re.match(pattern, value, re.IGNORECASE))
    
    def _validate_pattern(self, value: Any) -> bool:
        """Check if value matches regex pattern."""
        if not isinstance(value, str):
            return True
        
        pattern = self.schema.get("pattern")
        if not pattern:
            return True
        
        try:
            return bool(re.match(pattern, value))
        except re.error:
            return False
    
    def _validate_enum(self, value: Any) -> bool:
        """Check if value is in allowed enum values."""
        enum_values = self.schema.get("enum")
        if not enum_values:
            return True
        
        return value in enum_values
    
    def _validate_length(self, value: Any) -> int:
        """Score based on length constraints.
        
        Returns 0-10 points based on how well length matches constraints.
        """
        if isinstance(value, str):
            min_len = self.schema.get("minLength", 0)
            max_len = self.schema.get("maxLength", 10000)
            length = len(value)
            
            if length < min_len:
                return 0  # Too short
            if length > max_len:
                return 0  # Too long
            
            # Score based on content density
            if length >= min_len and length <= max_len:
                # Prefer values closer to intended use
                if min_len > 0:
                    utilization = length / max_len if max_len else 1
                    return int(10 * utilization)
                return 5  # Minimum score for valid length
        
        elif isinstance(value, list):
            min_items = self.schema.get("minItems", 0)
            max_items = self.schema.get("maxItems", 10000)
            count = len(value)
            
            if count < min_items:
                return 0
            if count > max_items:
                return 0
            
            return int(10 * (count / max(max_items, 1)))
        
        return 5  # Partial credit for having a value
    
    def _validate_range(self, value: Any) -> bool:
        """Check if numeric value is in range."""
        if not isinstance(value, (int, float)):
            return True
        
        minimum = self.schema.get("minimum")
        maximum = self.schema.get("maximum")
        
        if minimum is not None and value < minimum:
            return False
        if maximum is not None and value > maximum:
            return False
        
        return True
    
    def _evaluate_content_quality(self, value: Any) -> int:
        """Evaluate content quality beyond schema compliance.
        
        Returns 0-10 bonus points for high-quality content.
        """
        if not isinstance(value, str):
            return 0
        
        value = value.strip()
        
        # Penalize placeholder values
        if value.lower() in {"unknown", "n/a", "na", "none", "tbd", "todo"}:
            return -20
        
        # Bonus for detailed content
        if len(value) > 100:
            return 5
        
        # Bonus for properly formatted content
        if any(c.isupper() for c in value) and len(value) > 20:
            return 3
        
        return 0


class ConsolidationRuleSet:
    """Manages all parameter rules."""
    
    def __init__(self, schema: Dict[str, Any]):
        """Initialize rule set from schema.
        
        Args:
            schema: JSON Schema containing all parameter definitions
        """
        self.schema = schema
        self.rules: Dict[str, ParameterRule] = {}
        self.required_fields = set(schema.get("required", []))
        
        # Build rules for each property
        properties = schema.get("properties", {})
        for param_name, param_def in properties.items():
            rule = ParameterRule(param_name, param_def)
            rule.required = param_name in self.required_fields
            self.rules[param_name] = rule
    
    def get_rule(self, param_name: str) -> Optional[ParameterRule]:
        """Get rule for a parameter.
        
        Args:
            param_name: Parameter name
            
        Returns:
            ParameterRule if found, None otherwise
        """
        return self.rules.get(param_name)
    
    def score_field(self, param_name: str, value: Any) -> int:
        """Score a field value.
        
        Args:
            param_name: Parameter name
            value: Value to score
            
        Returns:
            Score 0-100
        """
        rule = self.get_rule(param_name)
        if not rule:
            # Unknown field - generic scoring
            return self._generic_score(value)
        
        return rule.score(value)
    
    def _generic_score(self, value: Any) -> int:
        """Generic scoring for unknown fields."""
        if value is None or value == "":
            return 0
        
        if isinstance(value, str):
            length = len(value.strip())
            if length == 0:
                return 0
            if length < 5:
                return 20
            if length < 50:
                return 50
            if length < 500:
                return 75
            return 90
        
        if isinstance(value, (int, float)):
            return 60
        
        if isinstance(value, list):
            if len(value) == 0:
                return 0
            return 40 + min(len(value) * 2, 50)
        
        if isinstance(value, dict):
            return 50 if value else 0
        
        return 30


def load_rules_from_schema_file(schema_path: str) -> ConsolidationRuleSet:
    """Load rules from schema JSON file.
    
    Args:
        schema_path: Path to schema JSON file
        
    Returns:
        ConsolidationRuleSet instance
    """
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema = json.load(f)
    
    return ConsolidationRuleSet(schema)


def load_rules_from_metadata_file(metadata_path: str) -> ConsolidationRuleSet:
    """Build rules from ``meta_data_complete.json`` style metadata.

    Converts tabular metadata rows into a JSON-Schema-like structure that can be
    consumed by ``ConsolidationRuleSet``.
    """
    with open(metadata_path, "r", encoding="utf-8") as f:
        raw_text = f.read()

    normalized = re.sub(r"\bNaN\b", "null", raw_text)
    rows: List[Dict[str, Any]] = json.loads(normalized)

    schema: Dict[str, Any] = {
        "type": "object",
        "properties": {},
        "required": [],
    }

    for row in rows:
        column_name = str(row.get("column_name", "")).strip()
        if not column_name:
            continue

        data_type_raw = str(row.get("data_type", "")).upper()
        nullability = str(row.get("nullability", "")).strip().lower()
        minimum_element = row.get("minimum_element")
        maximum_element = row.get("maximum_element")
        regex_pattern = str(row.get("regex_pattern", "")).strip()

        prop: Dict[str, Any] = {
            "title": str(row.get("content_type", "")).strip() or column_name,
            "description": str(row.get("description", "")).strip(),
        }

        if "INT" in data_type_raw:
            prop["type"] = "integer"
        elif any(token in data_type_raw for token in ("DECIMAL", "FLOAT", "DOUBLE", "NUMBER")):
            prop["type"] = "number"
        elif "BOOL" in data_type_raw:
            prop["type"] = "boolean"
        elif any(token in data_type_raw for token in ("TEXT", "CHAR", "VARCHAR", "STRING")):
            prop["type"] = "string"

        try:
            min_value = int(minimum_element) if minimum_element not in (None, "") else None
        except (TypeError, ValueError):
            min_value = None

        try:
            max_value = int(maximum_element) if maximum_element not in (None, "") else None
        except (TypeError, ValueError):
            max_value = None

        if prop.get("type") == "string":
            if min_value is not None:
                prop["minLength"] = max(0, min_value)
            if max_value is not None:
                prop["maxLength"] = max(0, max_value)
            if regex_pattern:
                prop["pattern"] = regex_pattern

        schema["properties"][column_name] = prop

        if "nullable" not in nullability or "not" in nullability:
            schema["required"].append(column_name)

    return ConsolidationRuleSet(schema)
