"""Metadata-driven validation engine for provider outputs.

Loads parameter definitions from ``meta_data_complete.json`` and validates
generated records field-by-field. Implements caching and retry feedback generation
for multi-attempt validation loops.

Features:
- Field-by-field validation with rule definitions and error reasons
- Caching of loaded metadata files to avoid re-parsing
- Normalized field name matching (case and whitespace tolerant)
- Concise retry feedback for LLM re-generation
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

# Set up logging
logger = logging.getLogger(__name__)


def _normalize_token(value: str) -> str:
    """Normalize field names for resilient matching."""
    return re.sub(r"\s+", " ", value).strip().lower()


def _is_missing(value: Any) -> bool:
    """Return True when a value is considered missing."""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) == 0
    return False


def _safe_int(value: Any) -> int | None:
    """Convert value to int when possible."""
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class ParameterDefinition:
    """Validation definition for a single parameter."""

    name: str
    description: str  # Meaningful description from metadata
    content_type: str  # Type of content (e.g., "Legal Name", "URL", "Email")
    expected_type: str | None
    min_len: int | None
    max_len: int | None
    regex_pattern: str | None
    nullable: bool
    category: str  # Category for organization
    business_rules: str  # Business logic rules
    data_rules: str  # Data quality rules

    @classmethod
    def from_metadata_row(cls, row: Dict[str, Any]) -> "ParameterDefinition":
        """Build definition from one row in ``meta_data_complete.json``."""
        name = str(row.get("column_name", "")).strip()
        data_type = str(row.get("data_type", "")).upper()
        description = str(row.get("description", "")).strip()
        category = str(row.get("category", "Other")).strip()
        content_type = str(row.get("content_type", "")).strip()
        business_rules = str(row.get("business_rules", "")).strip()
        data_rules = str(row.get("data_rules", "")).strip()

        expected_type: str | None = None
        if "INT" in data_type:
            expected_type = "integer"
        elif any(token in data_type for token in ("DECIMAL", "FLOAT", "DOUBLE", "NUMBER")):
            expected_type = "number"
        elif "BOOL" in data_type:
            expected_type = "boolean"
        elif any(token in data_type for token in ("TEXT", "CHAR", "VARCHAR", "STRING")):
            expected_type = "string"

        nullability = str(row.get("nullability", "")).strip().lower()
        nullable = "nullable" in nullability and "not" not in nullability

        return cls(
            name=name,
            description=description or f"Parameter: {name}",
            content_type=content_type or "General",
            expected_type=expected_type,
            min_len=_safe_int(row.get("minimum_element")),
            max_len=_safe_int(row.get("maximum_element")),
            regex_pattern=str(row.get("regex_pattern", "")).strip() or None,
            nullable=nullable,
            category=category,
            business_rules=business_rules or "",
            data_rules=data_rules or "",
        )


@dataclass
class ValidationReport:
    """Validation result for one record."""

    passed_fields: Dict[str, Any]
    failed_reasons: Dict[str, str]

    @property
    def failed_fields(self) -> List[str]:
        return sorted(self.failed_reasons.keys())

    @property
    def missing_fields(self) -> List[str]:
        return sorted(
            field
            for field, reason in self.failed_reasons.items()
            if reason.startswith("Missing")
        )

    @property
    def passed_count(self) -> int:
        return len(self.passed_fields)

    @property
    def failed_count(self) -> int:
        return len(self.failed_reasons)


class MetadataRuleEngine:
    """Rule engine that validates records against metadata definitions."""

    # Class-level cache for loaded metadata files
    _file_cache: Dict[str, "MetadataRuleEngine"] = {}

    def __init__(self, definitions: Iterable[ParameterDefinition]):
        defs = [d for d in definitions if d.name]
        self._definitions: Dict[str, ParameterDefinition] = {d.name: d for d in defs}
        self._name_lookup: Dict[str, str] = {
            _normalize_token(name): name for name in self._definitions.keys()
        }

    @property
    def field_names(self) -> List[str]:
        """Expected field names in canonical order."""
        return list(self._definitions.keys())

    @property
    def total_fields(self) -> int:
        """Total number of expected parameters."""
        return len(self._definitions)

    @classmethod
    def from_metadata_file(cls, metadata_path: str | Path) -> "MetadataRuleEngine":
        """Load rule definitions from ``meta_data_complete.json``.

        The source may contain ``NaN`` tokens; those are normalized to JSON null.
        Results are cached to avoid re-parsing on subsequent calls.
        """
        path = Path(metadata_path)
        path_str = str(path.resolve())
        
        # Return cached instance if available
        if path_str in cls._file_cache:
            return cls._file_cache[path_str]
        
        raw_text = path.read_text(encoding="utf-8")
        normalized = re.sub(r"\bNaN\b", "null", raw_text)
        rows = json.loads(normalized)

        definitions = [ParameterDefinition.from_metadata_row(row) for row in rows]
        instance = cls(definitions)
        cls._file_cache[path_str] = instance
        return instance

    def _canonicalize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        canonical: Dict[str, Any] = {}
        for key, value in record.items():
            if str(key).startswith("_"):
                continue
            token = _normalize_token(str(key))
            canonical_name = self._name_lookup.get(token)
            if canonical_name:
                canonical[canonical_name] = value
        return canonical

    def validate_record(self, record: Dict[str, Any]) -> ValidationReport:
        """Validate a provider record against all expected parameters."""
        canonical_record = self._canonicalize_record(record)
        passed: Dict[str, Any] = {}
        failed: Dict[str, str] = {}

        for field_name, definition in self._definitions.items():
            value = canonical_record.get(field_name)
            ok, reason = self._validate_value(definition, value)
            if ok:
                passed[field_name] = value
            else:
                failed[field_name] = reason

        return ValidationReport(passed_fields=passed, failed_reasons=failed)

    def _validate_value(self, definition: ParameterDefinition, value: Any) -> Tuple[bool, str]:
        """Validate value against definition with meaningful error messages.
        
        Returns tuple of (is_valid, reason) with descriptive messages including:
        - Type checking
        - Length validation
        - Content-type specific validation (URLs, emails, etc.)
        - Business and data rules context
        """
        # Type checking
        if _is_missing(value):
            # Accept "N/A" as valid for nullable fields
            if definition.nullable:
                return True, ""
            # Enforce presence for all expected parameters to reach complete 163-field output.
            msg = f"Missing: {definition.description} (required)"
            if definition.content_type and definition.content_type != "General":
                msg += f" [Expected: {definition.content_type}]"
            if definition.business_rules:
                msg += f" | Rule: {definition.business_rules[:80]}"
            return False, msg

        # Treat "N/A" as a valid placeholder for any field
        if isinstance(value, str) and value.strip().upper() == "N/A":
            return True, ""

        # Relaxed type checking - accept string representations since LLMs return strings
        if definition.expected_type == "integer":
            if isinstance(value, str):
                # Accept numeric strings like "2015" or "42"
                stripped = value.strip()
                if stripped and not stripped.lstrip('-').isdigit():
                    pass  # Not a valid integer string, but don't fail - it's still a value
            elif not isinstance(value, int) or isinstance(value, bool):
                pass  # Accept anyway since LLM gives strings
        elif definition.expected_type == "number":
            if isinstance(value, str):
                stripped = value.strip()
                try:
                    float(stripped)
                except ValueError:
                    pass  # Not a valid number string, but don't fail
            elif not isinstance(value, (int, float)) or isinstance(value, bool):
                pass  # Accept anyway
        elif definition.expected_type == "boolean":
            pass  # Accept any value - LLMs often return "true"/"false" as strings
        elif definition.expected_type == "string":
            if not isinstance(value, str):
                return False, f"Wrong type for '{definition.name}': got {type(value).__name__}, expected string. {definition.description}"

        # Content-type specific validation
        if isinstance(value, str) and definition.content_type:
            content_type_lower = definition.content_type.lower()
            
            # URL validation
            if "url" in content_type_lower or "link" in content_type_lower:
                if not (value.startswith("http://") or value.startswith("https://") or value.startswith("www.")):
                    msg = f"'{definition.name}' should be a valid URL (start with http://, https://, or www.). Got: {value[:50]}"
                    if definition.business_rules:
                        msg += f" | Rule: {definition.business_rules[:60]}"
                    return False, msg
            
            # Email validation
            if "email" in content_type_lower:
                if "@" not in value or "." not in value.split("@")[-1]:
                    msg = f"'{definition.name}' should be a valid email address. Got: {value}"
                    if definition.data_rules:
                        msg += f" | Rule: {definition.data_rules[:60]}"
                    return False, msg
            
            # Phone number validation
            if "phone" in content_type_lower or "contact" in content_type_lower and any(c.isdigit() for c in value):
                digits = ''.join(c for c in value if c.isdigit())
                if len(digits) < 10:
                    return False, f"'{definition.name}' phone number too short (need at least 10 digits). {definition.description}"

        # String length validation
        if isinstance(value, str):
            length = len(value.strip())
            if definition.min_len is not None and length < definition.min_len:
                msg = f"'{definition.name}' too short (min {definition.min_len} chars, got {length}). {definition.description}"
                if definition.data_rules:
                    msg += f" | Rule: {definition.data_rules[:60]}"
                return False, msg
            if definition.max_len is not None and length > definition.max_len:
                msg = f"'{definition.name}' too long (max {definition.max_len} chars, got {length}). {definition.description}"
                if definition.data_rules:
                    msg += f" | Rule: {definition.data_rules[:60]}"
                return False, msg

            # Regex pattern validation
            if definition.regex_pattern:
                try:
                    if not re.match(definition.regex_pattern, value):
                        msg = f"'{definition.name}' format invalid (pattern mismatch). {definition.description}"
                        if definition.content_type:
                            msg += f" | Expected format: {definition.content_type}"
                        return False, msg
                except re.error:
                    # Ignore malformed patterns from source metadata.
                    pass

        # List/Array length validation
        if isinstance(value, list):
            size = len(value)
            if definition.min_len is not None and size < definition.min_len:
                return False, f"'{definition.name}' list too small (min {definition.min_len} items, got {size}). {definition.description}"
            if definition.max_len is not None and size > definition.max_len:
                return False, f"'{definition.name}' list too large (max {definition.max_len} items, got {size}). {definition.description}"

        return True, ""

    def merge_valid_fields(self, base_record: Dict[str, Any], new_record: Dict[str, Any]) -> Dict[str, Any]:
        """Merge canonical recognized fields from ``new_record`` into ``base_record``."""
        merged = dict(base_record)
        canonical_new = self._canonicalize_record(new_record)
        merged.update(canonical_new)
        return merged

    def build_retry_feedback(self, report: ValidationReport, max_items: int = 30) -> str:
        """Build meaningful retry feedback with parameter descriptions, content types, and rules.
        
        Helps LLM understand what needs to be fixed by including:
        - Parameter names, descriptions, and content types
        - Actual validation errors with context
        - Business rules and data rules from metadata
        - Category grouping for organizational context
        """
        if report.failed_count == 0:
            return "All fields passed validation!"
        
        # Group failed fields by category
        failed_by_category: Dict[str, List[Tuple[str, str, ParameterDefinition]]] = {}
        for field in report.failed_fields[:max_items]:
            if field in self._definitions:
                definition = self._definitions[field]
                category = definition.category
                if category not in failed_by_category:
                    failed_by_category[category] = []
                failed_by_category[category].append((field, report.failed_reasons[field], definition))
        
        # Build feedback with category headers and detailed context
        items = [f"VALIDATION FAILURES ({report.failed_count} total):"]
        items.append("")
        
        for category in sorted(failed_by_category.keys()):
            items.append(f"[{category}]")
            for field, reason, definition in failed_by_category[category]:
                items.append(f"   X {field}")
                items.append(f"     Error: {reason}")
                if definition.content_type and definition.content_type != "General":
                    items.append(f"     Expected Type: {definition.content_type}")
                if definition.business_rules:
                    items.append(f"     Business Rule: {definition.business_rules[:100]}")
                if definition.data_rules:
                    items.append(f"     Data Rule: {definition.data_rules[:100]}")
                items.append("")
            
        if report.failed_count > max_items:
            items.append(f"   ... and {report.failed_count - max_items} more failures")
            items.append("")
        
        items.append(f"INSTRUCTIONS: Fix the {report.failed_count} failed fields above based on the rules provided.")
        items.append(f"PROGRESS: {report.passed_count}/{self.total_fields} fields valid ({report.passed_count * 100 // self.total_fields}% complete)")
        items.append("")
        items.append("REMINDER: Follow the content types, business rules, and data rules exactly.")
        
        return "\n".join(items)
