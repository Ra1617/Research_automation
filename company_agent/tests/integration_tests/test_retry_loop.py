"""Integration tests for the retry loop and consolidation flow.

Tests the full validation → retry → consolidation pipeline with controlled scenarios.
"""
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from services.validation_engine import MetadataRuleEngine


@pytest.fixture
def mock_metadata_file(tmp_path):
    """Create a minimal metadata file for testing."""
    metadata = [
        {
            "sr_no": 1,
            "column_name": "Company Name",
            "description": "Official company name",
            "content_type": "Legal Name",
            "data_type": "VARCHAR(255)",
            "minimum_element": 2,
            "maximum_element": 255,
            "regex_pattern": "^[A-Za-z0-9 ]+$",
            "nullability": "Not Null",
            "category": "Company Basics",
            "granularity": "One per Entity",
            "data_owner": "Legal",
            "confidence_level": "High",
            "criticality": "Critical",
            "data_volatility": "Static",
            "update_frequency": "Ad-hoc",
            "format_constraints": "Standard Case",
            "business_rules": "Must match official documents",
            "data_rules": "Trim spaces",
            "data_source": "Company Registry",
            "validation_mode": "Automated",
            "test_cases": "Check length > 1",
            "is_dervied_from": "No",
            "derivation_method": None,
        },
        {
            "sr_no": 2,
            "column_name": "Year Founded",
            "description": "Year company was founded",
            "content_type": "Year",
            "data_type": "INTEGER",
            "minimum_element": 4,
            "maximum_element": 4,
            "regex_pattern": "",
            "nullability": "Not Null",
            "category": "Company Basics",
            "granularity": "One per Entity",
            "data_owner": "Legal",
            "confidence_level": "High",
            "criticality": "High",
            "data_volatility": "Static",
            "update_frequency": "Never",
            "format_constraints": "YYYY",
            "business_rules": "Must be valid year",
            "data_rules": "Between 1800 and current year",
            "data_source": "Company Registry",
            "validation_mode": "Automated",
            "test_cases": "Check year range",
            "is_dervied_from": "No",
            "derivation_method": None,
        },
        {
            "sr_no": 3,
            "column_name": "Employee Count",
            "description": "Number of employees",
            "content_type": "Count",
            "data_type": "INTEGER",
            "minimum_element": "",
            "maximum_element": "",
            "regex_pattern": "",
            "nullability": "Nullable",
            "category": "Company Basics",
            "granularity": "One per Entity",
            "data_owner": "HR",
            "confidence_level": "Medium",
            "criticality": "Medium",
            "data_volatility": "Dynamic",
            "update_frequency": "Quarterly",
            "format_constraints": "Integer",
            "business_rules": "Must be positive",
            "data_rules": "Greater than zero",
            "data_source": "HR System",
            "validation_mode": "Automated",
            "test_cases": "Check positive integer",
            "is_dervied_from": "No",
            "derivation_method": None,
        },
    ]
    
    metadata_file = tmp_path / "meta_data_complete.json"
    metadata_file.write_text(json.dumps(metadata), encoding="utf-8")
    return metadata_file


def test_validation_engine_identifies_missing_fields(mock_metadata_file):
    """Test that validation correctly identifies missing and invalid fields."""
    engine = MetadataRuleEngine.from_metadata_file(mock_metadata_file)
    
    # Record with one valid, one invalid, one missing field
    record = {
        "Company Name": "OpenAI",
        "Year Founded": "2015",  # Wrong type (string instead of int)
        # "Employee Count" is missing
    }
    
    report = engine.validate_record(record)
    
    assert report.passed_count == 1
    assert "Company Name" in report.passed_fields
    assert "Year Founded" in report.failed_fields
    assert "Employee Count" in report.failed_fields
    assert report.failed_count == 2


def test_validation_engine_builds_retry_feedback(mock_metadata_file):
    """Test that retry feedback contains actionable information for LLM."""
    engine = MetadataRuleEngine.from_metadata_file(mock_metadata_file)
    
    record = {
        "Company Name": "X",  # Too short
        "Year Founded": 2015,
    }
    
    report = engine.validate_record(record)
    feedback = engine.build_retry_feedback(report)
    
    assert "Company Name" in feedback
    assert "Employee Count" in feedback
    assert "Missing" in feedback or "below minimum" in feedback.lower()


def test_consolidation_selects_best_value_per_field(tmp_path, mock_metadata_file):
    """Test that consolidation picks the best value for each field from multiple providers."""
    from main import consolidate
    
    records = {
        "gemini": {
            "Company Name": "OpenAI",
            "Year Founded": 2015,
            "Employee Count": 1200,
        },
        "groq": {
            "Company Name": "OpenAI Inc",  # Longer/more detailed
            "Year Founded": 2015,
            "Employee Count": 0,  # Invalid/missing
        },
        "openrouter": {
            "_error": "API timeout",  # Provider error
        },
    }
    
    result = consolidate(
        records=records,
        startup_errors=None,
        expected_fields=["Company Name", "Year Founded", "Employee Count"],
        schema_path=None,
        metadata_path=mock_metadata_file,
    )
    
    assert "consolidated" in result
    assert result["consolidated"]["Company Name"] in ["OpenAI", "OpenAI Inc"]
    assert result["consolidated"]["Year Founded"] == 2015
    assert "selected_by_field" in result
    assert "errors" in result
    assert "openrouter" in result["errors"]


def test_retry_feedback_targets_only_failed_fields(mock_metadata_file):
    """Test that retry feedback only includes failed fields, not all 163 parameters."""
    engine = MetadataRuleEngine.from_metadata_file(mock_metadata_file)
    
    # Simulate first attempt: 1 passed, 2 failed
    record = {
        "Company Name": "OpenAI",  # Valid
        # Missing: Year Founded, Employee Count
    }
    
    report = engine.validate_record(record)
    feedback = engine.build_retry_feedback(report)
    
    # Retry feedback should NOT mention the passed field
    assert "Company Name" not in feedback
    assert "Year Founded" in feedback
    assert "Employee Count" in feedback
    
    # Should only target failed fields
    assert report.failed_count == 2
    failed_fields_list = report.failed_fields
    assert len(failed_fields_list) == 2
    assert "Year Founded" in failed_fields_list
    assert "Employee Count" in failed_fields_list


def test_merge_valid_fields_preserves_passed_parameters(mock_metadata_file):
    """Test that validation merging preserves previously validated fields across retries."""
    engine = MetadataRuleEngine.from_metadata_file(mock_metadata_file)
    
    # First attempt: partial success
    base_record = {
        "Company Name": "OpenAI",
        "Year Founded": 2015,
    }
    
    # Second attempt: adds missing field
    new_record = {
        "Employee Count": 1200,
    }
    
    merged = engine.merge_valid_fields(base_record, new_record)
    
    assert merged["Company Name"] == "OpenAI"
    assert merged["Year Founded"] == 2015
    assert merged["Employee Count"] == 1200
    assert len(merged) == 3
