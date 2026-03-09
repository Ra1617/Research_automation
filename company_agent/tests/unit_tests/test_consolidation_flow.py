import json

from main import consolidate


def test_consolidate_uses_metadata_when_schema_missing(tmp_path) -> None:
    metadata_rows = [
        {
            "column_name": "Company Name",
            "description": "Official name",
            "content_type": "Legal Name",
            "data_type": "VARCHAR(255)",
            "minimum_element": 2,
            "maximum_element": 255,
            "regex_pattern": "^[A-Za-z ]+$",
            "nullability": "Not Null",
        },
        {
            "column_name": "Employee Count",
            "description": "Number of employees",
            "content_type": "Count",
            "data_type": "INTEGER",
            "minimum_element": "",
            "maximum_element": "",
            "regex_pattern": "",
            "nullability": "Nullable",
        },
    ]

    metadata_file = tmp_path / "meta_data_complete.json"
    metadata_file.write_text(json.dumps(metadata_rows), encoding="utf-8")

    records = {
        "gemini": {"Company Name": "OpenAI", "Employee Count": 1200},
        "openrouter": {"Company Name": "OpenAI", "Employee Count": 0},
    }

    result = consolidate(
        records=records,
        startup_errors=None,
        expected_fields=["Company Name", "Employee Count"],
        schema_path=tmp_path / "schema_validation.json",  # intentionally missing
        metadata_path=metadata_file,
    )

    assert "consolidated" in result
    assert result["consolidated"]["Company Name"] == "OpenAI"
    assert "selected_by_field" in result
    assert "Company Name" in result["selected_by_field"]


def test_consolidate_includes_expected_fields_when_missing(tmp_path) -> None:
    metadata_rows = [
        {
            "column_name": "Field A",
            "description": "a",
            "content_type": "a",
            "data_type": "VARCHAR(20)",
            "minimum_element": 1,
            "maximum_element": 20,
            "regex_pattern": "",
            "nullability": "Not Null",
        },
        {
            "column_name": "Field B",
            "description": "b",
            "content_type": "b",
            "data_type": "VARCHAR(20)",
            "minimum_element": 1,
            "maximum_element": 20,
            "regex_pattern": "",
            "nullability": "Not Null",
        },
    ]

    metadata_file = tmp_path / "meta_data_complete.json"
    metadata_file.write_text(json.dumps(metadata_rows), encoding="utf-8")

    records = {
        "gemini": {"Field A": "value-a"},
        "openrouter": {"Field A": "value-a-2"},
    }

    result = consolidate(
        records=records,
        startup_errors=None,
        expected_fields=["Field A", "Field B"],
        schema_path=tmp_path / "schema_validation.json",  # intentionally missing
        metadata_path=metadata_file,
    )

    assert "Field A" in result["consolidated"]
    assert "Field B" in result["consolidated"]
    assert result["consolidated"]["Field B"] is None
