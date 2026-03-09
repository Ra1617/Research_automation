import json

from services.validation_engine import MetadataRuleEngine, ParameterDefinition


def test_validation_engine_loads_metadata_with_nan(tmp_path) -> None:
    metadata = [
        {
            "sr_no": 1,
            "column_name": "Company Name",
            "data_type": "VARCHAR(255)",
            "minimum_element": 2,
            "maximum_element": 255,
            "regex_pattern": "^[A-Za-z ]+$",
            "nullability": "Not Null",
            "derivation_method": "NaN",
        },
        {
            "sr_no": 2,
            "column_name": "Year of Incorporation",
            "data_type": "INTEGER",
            "minimum_element": 4,
            "maximum_element": 4,
            "regex_pattern": "",
            "nullability": "Not Null",
            "derivation_method": "NaN",
        },
    ]
    metadata_text = json.dumps(metadata).replace('"NaN"', "NaN")
    metadata_file = tmp_path / "meta_data_complete.json"
    metadata_file.write_text(metadata_text, encoding="utf-8")

    engine = MetadataRuleEngine.from_metadata_file(metadata_file)

    assert engine.total_fields == 2
    assert "Company Name" in engine.field_names


def test_validation_engine_reports_failed_parameters() -> None:
    engine = MetadataRuleEngine(
        definitions=[
            ParameterDefinition(
                name="Company Name",
                description="Full legal name of the company",
                content_type="Legal Name",
                expected_type="string",
                min_len=2,
                max_len=255,
                regex_pattern="^[A-Za-z ]+$",
                nullable=False,
                category="Company Basics",
                business_rules="Must match official government registration",
                data_rules="Trim spaces, no emojis",
            ),
            ParameterDefinition(
                name="Year of Incorporation",
                description="Year company was founded",
                content_type="Year",
                expected_type="integer",
                min_len=None,
                max_len=None,
                regex_pattern=None,
                nullable=False,
                category="Company Basics",
                business_rules="Must be after 1800",
                data_rules="Four digit year",
            ),
        ]
    )

    report = engine.validate_record(
        {
            "company name": "OpenAI",
            "Year of Incorporation": "2015",
        }
    )

    assert report.passed_count == 1
    assert report.failed_count == 1
    assert "Wrong type for" in report.failed_reasons["Year of Incorporation"]
    assert "expected integer" in report.failed_reasons["Year of Incorporation"]


def test_retry_feedback_contains_failed_fields() -> None:
    engine = MetadataRuleEngine(
        definitions=[
            ParameterDefinition(
                name="Field A",
                description="Sample field A for testing",
                content_type="General",
                expected_type="string",
                min_len=1,
                max_len=10,
                regex_pattern=None,
                nullable=False,
                category="Test Fields",
                business_rules="Test rule A",
                data_rules="No special chars",
            ),
            ParameterDefinition(
                name="Field B",
                description="Sample field B for testing",
                content_type="General",
                expected_type="string",
                min_len=1,
                max_len=10,
                regex_pattern=None,
                nullable=False,
                category="Test Fields",
                business_rules="Test rule B",
                data_rules="No special chars",
            ),
        ]
    )

    report = engine.validate_record({"Field A": "ok"})
    feedback = engine.build_retry_feedback(report)

    assert "Field B" in feedback
    assert "Missing:" in feedback or "VALIDATION FAILURES" in feedback
