import pandas as pd

from app.services.detector import DetectorService
from app.services.mapper import MapperService
from app.services.validator import ValidatorService


def test_detector_identifies_email_column():
    df = pd.DataFrame(
        {
            "email": [
                "josh@example.com",
                "jane@example.com",
                "sam@example.org",
            ]
        }
    )

    detector = DetectorService()
    result = detector.detect_dataframe(df)

    assert result[0]["original_name"] == "email"
    assert result[0]["inferred_type"] == "email"
    assert result[0]["confidence"] == 100.0


def test_validator_catches_invalid_email():
    df = pd.DataFrame(
        {
            "email": [
                "josh@example.com",
                "bad-email",
            ]
        }
    )

    validator = ValidatorService()

    result = validator.validate_dataframe(
        df=df,
        confirmed_mappings=[
            {"original": "email", "canonical": "email"},
        ],
        target_schema=[
            {
                "name": "email",
                "type": "email",
                "required": True,
                "variants": [],
            },
        ],
    )

    assert result["total_rows"] == 2
    assert result["clean_rows"] == 1
    assert result["error_rows"] == 1
    assert result["errors"][0]["error_type"] == "INVALID_EMAIL"
    assert result["all_error_indices"] == [1]


def test_validator_returns_all_error_indices_independent_of_error_limit():
    df = pd.DataFrame(
        {
            "email": [
                "bad-one",
                "bad-two",
                "good@example.com",
            ]
        }
    )

    validator = ValidatorService()

    result = validator.validate_dataframe(
        df=df,
        confirmed_mappings=[
            {"original": "email", "canonical": "email"},
        ],
        target_schema=[
            {
                "name": "email",
                "type": "email",
                "required": True,
                "variants": [],
            },
        ],
        error_limit=1,
    )

    assert result["error_rows"] == 2
    assert result["errors_truncated"] is True
    assert len(result["errors"]) == 1
    assert result["all_error_indices"] == [0, 1]


def test_mapper_matches_fname_to_first_name_without_variants():
    mapper = MapperService()

    result = mapper.suggest_mappings(
        detected_columns=[
            {"original_name": "fname", "inferred_type": "string"},
        ],
        target_schema=[
            {
                "name": "first_name",
                "type": "string",
                "required": True,
                "variants": [],
            },
        ],
    )

    assert result[0]["suggested_canonical"] == "first_name"
    assert result[0]["confidence"] == 1.0


def test_mapper_matches_email_address_to_email_without_variants():
    mapper = MapperService()

    result = mapper.suggest_mappings(
        detected_columns=[
            {"original_name": "email address", "inferred_type": "string"},
        ],
        target_schema=[
            {
                "name": "email",
                "type": "email",
                "required": True,
                "variants": [],
            },
        ],
    )

    assert result[0]["suggested_canonical"] == "email"
    assert result[0]["confidence"] == 1.0


def test_mapper_matches_ph_no_to_phone_without_variants():
    mapper = MapperService()

    result = mapper.suggest_mappings(
        detected_columns=[
            {"original_name": "ph no", "inferred_type": "string"},
        ],
        target_schema=[
            {
                "name": "phone",
                "type": "phone",
                "required": False,
                "variants": [],
            },
        ],
    )

    assert result[0]["suggested_canonical"] == "phone"
    assert result[0]["confidence"] == 1.0