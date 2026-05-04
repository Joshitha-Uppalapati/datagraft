import pandas as pd

from app.services.detector import DetectorService
from app.services.validator import ValidatorService


def test_detector_identifies_email_column():
    df = pd.DataFrame({
        "email": [
            "josh@example.com",
            "jane@example.com",
            "sam@example.org",
        ]
    })

    detector = DetectorService()
    result = detector.detect_dataframe(df)

    assert result[0]["original_name"] == "email"
    assert result[0]["inferred_type"] == "email"
    assert result[0]["confidence"] == 100.0


def test_validator_catches_invalid_email():
    df = pd.DataFrame({
        "email": [
            "josh@example.com",
            "bad-email",
        ]
    })

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


def test_validator_returns_all_error_indices_independent_of_error_limit():
    df = pd.DataFrame({
        "email": [
            "bad-one",
            "bad-two",
            "good@example.com",
        ]
    })

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