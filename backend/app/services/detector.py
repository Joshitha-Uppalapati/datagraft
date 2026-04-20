import re
from typing import Any

import numpy as np
import pandas as pd


class DetectorService:
    MIN_TYPE_CONFIDENCE: float = 80.0

    EMAIL_REGEX = re.compile(
        r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$",
        re.IGNORECASE,
    )

    BOOLEAN_VALUES = {
        "true",
        "false",
        "yes",
        "no",
        "y",
        "n",
        "t",
        "f",
        "1",
        "0",
    }

    PHONE_ALLOWED_CHARS_REGEX = re.compile(r"^[\d\+\-\(\)\.\s/]+$")
    NON_DIGIT_REGEX = re.compile(r"\D+")

    PRIORITY_ORDER = (
        "email",
        "phone",
        "date",
        "boolean",
        "integer",
        "float",
    )

    def detect_series(self, series: pd.Series) -> dict[str, Any]:
        null_count = int(series.isna().sum())

        non_null = series.dropna()
        if non_null.empty:
            return {
                "inferred_type": "unknown",
                "confidence": 0.0,
                "null_count": null_count,
                "sample_values": [],
            }

        string_values = self._normalize_to_strings(non_null)
        sample_values = string_values.head(3).tolist()

        detectors = {
            "email": self._email_confidence,
            "phone": self._phone_confidence,
            "date": self._date_confidence,
            "boolean": self._boolean_confidence,
            "integer": self._integer_confidence,
            "float": self._float_confidence,
        }

        for candidate in self.PRIORITY_ORDER:
            confidence = detectors[candidate](string_values)
            if confidence >= self.MIN_TYPE_CONFIDENCE:
                return {
                    "inferred_type": candidate,
                    "confidence": round(confidence, 2),
                    "null_count": null_count,
                    "sample_values": sample_values,
                }

        return {
            "inferred_type": "string",
            "confidence": 100.0,
            "null_count": null_count,
            "sample_values": sample_values,
        }

    def detect_dataframe(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []

        for column_name in df.columns:
            detection = self.detect_series(df[column_name])
            results.append(
                {
                    "original_name": str(column_name),
                    **detection,
                }
            )

        return results

    def _normalize_to_strings(self, series: pd.Series) -> pd.Series:
        return series.astype(str).str.strip()

    def _email_confidence(self, values: pd.Series) -> float:
        matches = values.str.fullmatch(self.EMAIL_REGEX, na=False)
        return self._percentage(matches)

    def _phone_confidence(self, values: pd.Series) -> float:
        allowed_chars = values.str.fullmatch(self.PHONE_ALLOWED_CHARS_REGEX, na=False)
        digits_only = values.str.replace(self.NON_DIGIT_REGEX, "", regex=True)
        valid_digit_length = digits_only.str.len().between(10, 15)
        matches = allowed_chars & valid_digit_length
        return self._percentage(matches)

    def _date_confidence(self, values: pd.Series) -> float:
        parsed = pd.to_datetime(values, errors="coerce")
        matches = parsed.notna()
        return self._percentage(matches)

    def _boolean_confidence(self, values: pd.Series) -> float:
        lowered = values.str.lower()
        matches = lowered.isin(self.BOOLEAN_VALUES)
        return self._percentage(matches)

    def _integer_confidence(self, values: pd.Series) -> float:
        numeric = pd.to_numeric(values, errors="coerce")
        is_not_null = numeric.notna()

        if not is_not_null.any():
            return 0.0

        finite_mask = pd.Series(np.isfinite(numeric), index=values.index)
        integer_mask = is_not_null & finite_mask & ((numeric % 1) == 0)
        return self._percentage(integer_mask)

    def _float_confidence(self, values: pd.Series) -> float:
        numeric = pd.to_numeric(values, errors="coerce")
        is_not_null = numeric.notna()

        if not is_not_null.any():
            return 0.0

        finite_mask = pd.Series(np.isfinite(numeric), index=values.index)
        float_mask = is_not_null & finite_mask
        return self._percentage(float_mask)

    def _percentage(self, matches: pd.Series) -> float:
        if matches.empty:
            return 0.0
        return float(matches.mean() * 100)