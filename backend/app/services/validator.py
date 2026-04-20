import csv
import hashlib
import re
from typing import Any

import pandas as pd


class ValidatorService:
    EMAIL_REGEX = re.compile(
        r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$",
        re.IGNORECASE,
    )
    PHONE_ALLOWED_CHARS_REGEX = re.compile(r"^[\d\+\-\(\)\.\s/]+$")
    NON_DIGIT_REGEX = re.compile(r"\D+")
    CURRENCY_SYMBOL_REGEX = re.compile(r"[$,₹€£]")

    def validate_dataframe(
        self,
        df: pd.DataFrame,
        confirmed_mappings: list[dict[str, str]],
        target_schema: list[dict[str, Any]],
        error_limit: int = 100,
    ) -> dict[str, Any]:
        mapping_by_original = {
            item["original"]: item["canonical"]
            for item in confirmed_mappings
        }
        schema_by_name = {
            field["name"]: field
            for field in target_schema
        }

        errors: list[dict[str, Any]] = []
        rows_with_errors: set[int] = set()

        duplicate_mask = self._compute_duplicate_mask(df)

        for row_index, row in df.iterrows():
            row_has_error = False

            if bool(duplicate_mask.iloc[row_index]):
                row_has_error = True
                self._append_error(
                    errors,
                    error_limit,
                    row_index=int(row_index),
                    column="__row__",
                    value=self._safe_row_repr(row),
                    error_type="DUPLICATE_ROW",
                    message="Row is a duplicate of a previous row.",
                )

            for original_column, canonical_column in mapping_by_original.items():
                if original_column not in df.columns:
                    continue

                schema = schema_by_name.get(canonical_column)
                if not schema:
                    continue

                raw_value = row[original_column]
                field_type = str(schema.get("type", "string")).lower()
                required = bool(schema.get("required", False))

                if self._is_null(raw_value):
                    if required:
                        row_has_error = True
                        self._append_error(
                            errors,
                            error_limit,
                            row_index=int(row_index),
                            column=original_column,
                            value=None,
                            error_type="NULL_REQUIRED",
                            message=f"Required field '{canonical_column}' is null or empty.",
                        )
                    continue

                value_str = str(raw_value).strip()

                if field_type == "email":
                    if not self._is_valid_email(value_str):
                        row_has_error = True
                        self._append_error(
                            errors,
                            error_limit,
                            row_index=int(row_index),
                            column=original_column,
                            value=value_str,
                            error_type="INVALID_EMAIL",
                            message=f"Value '{value_str}' is not a valid email address.",
                        )

                elif field_type == "phone":
                    if not self._is_valid_phone(value_str):
                        row_has_error = True
                        self._append_error(
                            errors,
                            error_limit,
                            row_index=int(row_index),
                            column=original_column,
                            value=value_str,
                            error_type="INVALID_PHONE",
                            message=f"Value '{value_str}' is not a valid phone number.",
                        )

                elif field_type == "date":
                    if not self._is_valid_date(value_str):
                        row_has_error = True
                        self._append_error(
                            errors,
                            error_limit,
                            row_index=int(row_index),
                            column=original_column,
                            value=value_str,
                            error_type="INVALID_DATE",
                            message=f"Value '{value_str}' is not a valid date.",
                        )

                elif field_type == "integer":
                    if not self._can_cast_integer(value_str):
                        row_has_error = True
                        self._append_error(
                            errors,
                            error_limit,
                            row_index=int(row_index),
                            column=original_column,
                            value=value_str,
                            error_type="TYPE_MISMATCH",
                            message=f"Value '{value_str}' cannot be cast to integer.",
                        )

                elif field_type == "float":
                    if not self._can_cast_float(value_str):
                        row_has_error = True
                        self._append_error(
                            errors,
                            error_limit,
                            row_index=int(row_index),
                            column=original_column,
                            value=value_str,
                            error_type="TYPE_MISMATCH",
                            message=f"Value '{value_str}' cannot be cast to float.",
                        )

                elif field_type == "boolean":
                    if not self._can_cast_boolean(value_str):
                        row_has_error = True
                        self._append_error(
                            errors,
                            error_limit,
                            row_index=int(row_index),
                            column=original_column,
                            value=value_str,
                            error_type="TYPE_MISMATCH",
                            message=f"Value '{value_str}' cannot be cast to boolean.",
                        )

            if row_has_error:
                rows_with_errors.add(int(row_index))

        total_rows = int(len(df))
        error_rows = int(len(rows_with_errors))
        clean_rows = int(total_rows - error_rows)

        return {
            "total_rows": total_rows,
            "clean_rows": clean_rows,
            "error_rows": error_rows,
            "errors": errors,
        }

    def _append_error(
        self,
        errors: list[dict[str, Any]],
        error_limit: int,
        row_index: int,
        column: str,
        value: Any,
        error_type: str,
        message: str,
    ) -> None:
        if len(errors) >= error_limit:
            return

        errors.append(
            {
                "row_index": row_index,
                "column": column,
                "value": value,
                "error_type": error_type,
                "message": message,
            }
        )

    def _is_null(self, value: Any) -> bool:
        if pd.isna(value):
            return True
        if isinstance(value, str) and not value.strip():
            return True
        return False

    def _is_valid_email(self, value: str) -> bool:
        return bool(self.EMAIL_REGEX.fullmatch(value))

    def _is_valid_phone(self, value: str) -> bool:
        if not self.PHONE_ALLOWED_CHARS_REGEX.fullmatch(value):
            return False
        digits_only = self.NON_DIGIT_REGEX.sub("", value)
        return 10 <= len(digits_only) <= 15

    def _is_valid_date(self, value: str) -> bool:
        parsed = pd.to_datetime(value, errors="coerce")
        return pd.notna(parsed)

    def _normalize_numeric(self, value: str) -> str:
        cleaned = self.CURRENCY_SYMBOL_REGEX.sub("", value).strip()
        if cleaned.startswith("(") and cleaned.endswith(")"):
            cleaned = f"-{cleaned[1:-1]}"
        return cleaned

    def _can_cast_integer(self, value: str) -> bool:
        normalized = self._normalize_numeric(value)
        numeric = pd.to_numeric(normalized, errors="coerce")
        if pd.isna(numeric):
            return False
        return float(numeric).is_integer()

    def _can_cast_float(self, value: str) -> bool:
        normalized = self._normalize_numeric(value)
        numeric = pd.to_numeric(normalized, errors="coerce")
        return pd.notna(numeric)

    def _can_cast_boolean(self, value: str) -> bool:
        return value.strip().lower() in {"true", "false", "yes", "no", "y", "n", "t", "f", "1", "0"}

    def _compute_duplicate_mask(self, df: pd.DataFrame) -> pd.Series:
        row_hashes = df.fillna("").astype(str).apply(
            lambda row: hashlib.sha256("||".join(row.values).encode("utf-8")).hexdigest(),
            axis=1,
        )
        return row_hashes.duplicated(keep="first")

    def _safe_row_repr(self, row: pd.Series) -> str:
        return str(row.to_dict())