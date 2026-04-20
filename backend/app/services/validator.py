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
        total_error_count = 0

        duplicate_mask = self._compute_duplicate_mask(df)

        for row_index, row in df.iterrows():
            row_has_error = False

            # Using .loc because index alignment can drift after filtering; iloc becomes dangerous silently.
            if bool(duplicate_mask.loc[row_index]):
                row_has_error = True
                total_error_count += 1
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
                        total_error_count += 1
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
                        total_error_count += 1
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
                        total_error_count += 1
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
                        total_error_count += 1
                        self._append_error(
                            errors,
                            error_limit,
                            row_index=int(row_index),
                            column=original_column,
                            value=value_str,
                            error_type="INVALID_DATE",
                            message=f"Value '{value_str}' is not a valid date.",
                        )

                elif field_type == "float":
                    if not self._can_cast_float(value_str):
                        row_has_error = True
                        total_error_count += 1
                        self._append_error(
                            errors,
                            error_limit,
                            row_index=int(row_index),
                            column=original_column,
                            value=value_str,
                            error_type="TYPE_MISMATCH",
                            message=f"Value '{value_str}' cannot be cast to float.",
                        )

            if row_has_error:
                rows_with_errors.add(int(row_index))

        total_rows = len(df)
        error_rows = len(rows_with_errors)
        clean_rows = total_rows - error_rows

        return {
            "total_rows": total_rows,
            "clean_rows": clean_rows,
            "error_rows": error_rows,
            "errors_truncated": total_error_count > error_limit,
            "errors": errors,
        }

    def _compute_duplicate_mask(self, df: pd.DataFrame) -> pd.Series:
        row_hashes = df.fillna("").astype(str).apply(self._row_digest_from_series, axis=1)
        return row_hashes.duplicated(keep="first")

    def _row_digest_from_series(self, row: pd.Series) -> str:
        # TODO: hashing stringified rows is still brittle; move to tuple(sorted(row.items()))
        normalized_values = [str(value) for value in row.values]
        payload = "\x00".join(normalized_values).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def _safe_row_repr(self, row: pd.Series) -> str:
        # We must normalize EXACTLY like duplicate detection or hashes won't match.
        normalized = row.fillna("").astype(str)
        digest = self._row_digest_from_series(normalized)
        return f"row_sha256:{digest[:16]}"