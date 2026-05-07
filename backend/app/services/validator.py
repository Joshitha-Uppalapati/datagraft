import re
from typing import Any

import numpy as np
import pandas as pd


class ValidatorService:
    EMAIL_REGEX = re.compile(
        r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    )

    BOOLEAN_VALUES = {"true", "false", "1", "0", "yes", "no"}

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

        df_working = df.reset_index(drop=True)

        errors: list[dict[str, Any]] = []
        all_error_indices: set[int] = set()
        total_error_count = 0

        def append_errors(
            mask: pd.Series,
            column: str,
            error_type: str,
            message_for_row,
        ) -> None:
            nonlocal total_error_count

            failed_indices = mask[mask].index.tolist()

            if not failed_indices:
                return

            total_error_count += len(failed_indices)
            all_error_indices.update(int(index) for index in failed_indices)

            for row_index in failed_indices:
                if len(errors) >= error_limit:
                    break

                errors.append(
                    {
                        "column": column,
                        "row_index": int(row_index),
                        "error_type": error_type,
                        "message": message_for_row(row_index),
                    }
                )

        duplicate_mask = df_working.duplicated(keep="first")

        append_errors(
            duplicate_mask,
            "__row__",
            "DUPLICATE_ROW",
            lambda _: "Duplicate row detected.",
        )

        for original_column, canonical_column in mapping_by_original.items():
            if original_column not in df_working.columns:
                continue

            schema = schema_by_name.get(canonical_column)
            if not schema:
                continue

            raw_values = df_working[original_column]
            values = raw_values.astype("string").str.strip()

            null_mask = raw_values.isna() | values.isna() | values.eq("")
            non_null_mask = ~null_mask

            field_type = str(schema.get("type", "string")).lower()
            required = bool(schema.get("required", False))

            if required:
                append_errors(
                    null_mask,
                    original_column,
                    "NULL_REQUIRED",
                    lambda _, name=canonical_column: f"{name} is required.",
                )

            if field_type == "email":
                valid_mask = values.str.match(self.EMAIL_REGEX, na=False)
                invalid_mask = non_null_mask & ~valid_mask

                append_errors(
                    invalid_mask,
                    original_column,
                    "INVALID_EMAIL",
                    lambda index, series=values: (
                        f"{series.loc[index]} is not a valid email."
                    ),
                )

            elif field_type == "phone":
                digits = values.str.replace(r"\D", "", regex=True)
                valid_mask = digits.str.len().between(10, 15)
                invalid_mask = non_null_mask & ~valid_mask

                append_errors(
                    invalid_mask,
                    original_column,
                    "INVALID_PHONE",
                    lambda index, series=values: (
                        f"{series.loc[index]} is not a valid phone number."
                    ),
                )

            elif field_type == "date":
                parsed = pd.to_datetime(values, errors="coerce")
                invalid_mask = non_null_mask & parsed.isna()

                append_errors(
                    invalid_mask,
                    original_column,
                    "INVALID_DATE",
                    lambda index, series=values: (
                        f"{series.loc[index]} is not a valid date."
                    ),
                )

            elif field_type == "float":
                cleaned = values.str.replace(r"[$,₹€£]", "", regex=True)
                numeric = pd.to_numeric(cleaned, errors="coerce")
                invalid_mask = non_null_mask & numeric.isna()

                append_errors(
                    invalid_mask,
                    original_column,
                    "TYPE_MISMATCH",
                    lambda index, series=values: (
                        f"{series.loc[index]} cannot be cast to float."
                    ),
                )

            elif field_type == "integer":
                numeric = pd.to_numeric(values, errors="coerce")
                numeric_float = numeric.astype("float64")
                finite_mask = pd.Series(
                    np.isfinite(numeric_float),
                    index=values.index,
                )
                integer_mask = numeric.notna() & finite_mask & ((numeric % 1) == 0)
                invalid_mask = non_null_mask & ~integer_mask

                append_errors(
                    invalid_mask,
                    original_column,
                    "TYPE_MISMATCH",
                    lambda index, series=values: (
                        f"{series.loc[index]} cannot be cast to integer."
                    ),
                )

            elif field_type == "boolean":
                valid_mask = values.str.lower().isin(self.BOOLEAN_VALUES)
                invalid_mask = non_null_mask & ~valid_mask

                append_errors(
                    invalid_mask,
                    original_column,
                    "TYPE_MISMATCH",
                    lambda index, series=values: (
                        f"{series.loc[index]} is not a valid boolean."
                    ),
                )

        total_rows = len(df_working)
        error_rows = len(all_error_indices)

        return {
            "total_rows": total_rows,
            "clean_rows": total_rows - error_rows,
            "error_rows": error_rows,
            "errors_truncated": total_error_count > error_limit,
            "errors": errors,
            "all_error_indices": sorted(all_error_indices),
        }