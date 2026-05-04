import re
from typing import Any

import pandas as pd


class ValidatorService:
    EMAIL_REGEX = re.compile(
        r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    )

    def _is_null(self, val: Any) -> bool:
        return pd.isna(val) or str(val).strip() == ""

    def _is_valid_email(self, val: str) -> bool:
        if self._is_null(val):
            return False
        return self.EMAIL_REGEX.match(val) is not None

    def _is_valid_phone(self, val: str) -> bool:
        if self._is_null(val):
            return False
        digits = re.sub(r"\D", "", val)
        return 10 <= len(digits) <= 15

    def _is_valid_date(self, val: str) -> bool:
        if self._is_null(val):
            return False
        try:
            pd.to_datetime(val, errors="raise")
            return True
        except Exception:
            return False

    def _can_cast_float(self, val: str) -> bool:
        if self._is_null(val):
            return False
        cleaned = re.sub(r"[$,₹€£]", "", val).strip()
        try:
            float(cleaned)
            return True
        except ValueError:
            return False

    def _append_error(
        self,
        errors: list[dict],
        *,
        column: str,
        row_index: int,
        error_type: str,
        message: str,
    ) -> None:
        errors.append({
            "column": column,
            "row_index": row_index,
            "error_type": error_type,
            "message": message,
        })

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

        # Reset index to avoid weird pandas index bugs
        df_working = df.reset_index(drop=True)

        # Fast duplicate detection
        duplicate_mask = df_working.duplicated(keep="first")

        for row_index, row in df_working.iterrows():
            current_row = int(row_index)
            row_has_error = False

            # duplicate check
            if duplicate_mask.iloc[current_row]:
                row_has_error = True
                total_error_count += 1

                if len(errors) < error_limit:
                    self._append_error(
                        errors,
                        column="__row__",
                        row_index=current_row,
                        error_type="DUPLICATE_ROW",
                        message="Row is a duplicate of a previous row.",
                    )

            # field validation
            for original_column, canonical_column in mapping_by_original.items():

                if original_column not in df_working.columns:
                    continue

                schema = schema_by_name.get(canonical_column)
                if not schema:
                    continue

                try:
                    raw_value = row[original_column]
                except Exception:
                    continue

                field_type = str(schema.get("type", "string")).lower()
                required = bool(schema.get("required", False))

                if self._is_null(raw_value):
                    if required:
                        row_has_error = True
                        total_error_count += 1

                        if len(errors) < error_limit:
                            self._append_error(
                                errors,
                                column=original_column,
                                row_index=current_row,
                                error_type="NULL_REQUIRED",
                                message=f"{canonical_column} is required but missing.",
                            )
                    continue

                value_str = str(raw_value).strip()

                is_valid = True
                error_type = ""
                message = ""

                if field_type == "email" and not self._is_valid_email(value_str):
                    is_valid = False
                    error_type = "INVALID_EMAIL"
                    message = f"{value_str} is not a valid email."

                elif field_type == "phone" and not self._is_valid_phone(value_str):
                    is_valid = False
                    error_type = "INVALID_PHONE"
                    message = f"{value_str} is not a valid phone number."

                elif field_type == "date" and not self._is_valid_date(value_str):
                    is_valid = False
                    error_type = "INVALID_DATE"
                    message = f"{value_str} is not a valid date."

                elif field_type == "float" and not self._can_cast_float(value_str):
                    is_valid = False
                    error_type = "TYPE_MISMATCH"
                    message = f"{value_str} cannot be cast to float."

                if not is_valid:
                    row_has_error = True
                    total_error_count += 1

                    if len(errors) < error_limit:
                        self._append_error(
                            errors,
                            column=original_column,
                            row_index=current_row,
                            error_type=error_type,
                            message=message,
                        )

            if row_has_error:
                rows_with_errors.add(current_row)

        total_rows = len(df_working)
        error_rows = len(rows_with_errors)

        return {
            "total_rows": total_rows,
            "clean_rows": total_rows - error_rows,
            "error_rows": error_rows,
            "errors_truncated": total_error_count > error_limit,
            "errors": errors,
        }