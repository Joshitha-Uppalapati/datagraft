import re
from typing import Any

import pandas as pd


class ValidatorService:
    EMAIL_REGEX = re.compile(
        r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    )

    def _is_null(self, val: Any) -> bool:
        return pd.isna(val) or str(val).strip() == ""

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
        all_error_indices: set[int] = set()
        total_error_count = 0

        df_working = df.reset_index(drop=True)
        duplicate_mask = df_working.duplicated(keep="first")

        for row_index, row in df_working.iterrows():
            row_has_error = False

            # Duplicate check
            if duplicate_mask.iloc[row_index]:
                row_has_error = True
                total_error_count += 1
                all_error_indices.add(row_index)

                if len(errors) < error_limit:
                    errors.append({
                        "column": "__row__",
                        "row_index": row_index,
                        "error_type": "DUPLICATE_ROW",
                        "message": "Duplicate row detected.",
                    })

            for original_column, canonical_column in mapping_by_original.items():
                if original_column not in df_working.columns:
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
                        all_error_indices.add(row_index)

                        if len(errors) < error_limit:
                            errors.append({
                                "column": original_column,
                                "row_index": row_index,
                                "error_type": "NULL_REQUIRED",
                                "message": f"{canonical_column} is required.",
                            })
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

                elif field_type == "integer" and not self._can_cast_int(value_str):
                    is_valid = False
                    error_type = "TYPE_MISMATCH"
                    message = f"{value_str} cannot be cast to integer."

                elif field_type == "boolean" and not self._is_valid_boolean(value_str):
                    is_valid = False
                    error_type = "TYPE_MISMATCH"
                    message = f"{value_str} is not a valid boolean."

                if not is_valid:
                    row_has_error = True
                    total_error_count += 1
                    all_error_indices.add(row_index)

                    if len(errors) < error_limit:
                        errors.append({
                            "column": original_column,
                            "row_index": row_index,
                            "error_type": error_type,
                            "message": message,
                        })

        total_rows = len(df_working)
        error_rows = len(all_error_indices)

        return {
            "total_rows": total_rows,
            "clean_rows": total_rows - error_rows,
            "error_rows": error_rows,
            "errors_truncated": total_error_count > error_limit,
            "errors": errors,
            "all_error_indices": list(all_error_indices),
        }

    def _is_valid_email(self, val: str) -> bool:
        return self.EMAIL_REGEX.match(val) is not None

    def _is_valid_phone(self, val: str) -> bool:
        digits = re.sub(r"\D", "", val)
        return 10 <= len(digits) <= 15

    def _is_valid_date(self, val: str) -> bool:
        try:
            pd.to_datetime(val, errors="raise")
            return True
        except Exception:
            return False

    def _can_cast_float(self, val: str) -> bool:
        cleaned = re.sub(r"[$,₹€£]", "", val)
        try:
            float(cleaned)
            return True
        except ValueError:
            return False

    def _can_cast_int(self, val: str) -> bool:
        try:
            int(float(val))
            return True
        except Exception:
            return False

    def _is_valid_boolean(self, val: str) -> bool:
        return val.lower() in {"true", "false", "1", "0", "yes", "no"}