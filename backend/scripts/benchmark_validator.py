import sys
import time
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from app.services.validator import ValidatorService

def build_dataframe(row_count: int = 100_000) -> pd.DataFrame:
    rows = []

    for i in range(row_count):
        rows.append(
            {
                "first_name": f"User{i}",
                "email": f"user{i}@example.com",
                "phone": f"555000{i % 10000:04d}",
                "signup_date": "2024-01-15",
                "amount": f"{10 + (i % 100)}.50",
            }
        )

    rows[10]["email"] = "bad-email"
    rows[20]["phone"] = "123"
    rows[30]["signup_date"] = "not-a-date"
    rows[40]["amount"] = "$abc"
    rows[50] = rows[0].copy()

    return pd.DataFrame(rows)


def main() -> None:
    row_count = 100_000
    df = build_dataframe(row_count)

    confirmed_mappings = [
        {"original": "first_name", "canonical": "first_name"},
        {"original": "email", "canonical": "email"},
        {"original": "phone", "canonical": "phone"},
        {"original": "signup_date", "canonical": "signup_date"},
        {"original": "amount", "canonical": "amount"},
    ]

    target_schema = [
        {"name": "first_name", "type": "string", "required": True, "variants": []},
        {"name": "email", "type": "email", "required": True, "variants": []},
        {"name": "phone", "type": "phone", "required": True, "variants": []},
        {"name": "signup_date", "type": "date", "required": True, "variants": []},
        {"name": "amount", "type": "float", "required": True, "variants": []},
    ]

    validator = ValidatorService()

    start = time.perf_counter()
    result = validator.validate_dataframe(
        df=df,
        confirmed_mappings=confirmed_mappings,
        target_schema=target_schema,
        error_limit=100,
    )
    elapsed = time.perf_counter() - start

    print(f"rows: {row_count}")
    print(f"seconds: {elapsed:.3f}")
    print(f"clean_rows: {result['clean_rows']}")
    print(f"error_rows: {result['error_rows']}")
    print(f"errors_returned: {len(result['errors'])}")
    print(f"all_error_indices: {result['all_error_indices'][:10]}")


if __name__ == "__main__":
    main()