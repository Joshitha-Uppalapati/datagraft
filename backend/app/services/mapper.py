from typing import Any

from rapidfuzz import fuzz


CANONICAL_COLUMNS = {
    "first_name": [
        "fname",
        "first name",
        "firstname",
        "given name",
        "given_name",
    ],
    "last_name": [
        "lname",
        "last name",
        "lastname",
        "surname",
        "family name",
        "family_name",
    ],
    "email": [
        "email address",
        "email_address",
        "e-mail",
        "mail",
        "mail id",
        "mail_id",
    ],
    "phone": [
        "phone number",
        "phone_number",
        "ph no",
        "ph_no",
        "mobile",
        "mobile number",
        "contact",
        "contact number",
    ],
    "date": [
        "transaction date",
        "txn date",
        "created date",
        "signup date",
        "registered on",
    ],
    "signup_date": [
        "signup date",
        "registered on",
        "registration date",
        "created date",
        "date joined",
    ],
    "amount": [
        "amt",
        "total amount",
        "transaction amount",
        "price",
        "cost",
        "amount usd",
    ],
    "description": [
        "desc",
        "details",
        "memo",
        "notes",
        "transaction description",
    ],
}


class MapperService:
    MIN_CONFIDENCE = 0.60

    def suggest_mappings(
        self,
        detected_columns: list[dict[str, Any]],
        target_schema: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        mappings = []

        for column in detected_columns:
            original_name = column["original_name"]

            exact_match = self._find_exact_synonym_match(
                source_name=original_name,
                target_schema=target_schema,
            )

            if exact_match:
                mappings.append(
                    {
                        "original": original_name,
                        "suggested_canonical": exact_match,
                        "confidence": 1.0,
                    }
                )
                continue

            best_match = None
            best_score = 0.0

            for field in target_schema:
                canonical_name = field["name"]

                candidates = [
                    canonical_name,
                    *field.get("variants", []),
                    *CANONICAL_COLUMNS.get(canonical_name, []),
                ]

                for candidate in candidates:
                    score = fuzz.token_sort_ratio(
                        self._readable_name(original_name),
                        self._readable_name(candidate),
                    ) / 100

                    if score > best_score:
                        best_score = score
                        best_match = canonical_name

            mappings.append(
                {
                    "original": original_name,
                    "suggested_canonical": (
                        best_match if best_score >= self.MIN_CONFIDENCE else None
                    ),
                    "confidence": round(best_score, 2),
                }
            )

        return mappings

    def _find_exact_synonym_match(
        self,
        source_name: str,
        target_schema: list[dict[str, Any]],
    ) -> str | None:
        normalized_source = self._normalize_name(source_name)

        for field in target_schema:
            canonical_name = field["name"]

            candidates = [
                canonical_name,
                *field.get("variants", []),
                *CANONICAL_COLUMNS.get(canonical_name, []),
            ]

            for candidate in candidates:
                if self._normalize_name(candidate) == normalized_source:
                    return canonical_name

        return None

    def _normalize_name(self, value: str) -> str:
        return "".join(char for char in value.lower() if char.isalnum())

    def _readable_name(self, value: str) -> str:
        return value.replace("_", " ").replace("-", " ").strip().lower()