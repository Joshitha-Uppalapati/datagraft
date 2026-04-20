from typing import Any

from rapidfuzz import fuzz


class MapperService:
    MIN_CONFIDENCE = 0.60

    def suggest_mappings(
        self,
        detected_columns: list[dict[str, Any]],
        target_schema: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        canonical_lookup = self._build_canonical_lookup(target_schema)

        suggestions: list[dict[str, Any]] = []

        for column in detected_columns:
            original_name = str(column["original_name"])
            suggested_canonical, confidence = self._fuzzy_match(
                original_name,
                canonical_lookup,
            )

            suggestions.append(
                {
                    "original": original_name,
                    "suggested_canonical": suggested_canonical,
                    "confidence": round(confidence, 2),
                }
            )

        return suggestions

    def _build_canonical_lookup(
        self,
        target_schema: list[dict[str, Any]],
    ) -> dict[str, list[str]]:
        lookup: dict[str, list[str]] = {}

        for field in target_schema:
            canonical_name = str(field["name"]).strip()
            variants = field.get("variants", [])

            all_variants = [canonical_name, *variants]
            normalized_variants = [
                str(value).strip()
                for value in all_variants
                if str(value).strip()
            ]

            lookup[canonical_name] = normalized_variants

        return lookup

    def _fuzzy_match(
        self,
        input_name: str,
        canonical_lookup: dict[str, list[str]],
    ) -> tuple[str | None, float]:
        best_canonical: str | None = None
        best_score = 0.0

        for canonical_name, variants in canonical_lookup.items():
            for variant in variants:
                score = fuzz.token_sort_ratio(input_name, variant) / 100.0
                if score > best_score:
                    best_score = score
                    best_canonical = canonical_name

        if best_score < self.MIN_CONFIDENCE:
            return None, best_score

        return best_canonical, best_score