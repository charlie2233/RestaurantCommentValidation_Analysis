"""Deterministic brand entity resolution for reconciliation."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from difflib import SequenceMatcher

from qsr_audit.contracts.workbook import CANONICAL_BRAND_NAME_ALIASES


@dataclass(frozen=True)
class BrandResolution:
    """Resolved brand identity and audit metadata."""

    input_brand_name: str
    canonical_brand_name: str | None
    match_method: str
    match_confidence: float
    matched_alias: str | None = None

    @property
    def is_matched(self) -> bool:
        return self.canonical_brand_name is not None


def canonical_brand_dictionary() -> dict[str, tuple[str, ...]]:
    """Return canonical brands mapped to their known aliases."""

    by_brand: dict[str, set[str]] = {}
    for alias, canonical_name in CANONICAL_BRAND_NAME_ALIASES.items():
        by_brand.setdefault(canonical_name, set()).add(alias)
    for canonical_name in tuple(by_brand):
        by_brand[canonical_name].add(normalize_brand_key(canonical_name))
    return {brand: tuple(sorted(aliases)) for brand, aliases in sorted(by_brand.items())}


def normalize_brand_key(value: str | None) -> str:
    """Normalize a brand name into an alias lookup key."""

    if value is None:
        return ""
    text = str(value).strip().lower()
    text = text.replace("&", "and")
    text = re.sub(r"[^a-z0-9]+", "", text)
    return text


def resolve_brand_name(
    brand_name: str | None,
    *,
    candidate_brands: Iterable[str] | None = None,
) -> BrandResolution:
    """Resolve a workbook or reference brand to the canonical dictionary."""

    original = "" if brand_name is None else str(brand_name).strip()
    if not original:
        return BrandResolution(
            input_brand_name="",
            canonical_brand_name=None,
            match_method="missing",
            match_confidence=0.0,
        )

    normalized = normalize_brand_key(original)
    alias_map = _alias_map(candidate_brands)

    if normalized in alias_map:
        return BrandResolution(
            input_brand_name=original,
            canonical_brand_name=alias_map[normalized],
            match_method="alias_exact",
            match_confidence=1.0,
            matched_alias=normalized,
        )

    best_brand, best_score = _best_fuzzy_match(normalized, alias_map)
    if best_brand is not None and best_score >= 0.93:
        return BrandResolution(
            input_brand_name=original,
            canonical_brand_name=best_brand,
            match_method="fuzzy_high",
            match_confidence=0.9,
        )
    if best_brand is not None and best_score >= 0.85:
        return BrandResolution(
            input_brand_name=original,
            canonical_brand_name=best_brand,
            match_method="fuzzy_medium",
            match_confidence=0.75,
        )

    return BrandResolution(
        input_brand_name=original,
        canonical_brand_name=None,
        match_method="unmatched",
        match_confidence=0.0,
    )


def resolve_brand_series(
    values: Iterable[str | None],
    *,
    candidate_brands: Iterable[str] | None = None,
) -> list[BrandResolution]:
    """Resolve a series of brand names."""

    return [resolve_brand_name(value, candidate_brands=candidate_brands) for value in values]


def _alias_map(candidate_brands: Iterable[str] | None = None) -> dict[str, str]:
    alias_map = dict(CANONICAL_BRAND_NAME_ALIASES)
    if candidate_brands is not None:
        for brand in candidate_brands:
            normalized = normalize_brand_key(brand)
            if normalized:
                alias_map.setdefault(normalized, str(brand))
    else:
        for canonical_name in set(CANONICAL_BRAND_NAME_ALIASES.values()):
            alias_map.setdefault(normalize_brand_key(canonical_name), canonical_name)
    return alias_map


def _best_fuzzy_match(
    normalized: str,
    alias_map: dict[str, str],
) -> tuple[str | None, float]:
    best_brand: str | None = None
    best_score = 0.0
    for alias_key, canonical_name in alias_map.items():
        score = SequenceMatcher(a=normalized, b=alias_key).ratio()
        if score > best_score:
            best_brand = canonical_name
            best_score = score
    return best_brand, best_score


__all__ = [
    "BrandResolution",
    "canonical_brand_dictionary",
    "normalize_brand_key",
    "resolve_brand_name",
    "resolve_brand_series",
]
