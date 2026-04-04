"""Small parsing helpers for workbook ingestion."""

from __future__ import annotations

import math
import re
import unicodedata

from qsr_audit.contracts import CANONICAL_BRAND_NAME_ALIASES

_RANGE_SEPARATOR_RE = re.compile(r"\s*[-–—~]\s*")
_NON_NUMERIC_RE = re.compile(r"[^0-9.\-]")
_WHITESPACE_RE = re.compile(r"\s+")
_BRAND_SIMPLIFIER_RE = re.compile(r"[^a-z0-9]+")


def parse_numeric_range(value: object) -> tuple[float | None, float | None, float | None]:
    """Parse a numeric range like ``6-8`` or ``8-12%`` into min/max/mid."""

    text = _coerce_text(value)
    if text is None:
        return (None, None, None)

    normalized = text.replace(",", "")
    parts = [part for part in _RANGE_SEPARATOR_RE.split(normalized) if part]
    if len(parts) == 1:
        number = _safe_float(parts[0])
        if number is None:
            return (None, None, None)
        return (number, number, number)

    if len(parts) != 2:
        return (None, None, None)

    minimum = _safe_float(parts[0])
    maximum = _safe_float(parts[1])
    if minimum is None or maximum is None:
        return (None, None, None)

    midpoint = (minimum + maximum) / 2
    return (minimum, maximum, midpoint)


def parse_fte_range(value: object) -> tuple[float | None, float | None, float | None]:
    """Parse FTE ranges like ``6-8``."""

    return parse_numeric_range(value)


def parse_margin_range(value: object) -> tuple[float | None, float | None, float | None]:
    """Parse percentage ranges like ``8-12%``."""

    return parse_numeric_range(value)


def canonicalize_brand_name(value: object) -> str | None:
    """Normalize brand names into a stable canonical label for joins."""

    text = _coerce_text(value)
    if text is None:
        return None

    ascii_text = (
        unicodedata.normalize("NFKD", text)
        .replace("’", "'")
        .replace("‘", "'")
        .replace("–", "-")
        .replace("—", "-")
    )
    cleaned = _WHITESPACE_RE.sub(" ", ascii_text).strip()
    lookup_key = _BRAND_SIMPLIFIER_RE.sub("", cleaned.casefold())

    return CANONICAL_BRAND_NAME_ALIASES.get(lookup_key, cleaned)


def normalize_text(value: object) -> str | None:
    """Normalizes empty cells and line endings without truncating long text."""

    text = _coerce_text(value)
    if text is None:
        return None
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _coerce_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None

    text = str(value).strip()
    if not text:
        return None
    return text


def _safe_float(value: str) -> float | None:
    digits = _NON_NUMERIC_RE.sub("", value)
    if not digits or digits in {"-", ".", "-."}:
        return None

    try:
        return float(digits)
    except ValueError:
        return None
