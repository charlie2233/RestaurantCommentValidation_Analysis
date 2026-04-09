"""Shared syntheticness interpretation helpers for conservative review triage."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pandas as pd

from qsr_audit.reconcile.entity_resolution import resolve_brand_name

SYNTHETICNESS_INTERPRETATION_VERSION = "syntheticness-interpretation-v1.0.0"

FIELD_NAME_TO_METRIC = {
    "rank": "rank",
    "reference_rank": "rank",
    "us_store_count_2024": "store_count",
    "store_count": "store_count",
    "systemwide_revenue_usd_billions_2024": "system_sales",
    "system_sales": "system_sales",
    "average_unit_volume_usd_thousands": "auv",
    "implied_auv_k": "auv",
    "auv": "auv",
    "fte_mid": "fte_mid",
    "margin_mid_pct": "margin_mid_pct",
}
DEFAULT_SYNTHETICNESS_CAVEATS = (
    "Syntheticness signals are weak-to-moderate anomaly indicators, not proof of fabrication.",
    "Review clusters of signals, not any single metric in isolation.",
)
STRENGTH_SCORES = {
    "strong": 45,
    "moderate": 25,
    "weak": 10,
}


@dataclass(frozen=True)
class SyntheticnessInterpretation:
    """Row-level syntheticness interpretation used by credibility scoring."""

    syntheticness_score: int
    supporting_signals: list[dict[str, Any]]
    review_required: bool
    caveats: list[str]


def filter_applicable_signals(
    syntheticness_signals: pd.DataFrame,
    *,
    brand_name: str,
    canonical_brand_name: str,
    metric_name: str,
) -> list[dict[str, Any]]:
    """Return brand/metric signals that should influence one credibility row."""

    if syntheticness_signals.empty:
        return []

    aliases = _brand_aliases(brand_name, canonical_brand_name)
    applicable: list[dict[str, Any]] = []
    for row in syntheticness_signals.to_dict(orient="records"):
        signal_brand = extract_signal_brand_name(row)
        if signal_brand is None:
            continue
        if _normalize_brand_key(signal_brand) not in aliases:
            continue
        signal_metric = metric_name_for_signal(row)
        if signal_metric is not None and signal_metric != metric_name:
            continue
        applicable.append(row)
    return applicable


def interpret_syntheticness_signals(
    synthetic_signals: list[dict[str, Any]],
) -> SyntheticnessInterpretation:
    """Reduce raw syntheticness signals into a conservative row-level interpretation."""

    if not synthetic_signals:
        return SyntheticnessInterpretation(
            syntheticness_score=0,
            supporting_signals=[],
            review_required=False,
            caveats=[
                "No syntheticness signals were attached to this row.",
                "Absence of signals is not proof of cleanliness.",
            ],
        )

    ranked_signals: list[tuple[int, dict[str, Any]]] = []
    caveats: list[str] = []
    distinct_signal_keys: set[str] = set()
    for signal in synthetic_signals:
        contribution = _signal_contribution(signal)
        ranked_signals.append((contribution, signal))
        distinct_signal_keys.add(
            str(signal.get("field_name") or signal.get("signal_type") or "unknown")
        )
        caveat = signal.get("caveat")
        if caveat is not None:
            text = str(caveat).strip()
            if text and text not in caveats:
                caveats.append(text)

    ranked_signals.sort(
        key=lambda item: (
            item[0],
            str(item[1].get("signal_type") or ""),
            str(item[1].get("field_name") or ""),
            str(item[1].get("title") or ""),
        ),
        reverse=True,
    )

    syntheticness_score = min(
        100,
        int(
            round(
                sum(contribution for contribution, _signal in ranked_signals)
                + max(0, len(distinct_signal_keys) - 1) * 5
            )
        ),
    )
    supporting_signals = [
        {
            "title": str(signal.get("title") or "Untitled signal"),
            "strength": str(signal.get("strength") or "unknown"),
            "field_name": signal.get("field_name"),
            "signal_type": signal.get("signal_type"),
            "plain_english": str(signal.get("plain_english") or ""),
            "score_contribution": contribution,
        }
        for contribution, signal in ranked_signals[:3]
    ]
    review_required = syntheticness_score >= 35 or any(
        contribution >= STRENGTH_SCORES["moderate"] for contribution, _signal in ranked_signals
    )

    for default_caveat in DEFAULT_SYNTHETICNESS_CAVEATS:
        if default_caveat not in caveats:
            caveats.append(default_caveat)

    return SyntheticnessInterpretation(
        syntheticness_score=syntheticness_score,
        supporting_signals=supporting_signals,
        review_required=review_required,
        caveats=caveats[:5],
    )


def summarize_syntheticness_signals(
    synthetic_signals: list[dict[str, Any]],
) -> SyntheticnessInterpretation:
    """Backward-compatible alias for the shared interpretation path."""

    return interpret_syntheticness_signals(synthetic_signals)


def metric_name_for_signal(signal: dict[str, Any]) -> str | None:
    """Resolve a signal field to the credibility metric namespace."""

    field_name = signal.get("field_name")
    if field_name is None:
        return None
    return FIELD_NAME_TO_METRIC.get(str(field_name).strip())


def extract_signal_brand_name(signal: dict[str, Any]) -> str | None:
    """Best-effort brand extraction from signal details or title text."""

    details = _parse_json_value(signal.get("details"))
    if isinstance(details, dict):
        brand_name = details.get("brand_name")
        if brand_name is not None:
            return str(brand_name)

    title = signal.get("title")
    if isinstance(title, str):
        for marker in (" stands out ", " is unusual ", " deserves ", " AUV "):
            if marker in title:
                return title.split(marker, 1)[0]
    return None


def _syntheticness_strength_score(strength: str) -> int:
    return STRENGTH_SCORES.get(strength.lower(), 0)


def _signal_contribution(signal: dict[str, Any]) -> int:
    strength_score = _syntheticness_strength_score(str(signal.get("strength") or "unknown"))
    if signal.get("field_name") is None:
        return max(5, strength_score // 2)
    return strength_score


def _brand_aliases(brand_name: str, canonical_brand_name: str) -> set[str]:
    aliases = {
        _normalize_brand_key(brand_name),
        _normalize_brand_key(canonical_brand_name),
    }
    for candidate in (brand_name, canonical_brand_name):
        resolved = resolve_brand_name(candidate)
        aliases.add(_normalize_brand_key(resolved.canonical_brand_name))
    return aliases


def _normalize_brand_key(value: str) -> str:
    return "".join(ch.lower() for ch in value if ch.isalnum())


def _parse_json_value(value: object) -> object:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, dict | list):
        return value
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return value


__all__ = [
    "DEFAULT_SYNTHETICNESS_CAVEATS",
    "SYNTHETICNESS_INTERPRETATION_VERSION",
    "SyntheticnessInterpretation",
    "extract_signal_brand_name",
    "filter_applicable_signals",
    "interpret_syntheticness_signals",
    "metric_name_for_signal",
    "summarize_syntheticness_signals",
]
