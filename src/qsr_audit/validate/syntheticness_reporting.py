"""Human-readable reporting for syntheticness anomaly signals.

These helpers intentionally frame the output as weak-to-moderate anomaly
evidence, not proof of fabrication. The orchestration layer is expected to
provide structured signals from Benford, heaping, outlier, and multivariate
checks; this module turns those signals into analyst-readable markdown.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

SyntheticnessStrength = Literal["weak", "moderate", "strong", "unknown"]


@dataclass(frozen=True)
class SyntheticnessSignal:
    """Structured anomaly signal ready for markdown rendering."""

    signal_type: str
    title: str
    plain_english: str
    strength: SyntheticnessStrength = "unknown"
    dataset: str = "core_brand_metrics"
    field_name: str | None = None
    method: str | None = None
    sample_size: int | None = None
    score: float | None = None
    benchmark: float | None = None
    p_value: float | None = None
    z_score: float | None = None
    threshold: float | None = None
    observed: str | None = None
    expected: str | None = None
    interpretation: str | None = None
    caveat: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SyntheticnessReport:
    """Container for a rendered syntheticness analysis."""

    source_path: Path | None
    source_kind: str | None
    signals: tuple[SyntheticnessSignal, ...]
    generated_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    title: str = "Syntheticness Report"

    @property
    def counts(self) -> dict[str, int]:
        counts = {"weak": 0, "moderate": 0, "strong": 0, "unknown": 0}
        for signal in self.signals:
            counts[signal.strength] += 1
        return counts


DEFAULT_OUTPUT_PATH = Path("reports/validation/syntheticness_report.md")

DEFAULT_CAVEATS = (
    "These signals are weak-to-moderate anomaly indicators, not proof of fabrication.",
    "Benford tests are often unreliable for tiny samples, bounded ranges, engineered values, or business metrics that are naturally constrained.",
    "Rounding and end-digit heaping can reflect reporting conventions, target-setting, or metric normalization rather than manipulation.",
    "A single anomalous field is rarely decisive; review clusters of signals, the data-generation process, and the underlying workbook context.",
)


def coerce_signal(signal: SyntheticnessSignal | Mapping[str, Any]) -> SyntheticnessSignal:
    """Convert a mapping or signal-like object into a typed signal."""

    if isinstance(signal, SyntheticnessSignal):
        return signal

    payload = dict(signal)
    return SyntheticnessSignal(
        signal_type=str(payload.get("signal_type") or payload.get("type") or "unknown"),
        title=str(payload.get("title") or payload.get("name") or "Untitled signal"),
        plain_english=str(
            payload.get("plain_english")
            or payload.get("explanation")
            or payload.get("message")
            or "No plain-English explanation was provided."
        ),
        strength=_normalize_strength(payload.get("strength")),
        dataset=str(payload.get("dataset") or "core_brand_metrics"),
        field_name=_maybe_str(payload.get("field_name")),
        method=_maybe_str(payload.get("method")),
        sample_size=_maybe_int(payload.get("sample_size")),
        score=_maybe_float(payload.get("score")),
        benchmark=_maybe_float(payload.get("benchmark")),
        p_value=_maybe_float(payload.get("p_value")),
        z_score=_maybe_float(payload.get("z_score")),
        threshold=_maybe_float(payload.get("threshold")),
        observed=_maybe_str(payload.get("observed")),
        expected=_maybe_str(payload.get("expected")),
        interpretation=_maybe_str(payload.get("interpretation")),
        caveat=_maybe_str(payload.get("caveat")),
        details=_json_safe(payload.get("details") or {}),
    )


def build_syntheticness_report(
    signals: Sequence[SyntheticnessSignal | Mapping[str, Any]],
    *,
    source_path: Path | str | None = None,
    source_kind: str | None = None,
    title: str = "Syntheticness Report",
    generated_at: datetime | None = None,
) -> SyntheticnessReport:
    """Build a report object from structured signals."""

    coerced = tuple(coerce_signal(signal) for signal in signals)
    resolved_source_path = None if source_path is None else Path(source_path)
    return SyntheticnessReport(
        source_path=resolved_source_path,
        source_kind=source_kind,
        signals=coerced,
        generated_at=generated_at or datetime.now(tz=UTC),
        title=title,
    )


def render_syntheticness_report(
    report_or_signals: SyntheticnessReport | Sequence[SyntheticnessSignal | Mapping[str, Any]],
    *,
    source_path: Path | str | None = None,
    source_kind: str | None = None,
    title: str = "Syntheticness Report",
    generated_at: datetime | None = None,
) -> str:
    """Render a markdown syntheticness report."""

    report = _ensure_report(
        report_or_signals,
        source_path=source_path,
        source_kind=source_kind,
        title=title,
        generated_at=generated_at,
    )

    lines: list[str] = [
        f"# {report.title}",
        "",
        f"- Generated at: `{report.generated_at.isoformat()}`",
    ]
    if report.source_path is not None:
        lines.append(f"- Source: `{report.source_path}`")
    if report.source_kind is not None:
        lines.append(f"- Source kind: `{report.source_kind}`")
    lines.extend(
        [
            f"- Signals: `{len(report.signals)}`",
            f"- Weak signals: `{report.counts['weak']}`",
            f"- Moderate signals: `{report.counts['moderate']}`",
            f"- Strong signals: `{report.counts['strong']}`",
            "",
            "## How To Read This",
            "",
        ]
    )

    for caveat in DEFAULT_CAVEATS:
        lines.append(f"- {caveat}")

    lines.extend(
        [
            "",
            "## Signal Summary",
            "",
        ]
    )

    if report.signals:
        lines.extend(
            [
                "| Strength | Type | Dataset | Field | Sample | Plain-English explanation |",
                "|---|---|---|---|---:|---|",
            ]
        )
        for signal in report.signals:
            lines.append(
                "| "
                + " | ".join(
                    [
                        signal.strength,
                        _escape_cell(signal.signal_type),
                        _escape_cell(signal.dataset),
                        _escape_cell(signal.field_name or "-"),
                        _escape_cell(
                            str(signal.sample_size) if signal.sample_size is not None else "-"
                        ),
                        _escape_cell(signal.plain_english),
                    ]
                )
                + " |"
            )
    else:
        lines.append("No syntheticness signals were produced for this run.")

    lines.extend(["", "## Detailed Signals", ""])

    if not report.signals:
        lines.append("No signals to detail.")
    for index, signal in enumerate(report.signals, start=1):
        lines.extend(_render_signal_detail(index, signal))

    return "\n".join(lines).rstrip() + "\n"


def write_syntheticness_report(
    report_or_signals: SyntheticnessReport | Sequence[SyntheticnessSignal | Mapping[str, Any]],
    *,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    source_path: Path | str | None = None,
    source_kind: str | None = None,
    title: str = "Syntheticness Report",
    generated_at: datetime | None = None,
) -> Path:
    """Write the syntheticness report markdown to disk."""

    markdown = render_syntheticness_report(
        report_or_signals,
        source_path=source_path,
        source_kind=source_kind,
        title=title,
        generated_at=generated_at,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown, encoding="utf-8")
    return output_path


def _render_signal_detail(index: int, signal: SyntheticnessSignal) -> list[str]:
    lines = [
        f"### {index}. {signal.title}",
        "",
        f"- Type: `{signal.signal_type}`",
        f"- Strength: `{signal.strength}`",
        f"- Dataset: `{signal.dataset}`",
    ]
    if signal.field_name is not None:
        lines.append(f"- Field: `{signal.field_name}`")
    if signal.method is not None:
        lines.append(f"- Method: `{signal.method}`")
    if signal.sample_size is not None:
        lines.append(f"- Sample size: `{signal.sample_size}`")
    if signal.score is not None:
        lines.append(f"- Score: `{signal.score}`")
    if signal.benchmark is not None:
        lines.append(f"- Benchmark: `{signal.benchmark}`")
    if signal.p_value is not None:
        lines.append(f"- p-value: `{signal.p_value}`")
    if signal.z_score is not None:
        lines.append(f"- z-score: `{signal.z_score}`")
    if signal.threshold is not None:
        lines.append(f"- Threshold: `{signal.threshold}`")
    if signal.observed is not None:
        lines.append(f"- Observed: `{signal.observed}`")
    if signal.expected is not None:
        lines.append(f"- Expected: `{signal.expected}`")
    lines.extend(
        [
            "",
            f"- Plain-English explanation: {signal.plain_english}",
        ]
    )
    if signal.interpretation:
        lines.append(f"- Interpretation: {signal.interpretation}")
    if signal.caveat:
        lines.append(f"- Caveat: {signal.caveat}")
    if signal.details:
        lines.append("")
        lines.append("```json")
        lines.append(_render_details_json(signal.details))
        lines.append("```")
    lines.append("")
    return lines


def _ensure_report(
    report_or_signals: SyntheticnessReport | Sequence[SyntheticnessSignal | Mapping[str, Any]],
    *,
    source_path: Path | str | None,
    source_kind: str | None,
    title: str,
    generated_at: datetime | None,
) -> SyntheticnessReport:
    if isinstance(report_or_signals, SyntheticnessReport):
        return report_or_signals
    return build_syntheticness_report(
        report_or_signals,
        source_path=source_path,
        source_kind=source_kind,
        title=title,
        generated_at=generated_at,
    )


def _normalize_strength(value: object) -> SyntheticnessStrength:
    text = str(value or "unknown").strip().casefold()
    if text in {"weak", "moderate", "strong", "unknown"}:
        return text  # type: ignore[return-value]
    return "unknown"


def _maybe_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _maybe_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _maybe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item") and callable(value.item):
        try:
            return value.item()
        except (TypeError, ValueError):
            return str(value)
    return value


def _render_details_json(details: Mapping[str, Any]) -> str:
    import json

    return json.dumps(_json_safe(details), ensure_ascii=False, indent=2, default=str)


def _escape_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "DEFAULT_OUTPUT_PATH",
    "DEFAULT_CAVEATS",
    "SyntheticnessReport",
    "SyntheticnessSignal",
    "build_syntheticness_report",
    "coerce_signal",
    "render_syntheticness_report",
    "write_syntheticness_report",
]
