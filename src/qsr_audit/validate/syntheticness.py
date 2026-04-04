"""Syntheticness signal orchestration for normalized core brand metrics."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.config import Settings
from qsr_audit.contracts.workbook import CORE_BRAND_METRICS_SHEET, SILVER_OUTPUT_FILES
from qsr_audit.ingest import load_workbook_sheets
from qsr_audit.normalize import normalize_core_brand_metrics
from qsr_audit.validate.syntheticness_anomalies import (
    analyze_isolation_forest,
    analyze_univariate_outliers,
    derive_metric_frame,
)
from qsr_audit.validate.syntheticness_reporting import (
    SyntheticnessReport,
    SyntheticnessSignal,
    build_syntheticness_report,
    write_syntheticness_report,
)
from qsr_audit.validate.syntheticness_stats import (
    BENFORD_MIN_ORDER_SPAN,
    analyze_correlation_sanity,
    analyze_end_digit_heaping,
    analyze_first_digit_benford,
    analyze_first_two_digit_benford,
    analyze_nice_number_spikes,
)


@dataclass(frozen=True)
class SyntheticnessArtifacts:
    """Paths produced by the syntheticness workflow."""

    report_markdown: Path
    signals_parquet: Path


@dataclass(frozen=True)
class SyntheticnessRun:
    """Complete syntheticness run result."""

    source_path: Path
    source_kind: str
    report: SyntheticnessReport
    artifacts: SyntheticnessArtifacts

    @property
    def counts(self) -> dict[str, int]:
        return self.report.counts


def run_syntheticness(
    input_path: Path,
    settings: Settings | None = None,
    *,
    output_dir: Path | None = None,
    gold_dir: Path | None = None,
    include_isolation_forest: bool = True,
) -> SyntheticnessRun:
    """Run syntheticness diagnostics against normalized core metrics."""

    resolved_source = input_path.expanduser().resolve()
    source_kind, core_brand_metrics = load_core_metrics_for_syntheticness(resolved_source)

    resolved_output_dir = output_dir
    resolved_gold_dir = gold_dir
    if settings is not None:
        resolved_output_dir = resolved_output_dir or settings.reports_dir / "validation"
        resolved_gold_dir = resolved_gold_dir or settings.data_gold
    resolved_output_dir = resolved_output_dir or Path("reports/validation")
    resolved_gold_dir = resolved_gold_dir or Path("data/gold")

    signals = analyze_syntheticness_signals(
        core_brand_metrics, include_isolation_forest=include_isolation_forest
    )
    report = build_syntheticness_report(
        signals,
        source_path=resolved_source,
        source_kind=source_kind,
        title="Syntheticness Report",
    )
    artifacts = write_syntheticness_outputs(
        report,
        output_dir=resolved_output_dir,
        gold_dir=resolved_gold_dir,
    )
    return SyntheticnessRun(
        source_path=resolved_source,
        source_kind=source_kind,
        report=report,
        artifacts=artifacts,
    )


def load_core_metrics_for_syntheticness(source_path: Path) -> tuple[str, pd.DataFrame]:
    """Load normalized core metrics from a workbook, Silver directory, or parquet file."""

    if source_path.is_file():
        suffix = source_path.suffix.lower()
        if suffix in {".xlsx", ".xlsm", ".xls"}:
            raw_sheets = load_workbook_sheets(source_path)
            return "raw_workbook", normalize_core_brand_metrics(
                raw_sheets[CORE_BRAND_METRICS_SHEET]
            )
        if suffix == ".parquet":
            return "core_metrics_parquet", pd.read_parquet(source_path)
        raise ValueError(f"Unsupported syntheticness input: {source_path}")

    if source_path.is_dir():
        parquet_path = _resolve_core_metrics_path(source_path)
        return "silver_directory", pd.read_parquet(parquet_path)

    raise FileNotFoundError(f"Syntheticness input does not exist: {source_path}")


def analyze_syntheticness_signals(
    core_brand_metrics: pd.DataFrame,
    *,
    include_isolation_forest: bool,
) -> list[SyntheticnessSignal]:
    """Build syntheticness signals for the core metrics table."""

    signals: list[SyntheticnessSignal] = []
    implied_auv_k = (
        core_brand_metrics["systemwide_revenue_usd_billions_2024"].astype(float)
        * 1_000_000
        / core_brand_metrics["us_store_count_2024"].astype(float)
    )

    benford_fields = [
        (
            "us_store_count_2024",
            core_brand_metrics["us_store_count_2024"],
            "Store counts are bounded by market footprint and are commonly rounded.",
        ),
        (
            "systemwide_revenue_usd_billions_2024",
            core_brand_metrics["systemwide_revenue_usd_billions_2024"],
            "System sales are business estimates on a narrow scale, not an ideal Benford population.",
        ),
        (
            "average_unit_volume_usd_thousands",
            core_brand_metrics["average_unit_volume_usd_thousands"],
            "Recorded AUV values are usually curated and rounded for presentations.",
        ),
        (
            "implied_auv_k",
            implied_auv_k,
            "Implied AUV is a formula-driven derived metric, so Benford is descriptive rather than diagnostic here.",
        ),
    ]
    for field_name, values, caveat in benford_fields:
        order_span = _order_of_magnitude_span(values)
        benford_caveat = caveat
        if order_span < BENFORD_MIN_ORDER_SPAN:
            benford_caveat = f"{caveat} The values span only {order_span:.1f}x, so Benford is especially fragile."
        signals.append(
            analyze_first_digit_benford(values, field_name=field_name, caveat=benford_caveat)
        )
        signals.append(
            analyze_first_two_digit_benford(values, field_name=field_name, caveat=benford_caveat)
        )

    heaping_fields = [
        (
            "us_store_count_2024",
            core_brand_metrics["us_store_count_2024"],
            "Store counts often arrive as tidy franchise-count estimates.",
        ),
        (
            "average_unit_volume_usd_thousands",
            core_brand_metrics["average_unit_volume_usd_thousands"],
            "AUV values are often rounded before they reach strategy slides.",
        ),
    ]
    for field_name, values, caveat in heaping_fields:
        signals.append(analyze_end_digit_heaping(values, field_name=field_name, caveat=caveat))
        signals.append(analyze_nice_number_spikes(values, field_name=field_name, caveat=caveat))

    derived = derive_metric_frame(core_brand_metrics)
    correlation_frame = derived.frame.loc[
        :,
        [
            "recorded_auv_k",
            "implied_auv_k",
            "fte_mid",
            "margin_mid_pct",
            "store_count",
            "system_sales_b",
        ],
    ]
    signals.extend(analyze_correlation_sanity(correlation_frame))
    signals.extend(analyze_univariate_outliers(core_brand_metrics))
    if include_isolation_forest:
        signals.extend(analyze_isolation_forest(core_brand_metrics))

    return signals


def write_syntheticness_outputs(
    report: SyntheticnessReport,
    *,
    output_dir: Path,
    gold_dir: Path,
) -> SyntheticnessArtifacts:
    """Write markdown and parquet syntheticness artifacts."""

    output_dir.mkdir(parents=True, exist_ok=True)
    gold_dir.mkdir(parents=True, exist_ok=True)

    report_path = write_syntheticness_report(
        report,
        output_path=output_dir / "syntheticness_report.md",
    )
    parquet_path = gold_dir / "syntheticness_signals.parquet"
    _write_signals_parquet(report.signals, parquet_path)
    return SyntheticnessArtifacts(report_markdown=report_path, signals_parquet=parquet_path)


def _write_signals_parquet(signals: tuple[SyntheticnessSignal, ...], path: Path) -> None:
    records = []
    for signal in signals:
        records.append(
            {
                "signal_type": signal.signal_type,
                "title": signal.title,
                "plain_english": signal.plain_english,
                "strength": signal.strength,
                "dataset": signal.dataset,
                "field_name": signal.field_name,
                "method": signal.method,
                "sample_size": signal.sample_size,
                "score": signal.score,
                "benchmark": signal.benchmark,
                "p_value": signal.p_value,
                "z_score": signal.z_score,
                "threshold": signal.threshold,
                "observed": signal.observed,
                "expected": signal.expected,
                "interpretation": signal.interpretation,
                "caveat": signal.caveat,
                "details": json.dumps(_json_safe(signal.details), ensure_ascii=False, default=str),
            }
        )
    frame = pd.DataFrame(records)
    if frame.empty:
        frame = pd.DataFrame(
            columns=[
                "signal_type",
                "title",
                "plain_english",
                "strength",
                "dataset",
                "field_name",
                "method",
                "sample_size",
                "score",
                "benchmark",
                "p_value",
                "z_score",
                "threshold",
                "observed",
                "expected",
                "interpretation",
                "caveat",
                "details",
            ]
        )
    frame.to_parquet(path, index=False)


def _resolve_core_metrics_path(source_path: Path) -> Path:
    direct = source_path / SILVER_OUTPUT_FILES["core_brand_metrics"]
    if direct.exists():
        return direct
    nested = source_path / "silver" / SILVER_OUTPUT_FILES["core_brand_metrics"]
    if nested.exists():
        return nested
    raise FileNotFoundError(
        f"Could not locate {SILVER_OUTPUT_FILES['core_brand_metrics']} under {source_path}."
    )


def _order_of_magnitude_span(values: pd.Series) -> float:
    positive = pd.to_numeric(values, errors="coerce")
    positive = positive[(positive.notna()) & (positive > 0)]
    if positive.empty:
        return 0.0
    minimum = float(positive.min())
    maximum = float(positive.max())
    if minimum <= 0:
        return 0.0
    return maximum / minimum


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item") and callable(value.item):
        try:
            return value.item()
        except (TypeError, ValueError):
            return str(value)
    return value


__all__ = [
    "SyntheticnessArtifacts",
    "SyntheticnessRun",
    "analyze_syntheticness_signals",
    "load_core_metrics_for_syntheticness",
    "run_syntheticness",
    "write_syntheticness_outputs",
]
