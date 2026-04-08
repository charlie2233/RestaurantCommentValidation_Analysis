"""Offline benchmark harness for the syntheticness interpretation path."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import fmean
from typing import Any, Sequence

from qsr_audit.config import Settings
from qsr_audit.reporting.scorecards import _syntheticness_summary
from qsr_audit.validate.syntheticness_reporting import SyntheticnessSignal

BENCHMARK_ID = "syntheticness-interpretation-benchmark-v1"
BENCHMARK_SCORING_VERSION = "syntheticness-interpretation-v1"
DEFAULT_BENCHMARK_ROOT = Path("artifacts/syntheticness")


@dataclass(frozen=True)
class SyntheticnessBenchmarkCase:
    """Deterministic fixture used to exercise the interpretation layer."""

    case_id: str
    brand_name: str
    canonical_brand_name: str
    metric_name: str
    signals: tuple[SyntheticnessSignal, ...]
    expected_score: int
    expected_review_required: bool


@dataclass(frozen=True)
class SyntheticnessBenchmarkArtifacts:
    """Files written by the benchmark harness."""

    metrics_json_path: Path
    summary_markdown_path: Path


@dataclass(frozen=True)
class SyntheticnessBenchmarkRun:
    """Complete benchmark run result."""

    artifacts: SyntheticnessBenchmarkArtifacts
    metrics: dict[str, Any]
    cases: tuple[dict[str, Any], ...]


def run_syntheticness_benchmark(
    *,
    settings: Settings | None = None,
    output_root: Path | None = None,
) -> SyntheticnessBenchmarkRun:
    """Run the offline syntheticness benchmark and write summary artifacts."""

    resolved_output_root = _resolve_output_root(settings=settings, output_root=output_root)
    resolved_output_root.mkdir(parents=True, exist_ok=True)

    cases = build_syntheticness_benchmark_cases()
    case_results = tuple(_evaluate_case(case) for case in cases)
    metrics = _build_benchmark_metrics(case_results)

    artifacts = SyntheticnessBenchmarkArtifacts(
        metrics_json_path=resolved_output_root / "benchmark_metrics.json",
        summary_markdown_path=resolved_output_root / "benchmark_summary.md",
    )
    artifacts.metrics_json_path.write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    artifacts.summary_markdown_path.write_text(
        render_syntheticness_benchmark_summary(metrics),
        encoding="utf-8",
    )

    return SyntheticnessBenchmarkRun(
        artifacts=artifacts,
        metrics=metrics,
        cases=case_results,
    )


def build_syntheticness_benchmark_cases() -> tuple[SyntheticnessBenchmarkCase, ...]:
    """Return the deterministic fixture cases covered by the benchmark."""

    return (
        SyntheticnessBenchmarkCase(
            case_id="quiet",
            brand_name="McDonald's",
            canonical_brand_name="McDonald's",
            metric_name="average_unit_volume_usd_thousands",
            signals=(),
            expected_score=0,
            expected_review_required=False,
        ),
        SyntheticnessBenchmarkCase(
            case_id="weak",
            brand_name="Taco Bell",
            canonical_brand_name="Taco Bell",
            metric_name="us_store_count_2024",
            signals=(
                _signal(
                    title="Store count ends on a rounded digit",
                    plain_english="The store count is rounded, which is a weak anomaly signal only.",
                    strength="weak",
                    field_name="us_store_count_2024",
                    caveat="Rounded counts can reflect presentation conventions rather than fabrication.",
                ),
            ),
            expected_score=10,
            expected_review_required=False,
        ),
        SyntheticnessBenchmarkCase(
            case_id="moderate",
            brand_name="Raising Cane's",
            canonical_brand_name="Raising Cane's",
            metric_name="average_unit_volume_usd_thousands",
            signals=(
                _signal(
                    title="AUV cluster shows repeated rounding",
                    plain_english="Repeated rounding makes this field a moderate review signal.",
                    strength="moderate",
                    field_name="average_unit_volume_usd_thousands",
                    caveat="Moderate signals should raise review pressure, not prove fabrication.",
                ),
            ),
            expected_score=25,
            expected_review_required=True,
        ),
        SyntheticnessBenchmarkCase(
            case_id="strong",
            brand_name="Dutch Bros",
            canonical_brand_name="Dutch Bros",
            metric_name="systemwide_revenue_usd_billions_2024",
            signals=(
                _signal(
                    title="Revenue field has a strong anomaly shape",
                    plain_english="The revenue pattern is unusual enough to warrant strong review pressure.",
                    strength="strong",
                    field_name="systemwide_revenue_usd_billions_2024",
                    caveat="Strong signals should trigger review, not automated blocking by themselves.",
                ),
            ),
            expected_score=45,
            expected_review_required=True,
        ),
        SyntheticnessBenchmarkCase(
            case_id="clustered",
            brand_name="Shake Shack",
            canonical_brand_name="Shake Shack",
            metric_name="reconciliation_and_syntheticness_cluster",
            signals=(
                _signal(
                    title="Store count heaping",
                    plain_english="Rounded store counts add weak evidence of presentation bias.",
                    strength="weak",
                    field_name="us_store_count_2024",
                    caveat="Weak signals are only one input to the review picture.",
                ),
                _signal(
                    title="Sales pattern is moderate outlier evidence",
                    plain_english="The sales pattern looks moderately unusual relative to the benchmark fixture.",
                    strength="moderate",
                    field_name="systemwide_revenue_usd_billions_2024",
                    caveat="Moderate signals should increase review pressure when they cluster.",
                ),
                _signal(
                    title="AUV pattern is strongly unusual",
                    plain_english="The AUV pattern is strongly unusual when combined with the other signals.",
                    strength="strong",
                    field_name="average_unit_volume_usd_thousands",
                    caveat="Clustered evidence is the strongest review cue in this harness.",
                ),
            ),
            expected_score=90,
            expected_review_required=True,
        ),
    )


def render_syntheticness_benchmark_summary(metrics: dict[str, Any]) -> str:
    """Render a concise benchmark summary for reviewers."""

    lines = [
        "# Syntheticness Benchmark Summary",
        "",
        "This benchmark is offline, local-only, and deterministic. It exercises the current",
        "syntheticness interpretation layer on fixed fixture cases and checks whether the",
        "expected review pressure matches the observed review pressure.",
        "",
        f"- Benchmark ID: `{metrics['benchmark_id']}`",
        f"- Scoring version: `{metrics['scoring_version']}`",
        f"- Cases: `{metrics['case_count']}`",
        f"- Passed cases: `{metrics['passed_case_count']}`",
        f"- Review-required agreement: `{metrics['review_required_agreement_rate']:.0%}`",
        f"- Mean absolute score error: `{metrics['mean_absolute_score_error']:.1f}`",
        "",
        "## Case Results",
        "",
        "| Case | Brand | Metric | Expected score | Observed score | Expected review | Observed review | Result |",
        "|---|---|---|---:|---:|---|---|---|",
    ]
    for case in metrics["cases"]:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(case["case_id"]),
                    str(case["brand_name"]),
                    str(case["metric_name"]),
                    str(case["expected_score"]),
                    str(case["observed_score"]),
                    "yes" if case["expected_review_required"] else "no",
                    "yes" if case["observed_review_required"] else "no",
                    "pass" if case["passed"] else "fail",
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Interpretation Notes",
            "",
            "- Weak signals remain review cues, but they do not trigger review_required on their own.",
            "- Moderate, strong, and clustered evidence should trigger review_required deterministically.",
            "- This harness does not call hosted inference and does not download any external model.",
            "- It validates interpretation behavior only; it does not claim fabrication detection.",
        ]
    )
    return "\n".join(lines) + "\n"


def _evaluate_case(case: SyntheticnessBenchmarkCase) -> dict[str, Any]:
    summary = _syntheticness_summary([asdict(signal) for signal in case.signals])
    observed_score = int(summary["syntheticness_score"])
    observed_review_required = bool(summary["review_required"])
    supporting_signals = list(summary["supporting_signals"])
    caveats = list(summary["caveats"])
    passed = (
        observed_score == case.expected_score
        and observed_review_required == case.expected_review_required
    )
    return {
        "case_id": case.case_id,
        "brand_name": case.brand_name,
        "canonical_brand_name": case.canonical_brand_name,
        "metric_name": case.metric_name,
        "expected_score": case.expected_score,
        "observed_score": observed_score,
        "score_error": observed_score - case.expected_score,
        "expected_review_required": case.expected_review_required,
        "observed_review_required": observed_review_required,
        "supporting_signal_count": len(supporting_signals),
        "supporting_signals": supporting_signals,
        "caveats": caveats,
        "passed": passed,
    }


def _build_benchmark_metrics(case_results: Sequence[dict[str, Any]]) -> dict[str, Any]:
    case_count = len(case_results)
    passed_case_count = sum(1 for case in case_results if case["passed"])
    review_required_matches = sum(
        1
        for case in case_results
        if case["expected_review_required"] == case["observed_review_required"]
    )
    observed_scores = [int(case["observed_score"]) for case in case_results]
    expected_scores = [int(case["expected_score"]) for case in case_results]
    abs_errors = [abs(int(case["score_error"])) for case in case_results]

    metrics = {
        "benchmark_id": BENCHMARK_ID,
        "scoring_version": BENCHMARK_SCORING_VERSION,
        "case_count": case_count,
        "passed_case_count": passed_case_count,
        "failed_case_count": case_count - passed_case_count,
        "review_required_agreement_rate": review_required_matches / case_count if case_count else 0.0,
        "mean_observed_score": fmean(observed_scores) if observed_scores else 0.0,
        "mean_expected_score": fmean(expected_scores) if expected_scores else 0.0,
        "mean_absolute_score_error": fmean(abs_errors) if abs_errors else 0.0,
        "review_required_case_count": sum(
            1 for case in case_results if case["observed_review_required"]
        ),
        "quiet_case_count": sum(1 for case in case_results if case["observed_score"] == 0),
        "cases": list(case_results),
    }
    return metrics


def _resolve_output_root(
    *,
    settings: Settings | None,
    output_root: Path | None,
) -> Path:
    if output_root is not None:
        return Path(output_root).expanduser().resolve()
    if settings is not None:
        return (settings.artifacts_dir / "syntheticness").expanduser().resolve()
    return DEFAULT_BENCHMARK_ROOT.expanduser().resolve()


def _signal(
    *,
    title: str,
    plain_english: str,
    strength: str,
    field_name: str,
    caveat: str,
) -> SyntheticnessSignal:
    return SyntheticnessSignal(
        signal_type="benchmark_fixture",
        title=title,
        plain_english=plain_english,
        strength=strength,  # type: ignore[arg-type]
        dataset="core_brand_metrics",
        field_name=field_name,
        method="fixture",
        sample_size=5,
        score=None,
        benchmark=None,
        p_value=None,
        z_score=None,
        threshold=None,
        observed=None,
        expected=None,
        interpretation="benchmark fixture",
        caveat=caveat,
        details={"fixture": True, "field_name": field_name},
    )


__all__ = [
    "BENCHMARK_ID",
    "BENCHMARK_SCORING_VERSION",
    "DEFAULT_BENCHMARK_ROOT",
    "SyntheticnessBenchmarkArtifacts",
    "SyntheticnessBenchmarkCase",
    "SyntheticnessBenchmarkRun",
    "build_syntheticness_benchmark_cases",
    "render_syntheticness_benchmark_summary",
    "run_syntheticness_benchmark",
]
