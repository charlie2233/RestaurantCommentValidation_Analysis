"""Tests for the offline syntheticness benchmark harness."""

from __future__ import annotations

import json
from pathlib import Path

from qsr_audit.validate.syntheticness_benchmark import (
    BENCHMARK_SCORING_VERSION,
    build_syntheticness_benchmark_cases,
    render_syntheticness_benchmark_summary,
    run_syntheticness_benchmark,
)

from tests.helpers import build_settings


def test_syntheticness_benchmark_writes_expected_artifacts(tmp_path: Path) -> None:
    output_root = tmp_path / "artifacts" / "syntheticness"

    run = run_syntheticness_benchmark(output_root=output_root)

    assert run.artifacts.metrics_json_path == output_root / "benchmark_metrics.json"
    assert run.artifacts.summary_markdown_path == output_root / "benchmark_summary.md"
    assert run.artifacts.metrics_json_path.exists()
    assert run.artifacts.summary_markdown_path.exists()

    metrics = json.loads(run.artifacts.metrics_json_path.read_text(encoding="utf-8"))
    assert metrics["scoring_version"] == BENCHMARK_SCORING_VERSION
    assert metrics["case_count"] == 5
    assert metrics["passed_case_count"] == 5
    assert metrics["review_required_agreement_rate"] == 1.0
    assert [case["case_id"] for case in metrics["cases"]] == [
        "quiet",
        "weak",
        "moderate",
        "strong",
        "clustered",
    ]

    summary = run.artifacts.summary_markdown_path.read_text(encoding="utf-8")
    assert "Syntheticness Benchmark Summary" in summary
    assert "offline, local-only, and deterministic" in summary
    assert "quiet" in summary
    assert "clustered" in summary


def test_syntheticness_benchmark_defaults_to_settings_artifacts_dir(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)

    run = run_syntheticness_benchmark(settings=settings)

    assert run.artifacts.metrics_json_path == settings.artifacts_dir / "syntheticness" / (
        "benchmark_metrics.json"
    )
    assert run.artifacts.summary_markdown_path == settings.artifacts_dir / "syntheticness" / (
        "benchmark_summary.md"
    )
    assert run.artifacts.metrics_json_path.exists()
    assert run.artifacts.summary_markdown_path.exists()


def test_syntheticness_benchmark_review_threshold_is_deterministic(
    tmp_path: Path,
) -> None:
    cases = {case.case_id: case for case in build_syntheticness_benchmark_cases()}

    run = run_syntheticness_benchmark(output_root=tmp_path / "benchmark")
    observed = {case["case_id"]: case for case in run.metrics["cases"]}

    assert observed["quiet"]["observed_score"] == 0
    assert observed["quiet"]["observed_review_required"] is False
    assert observed["weak"]["observed_score"] == 10
    assert observed["weak"]["observed_review_required"] is False
    assert observed["moderate"]["observed_score"] == 25
    assert observed["moderate"]["observed_review_required"] is True
    assert observed["strong"]["observed_score"] == 45
    assert observed["strong"]["observed_review_required"] is True
    assert observed["clustered"]["observed_score"] == 90
    assert observed["clustered"]["observed_review_required"] is True

    for case_id, case in cases.items():
        assert observed[case_id]["expected_score"] == case.expected_score
        assert observed[case_id]["expected_review_required"] == case.expected_review_required


def test_syntheticness_benchmark_summary_renderer_matches_metrics(
    tmp_path: Path,
) -> None:
    run = run_syntheticness_benchmark(output_root=tmp_path / "benchmark-summary")

    summary = render_syntheticness_benchmark_summary(run.metrics)

    assert f"`{run.metrics['benchmark_id']}`" in summary
    assert f"`{run.metrics['case_count']}`" in summary
    assert "review_required" in summary
    assert "does not call hosted inference" in summary
