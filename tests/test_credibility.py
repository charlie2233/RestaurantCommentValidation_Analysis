"""Tests for the calibrated credibility engine."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from qsr_audit.cli import app
from qsr_audit.credibility import REQUIRED_COLUMNS, SCORING_VERSION, score_credibility
from qsr_audit.gold import gate_gold_publish
from typer.testing import CliRunner

from tests.test_demo_happy_path import _set_settings_env
from tests.test_gold_gating import _write_gate_inputs


def _build_credibility_fixture(
    tmp_path: Path,
    *,
    include_auv_mismatch: bool = True,
    include_synthetic_signal: bool = True,
    synthetic_brand_name: str = "Taco Bell",
):
    _gold_dir, settings = _write_gate_inputs(
        tmp_path,
        include_auv_mismatch=include_auv_mismatch,
        include_synthetic_signal=include_synthetic_signal,
        synthetic_brand_name=synthetic_brand_name,
    )
    gate_run = gate_gold_publish(settings=settings)
    return settings, gate_run


def test_credibility_rollup_schema_is_stable(tmp_path: Path) -> None:
    settings, _gate_run = _build_credibility_fixture(tmp_path)

    run = score_credibility(settings=settings)

    assert list(run.rollup.columns) == REQUIRED_COLUMNS
    assert run.artifacts.rollup_parquet_path.exists()
    persisted = pd.read_parquet(run.artifacts.rollup_parquet_path)
    assert list(persisted.columns) == REQUIRED_COLUMNS
    assert set(persisted["scoring_version"]) == {SCORING_VERSION}

    sample_row = persisted.iloc[0].to_dict()
    assert isinstance(json.loads(sample_row["supporting_signals"]), list)
    assert isinstance(json.loads(sample_row["caveats"]), list)


def test_review_required_fires_on_controlled_high_risk_fixture_rows(tmp_path: Path) -> None:
    settings, _gate_run = _build_credibility_fixture(tmp_path, include_auv_mismatch=True)

    run = score_credibility(settings=settings)
    taco_bell_auv = run.rollup.loc[
        run.rollup["canonical_brand_name"].eq("Taco Bell") & run.rollup["metric_name"].eq("auv")
    ].iloc[0]

    assert bool(taco_bell_auv["review_required"]) is True
    assert taco_bell_auv["invariant_status"] == "failed"
    assert int(taco_bell_auv["credibility_score"]) < 50

    supporting_signals = json.loads(taco_bell_auv["supporting_signals"])
    assert any("implied AUV" in signal["message"] for signal in supporting_signals)
    assert any("Reconciliation grade" in signal["message"] for signal in supporting_signals)


def test_syntheticness_only_evidence_does_not_silently_auto_block_rows(tmp_path: Path) -> None:
    settings, gate_run = _build_credibility_fixture(
        tmp_path,
        include_auv_mismatch=False,
        include_synthetic_signal=True,
        synthetic_brand_name="McDonald's",
    )

    run = score_credibility(settings=settings)
    gate_row = gate_run.decisions.loc[
        gate_run.decisions["canonical_brand_name"].eq("McDonald's")
        & gate_run.decisions["metric_name"].eq("auv")
    ].iloc[0]
    rollup_row = run.rollup.loc[
        run.rollup["canonical_brand_name"].eq("McDonald's") & run.rollup["metric_name"].eq("auv")
    ].iloc[0]

    assert gate_row["publish_status"] != "blocked"
    assert rollup_row["publish_status"] == gate_row["publish_status"]
    assert bool(rollup_row["review_required"]) is True
    assert int(rollup_row["syntheticness_score"]) >= 25


def test_credibility_threshold_logic_is_deterministic_on_fixtures(tmp_path: Path) -> None:
    settings, _gate_run = _build_credibility_fixture(tmp_path)

    first = score_credibility(settings=settings).rollup
    second = score_credibility(settings=settings).rollup

    pd.testing.assert_frame_equal(first, second)


def test_score_credibility_generates_benchmark_artifacts(tmp_path: Path) -> None:
    settings, _gate_run = _build_credibility_fixture(tmp_path)

    run = score_credibility(settings=settings)

    assert run.artifacts.benchmark_metrics_json_path.exists()
    assert run.artifacts.benchmark_summary_markdown_path.exists()

    metrics = json.loads(run.artifacts.benchmark_metrics_json_path.read_text(encoding="utf-8"))
    assert metrics["case_count"] == 5
    assert metrics["passed_case_count"] == 5
    summary_text = run.artifacts.benchmark_summary_markdown_path.read_text(encoding="utf-8")
    assert "Syntheticness Benchmark Summary" in summary_text
    assert "offline, local-only, and deterministic" in summary_text


def test_score_credibility_cli_emits_expected_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings, _gate_run = _build_credibility_fixture(tmp_path)
    _set_settings_env(monkeypatch, settings)

    runner = CliRunner()
    result = runner.invoke(app, ["score-credibility"])

    assert result.exit_code == 0, result.stdout
    assert "Credibility scoring complete" in result.stdout
    assert (settings.data_gold / "credibility_rollup.parquet").exists()
    assert (settings.reports_dir / "summary" / "credibility_scorecard.html").exists()
    assert (settings.reports_dir / "summary" / "credibility_method.md").exists()
    assert (settings.artifacts_dir / "syntheticness" / "benchmark_metrics.json").exists()
    assert (settings.artifacts_dir / "syntheticness" / "benchmark_summary.md").exists()
