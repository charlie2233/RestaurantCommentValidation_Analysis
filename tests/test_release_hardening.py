"""Security, lineage, and release-hardening regressions."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from qsr_audit.cli import app
from qsr_audit.config import Settings
from qsr_audit.governance import (
    DataClassification,
    latest_manifest_path,
    write_artifact_manifest,
)
from qsr_audit.release import preflight_release
from typer.testing import CliRunner

from tests.helpers import build_settings, write_sample_workbook


def test_validate_workbook_cli_emits_manifest_and_audit_log(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = build_settings(tmp_path)
    _set_settings_env(monkeypatch, settings)
    workbook_path = settings.data_raw / "source_workbook.xlsx"
    write_sample_workbook(workbook_path)

    runner = CliRunner()
    result = runner.invoke(app, ["validate-workbook", "--input", str(workbook_path)])

    assert result.exit_code == 0
    manifest_path = settings.artifacts_dir / "manifests" / "validate-workbook" / "latest.json"
    assert manifest_path.exists()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["command_name"] == "validate-workbook"
    assert payload["data_classification"] == DataClassification.CONFIDENTIAL.value
    assert any(path.endswith("validation_summary.md") for path in payload["output_paths"])

    audit_dir = settings.artifacts_dir / "audit_logs" / "validate-workbook"
    audit_logs = sorted(audit_dir.glob("*.json"))
    assert len(audit_logs) == 1
    audit_payload = json.loads(audit_logs[0].read_text(encoding="utf-8"))
    assert audit_payload["status"] == "success"
    assert "artifacts/manifests/validate-workbook/" in audit_payload["manifest_path"]
    assert audit_payload["manifest_path"].endswith(".json")
    assert not str(manifest_path).startswith(str(settings.reports_dir))
    assert not str(manifest_path).startswith(str(settings.strategy_dir))


def test_safe_debug_summary_redacts_secret_like_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-live-123")
    monkeypatch.setenv("HF_TOKEN", "hf-secret")
    settings = build_settings(tmp_path)

    payload = settings.safe_debug_summary()

    assert payload["secret_environment"]["OPENAI_API_KEY"] == "***REDACTED***"
    assert payload["secret_environment"]["HF_TOKEN"] == "***REDACTED***"
    serialized = json.dumps(payload, ensure_ascii=False)
    assert "sk-live-123" not in serialized
    assert "hf-secret" not in serialized


def test_settings_rejects_unsafe_artifact_root(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(ValueError, match="must not overlap"):
        Settings(
            data_raw=tmp_path / "raw",
            data_bronze=tmp_path / "bronze",
            data_silver=tmp_path / "silver",
            data_gold=tmp_path / "gold",
            data_reference=tmp_path / "reference",
            gold_history_dir=tmp_path / "gold" / "history",
            reports_dir=reports_dir,
            strategy_dir=tmp_path / "strategy",
            artifacts_dir=reports_dir / "artifacts",
        )


def test_preflight_release_fails_when_gold_artifacts_are_missing(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)

    run = preflight_release(settings=settings)

    assert run.passed is False
    assert run.artifacts.summary_json_path.exists()
    assert run.artifacts.summary_markdown_path.exists()
    failed_checks = {check.name: check for check in run.checks if check.status == "fail"}
    assert "required_gold_artifacts" in failed_checks


def test_preflight_release_fails_when_upstream_manifests_are_missing(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    _write_gold_publish_outputs(settings)

    run = preflight_release(settings=settings)

    assert run.passed is False
    failed_checks = {check.name: check for check in run.checks if check.status == "fail"}
    assert "upstream_manifests" in failed_checks


def test_preflight_release_passes_on_clean_fixture(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    _write_gold_publish_outputs(settings)
    _write_required_manifest_fixtures(settings)

    run = preflight_release(settings=settings)

    assert run.passed is True
    summary_payload = json.loads(run.artifacts.summary_json_path.read_text(encoding="utf-8"))
    assert summary_payload["passed"] is True
    assert "Gold Decision Counts" in run.artifacts.summary_markdown_path.read_text(encoding="utf-8")


def test_preflight_release_rejects_non_publishable_rows_in_publishable_artifact(
    tmp_path: Path,
) -> None:
    settings = build_settings(tmp_path)
    decisions = _write_gold_publish_outputs(settings)
    _write_required_manifest_fixtures(settings)

    bad_publishable = decisions.loc[decisions["publish_status"] != "blocked"].copy()
    bad_publishable.to_parquet(settings.data_gold / "publishable_kpis.parquet", index=False)

    run = preflight_release(settings=settings)

    assert run.passed is False
    failed_checks = {check.name: check for check in run.checks if check.status == "fail"}
    assert "gold_artifact_consistency" in failed_checks
    assert (
        "publishable_kpis.parquet contains advisory or blocked rows."
        in failed_checks["gold_artifact_consistency"].details["issues"]
    )


def _set_settings_env(monkeypatch: pytest.MonkeyPatch, settings: Settings) -> None:
    monkeypatch.setenv("QSR_DATA_RAW", str(settings.data_raw))
    monkeypatch.setenv("QSR_DATA_BRONZE", str(settings.data_bronze))
    monkeypatch.setenv("QSR_DATA_SILVER", str(settings.data_silver))
    monkeypatch.setenv("QSR_DATA_GOLD", str(settings.data_gold))
    monkeypatch.setenv("QSR_DATA_REFERENCE", str(settings.data_reference))
    monkeypatch.setenv("QSR_GOLD_HISTORY_DIR", str(settings.gold_history_dir))
    monkeypatch.setenv("QSR_REPORTS_DIR", str(settings.reports_dir))
    monkeypatch.setenv("QSR_STRATEGY_DIR", str(settings.strategy_dir))
    monkeypatch.setenv("QSR_ARTIFACTS_DIR", str(settings.artifacts_dir))


def _write_gold_publish_outputs(settings: Settings) -> pd.DataFrame:
    decisions = pd.DataFrame(
        [
            {
                "brand_name": "McDonald's",
                "canonical_brand_name": "McDonald's",
                "metric_name": "system_sales",
                "metric_value": 53.5,
                "publish_status": "publishable",
            },
            {
                "brand_name": "Domino's",
                "canonical_brand_name": "Domino's",
                "metric_name": "fte_mid",
                "metric_value": 13.5,
                "publish_status": "advisory",
            },
            {
                "brand_name": "Taco Bell",
                "canonical_brand_name": "Taco Bell",
                "metric_name": "auv",
                "metric_value": 2100.0,
                "publish_status": "blocked",
            },
        ]
    )
    decisions.to_parquet(settings.data_gold / "gold_publish_decisions.parquet", index=False)
    decisions.loc[decisions["publish_status"] == "publishable"].to_parquet(
        settings.data_gold / "publishable_kpis.parquet",
        index=False,
    )
    decisions.loc[decisions["publish_status"] == "blocked"].to_parquet(
        settings.data_gold / "blocked_kpis.parquet",
        index=False,
    )
    return decisions


def _write_required_manifest_fixtures(settings: Settings) -> None:
    validation_summary = settings.reports_dir / "validation" / "validation_summary.md"
    validation_results = settings.reports_dir / "validation" / "validation_results.json"
    validation_summary.parent.mkdir(parents=True, exist_ok=True)
    validation_summary.write_text("# Validation\n", encoding="utf-8")
    validation_results.write_text("{}", encoding="utf-8")
    pd.DataFrame([{"severity": "warning"}]).to_parquet(
        settings.data_gold / "validation_flags.parquet",
        index=False,
    )

    synthetic_report = settings.reports_dir / "validation" / "syntheticness_report.md"
    synthetic_report.write_text("# Syntheticness\n", encoding="utf-8")
    pd.DataFrame([{"signal_type": "heaping"}]).to_parquet(
        settings.data_gold / "syntheticness_signals.parquet",
        index=False,
    )

    reconciled = pd.DataFrame(
        [
            {
                "brand_name": "McDonald's",
                "canonical_brand_name": "McDonald's",
                "reference_source_count": 1,
            }
        ]
    )
    reconciled.to_parquet(settings.data_gold / "reconciled_core_metrics.parquet", index=False)
    pd.DataFrame([{"extra": json.dumps({"canonical_brand_name": "McDonald's"})}]).to_parquet(
        settings.data_gold / "provenance_registry.parquet",
        index=False,
    )
    reference = pd.DataFrame([{"canonical_brand_name": "McDonald's", "missing_metrics": "[]"}])
    reference.to_parquet(settings.data_gold / "reference_coverage.parquet", index=False)
    reconciliation_summary = settings.reports_dir / "reconciliation" / "reconciliation_summary.md"
    reference_summary = settings.reports_dir / "reference" / "reference_coverage.md"
    reconciliation_summary.parent.mkdir(parents=True, exist_ok=True)
    reference_summary.parent.mkdir(parents=True, exist_ok=True)
    reconciliation_summary.write_text("# Reconciliation\n", encoding="utf-8")
    reference_summary.write_text("# Reference Coverage\n", encoding="utf-8")

    validate_manifest = write_artifact_manifest(
        settings=settings,
        command_name="validate-workbook",
        input_paths=[settings.data_raw / "source_workbook.xlsx"],
        output_paths=[
            validation_summary,
            validation_results,
            settings.data_gold / "validation_flags.parquet",
        ],
        row_counts={"validation_findings": 1},
        data_classification=DataClassification.CONFIDENTIAL,
        intended_audience="analyst",
        publish_status_scope="working_layer_findings",
        warnings_count=1,
        errors_count=0,
    )
    write_artifact_manifest(
        settings=settings,
        command_name="run-syntheticness",
        input_paths=[settings.data_silver / "core_brand_metrics.parquet"],
        output_paths=[synthetic_report, settings.data_gold / "syntheticness_signals.parquet"],
        row_counts={"syntheticness_signals": 1},
        data_classification=DataClassification.INTERNAL,
        intended_audience="analyst",
        publish_status_scope="experimental_signals",
        warnings_count=1,
        errors_count=0,
        upstream_artifact_references=[validate_manifest],
    )
    write_artifact_manifest(
        settings=settings,
        command_name="reconcile",
        input_paths=[settings.data_silver / "core_brand_metrics.parquet", settings.data_reference],
        output_paths=[
            settings.data_gold / "reconciled_core_metrics.parquet",
            settings.data_gold / "provenance_registry.parquet",
            settings.data_gold / "reference_coverage.parquet",
            reconciliation_summary,
            reference_summary,
        ],
        row_counts={
            "reconciled_core_metrics": 1,
            "provenance_registry": 1,
            "reference_coverage": 1,
        },
        data_classification=DataClassification.CONFIDENTIAL,
        intended_audience="analyst",
        publish_status_scope="all_gold_rows",
        warnings_count=0,
        errors_count=0,
        upstream_artifact_references=[validate_manifest],
    )
    write_artifact_manifest(
        settings=settings,
        command_name="gate-gold",
        input_paths=[
            settings.data_gold / "reconciled_core_metrics.parquet",
            settings.data_gold / "provenance_registry.parquet",
            settings.data_gold / "validation_flags.parquet",
            settings.data_gold / "reference_coverage.parquet",
            settings.data_gold / "syntheticness_signals.parquet",
        ],
        output_paths=[
            settings.data_gold / "gold_publish_decisions.parquet",
            settings.data_gold / "publishable_kpis.parquet",
            settings.data_gold / "blocked_kpis.parquet",
        ],
        row_counts={
            "decision_rows": 3,
            "publishable_rows": 1,
            "advisory_rows": 1,
            "blocked_rows": 1,
        },
        data_classification=DataClassification.INTERNAL,
        intended_audience="release_manager",
        publish_status_scope="publishable_advisory_blocked",
        warnings_count=1,
        errors_count=1,
        upstream_artifact_references=[
            latest_manifest_path(settings, "validate-workbook"),
            latest_manifest_path(settings, "run-syntheticness"),
            latest_manifest_path(settings, "reconcile"),
        ],
    )
