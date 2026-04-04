"""End-to-end local workflow smoke test."""

from __future__ import annotations

import pytest
from qsr_audit.cli import app
from typer.testing import CliRunner

from tests.helpers import build_settings, write_sample_reference_csv, write_sample_workbook


def test_cli_end_to_end_workflow(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = build_settings(tmp_path)
    workbook_path = settings.data_raw / "workflow_fixture.xlsx"
    write_sample_workbook(workbook_path)
    write_sample_reference_csv(settings.data_reference / "qsr50_reference.csv")

    monkeypatch.setenv("QSR_DATA_RAW", str(settings.data_raw))
    monkeypatch.setenv("QSR_DATA_BRONZE", str(settings.data_bronze))
    monkeypatch.setenv("QSR_DATA_SILVER", str(settings.data_silver))
    monkeypatch.setenv("QSR_DATA_GOLD", str(settings.data_gold))
    monkeypatch.setenv("QSR_DATA_REFERENCE", str(settings.data_reference))
    monkeypatch.setenv("QSR_REPORTS_DIR", str(settings.reports_dir))
    monkeypatch.setenv("QSR_STRATEGY_DIR", str(settings.strategy_dir))

    runner = CliRunner()

    result = runner.invoke(app, ["ingest-workbook", "--input", str(workbook_path)])
    assert result.exit_code == 0

    result = runner.invoke(
        app,
        ["validate-workbook", "--input", str(settings.data_silver), "--tolerance-auv", "0.05"],
    )
    assert result.exit_code == 0

    result = runner.invoke(
        app,
        [
            "run-syntheticness",
            "--input",
            str(settings.data_silver / "core_brand_metrics.parquet"),
        ],
    )
    assert result.exit_code == 0

    result = runner.invoke(
        app,
        [
            "reconcile",
            "--core",
            str(settings.data_silver / "core_brand_metrics.parquet"),
            "--reference-dir",
            str(settings.data_reference),
        ],
    )
    assert result.exit_code == 0

    result = runner.invoke(app, ["report", "--output", str(settings.reports_dir)])
    assert result.exit_code == 0

    assert (settings.data_bronze / "qsr_top30_core_data_raw.parquet").exists()
    assert (settings.data_silver / "core_brand_metrics.parquet").exists()
    assert (settings.data_gold / "validation_flags.parquet").exists()
    assert (settings.data_gold / "syntheticness_signals.parquet").exists()
    assert (settings.data_gold / "reconciled_core_metrics.parquet").exists()
    assert (settings.reports_dir / "index.md").exists()
    assert (settings.reports_dir / "strategy" / "strategy_playbook.md").exists()
    assert (settings.strategy_dir / "recommendations.parquet").exists()
