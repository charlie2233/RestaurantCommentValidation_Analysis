"""Tests for analyst-facing report generation and dashboard handoff."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from qsr_audit.cli import app
from qsr_audit.config import Settings
from qsr_audit.reporting import write_reports
from typer.testing import CliRunner

from dashboard.app import build_dashboard_json, load_dashboard_artifacts, render_dashboard_html


def _build_settings(tmp_path: Path) -> Settings:
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    reference_dir = tmp_path / "reference"
    reports_dir = tmp_path / "reports"
    strategy_dir = tmp_path / "strategy"
    for directory in [
        raw_dir,
        bronze_dir,
        silver_dir,
        gold_dir,
        reference_dir,
        reports_dir,
        strategy_dir,
    ]:
        directory.mkdir(parents=True, exist_ok=True)

    return Settings(
        data_raw=raw_dir,
        data_bronze=bronze_dir,
        data_silver=silver_dir,
        data_gold=gold_dir,
        data_reference=reference_dir,
        reports_dir=reports_dir,
        strategy_dir=strategy_dir,
    )


def _write_reporting_inputs(settings: Settings) -> None:
    reconciled = pd.DataFrame(
        [
            {
                "brand_name": "McDonalds",
                "canonical_brand_name": "McDonald's",
                "rank": 1,
                "category": "Burger",
                "ownership_model": "Franchise",
                "us_store_count_2024": 13559,
                "systemwide_revenue_usd_billions_2024": 53.5,
                "average_unit_volume_usd_thousands": 4001,
                "fte_mid": 30.0,
                "margin_mid_pct": 19.0,
                "brand_match_confidence": 1.0,
                "rank_reference_confidence_score": 0.95,
                "store_count_reference_confidence_score": 0.94,
                "system_sales_reference_confidence_score": 0.93,
                "auv_reference_confidence_score": 0.92,
                "rank_reference_value": 1,
                "rank_absolute_error": 0,
                "rank_relative_error": 0.0,
                "rank_credibility_grade": "A",
                "rank_reference_source_name": "QSR 50",
                "store_count_reference_value": 13559,
                "store_count_absolute_error": 0,
                "store_count_relative_error": 0.0,
                "store_count_credibility_grade": "A",
                "store_count_reference_source_name": "QSR 50",
                "system_sales_reference_value": 53.5,
                "system_sales_absolute_error": 0.0,
                "system_sales_relative_error": 0.0,
                "system_sales_credibility_grade": "A",
                "system_sales_reference_source_name": "QSR 50",
                "auv_reference_value": 4001,
                "auv_absolute_error": 0,
                "auv_relative_error": 0.0,
                "auv_credibility_grade": "A",
                "auv_reference_source_name": "QSR 50",
                "overall_credibility_grade": "A",
                "reconciliation_warning": None,
            },
            {
                "brand_name": "Taco Bell",
                "canonical_brand_name": "Taco Bell",
                "rank": 2,
                "category": "Mexican",
                "ownership_model": "Franchise",
                "us_store_count_2024": 7604,
                "systemwide_revenue_usd_billions_2024": 15.0,
                "average_unit_volume_usd_thousands": 2100,
                "fte_mid": 17.5,
                "margin_mid_pct": 20.0,
                "brand_match_confidence": 1.0,
                "rank_reference_confidence_score": None,
                "store_count_reference_confidence_score": None,
                "system_sales_reference_confidence_score": None,
                "auv_reference_confidence_score": None,
                "rank_reference_value": None,
                "rank_absolute_error": None,
                "rank_relative_error": None,
                "rank_credibility_grade": "MISSING",
                "rank_reference_source_name": None,
                "store_count_reference_value": None,
                "store_count_absolute_error": None,
                "store_count_relative_error": None,
                "store_count_credibility_grade": "MISSING",
                "store_count_reference_source_name": None,
                "system_sales_reference_value": 14.0,
                "system_sales_absolute_error": 1.0,
                "system_sales_relative_error": 0.0714,
                "system_sales_credibility_grade": "D",
                "system_sales_reference_source_name": "Technomic",
                "auv_reference_value": 2050,
                "auv_absolute_error": 50.0,
                "auv_relative_error": 0.0244,
                "auv_credibility_grade": "C",
                "auv_reference_source_name": "Technomic",
                "overall_credibility_grade": "D",
                "reconciliation_warning": "No reference coverage found for `Taco Bell`.",
            },
        ]
    )
    reconciled.to_parquet(settings.data_gold / "reconciled_core_metrics.parquet", index=False)

    pd.DataFrame(
        [
            {
                "source_type": "workbook",
                "source_name": "Workbook",
                "source_url_or_doc_id": "local",
                "as_of_date": "2026-04-04",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.35,
                "notes": "Hypothesis artifact",
                "extra": json.dumps({"canonical_brand_name": "Taco Bell"}),
            },
            {
                "source_type": "qsr50",
                "source_name": "QSR 50",
                "source_url_or_doc_id": "doc-1",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.95,
                "notes": "Manual reference",
                "extra": json.dumps({"canonical_brand_name": "McDonald's"}),
            },
        ]
    ).to_parquet(settings.data_gold / "provenance_registry.parquet", index=False)

    pd.DataFrame(
        [
            {
                "signal_type": "heaping",
                "title": "Taco Bell store count ends on a rounded digit",
                "plain_english": "Taco Bell uses a rounded store count, which is a weak anomaly signal only.",
                "strength": "weak",
                "dataset": "core_brand_metrics",
                "field_name": "us_store_count_2024",
                "method": "end_digit",
                "sample_size": 2,
                "score": 0.8,
                "benchmark": None,
                "p_value": None,
                "z_score": None,
                "threshold": None,
                "observed": None,
                "expected": None,
                "interpretation": "Rounded values deserve manual review.",
                "caveat": "Tiny sample.",
                "details": json.dumps({"brand_name": "Taco Bell"}),
            }
        ]
    ).to_parquet(settings.data_gold / "syntheticness_signals.parquet", index=False)

    pd.DataFrame(
        [
            {
                "severity": "error",
                "category": "arithmetic_invariant",
                "check_name": "implied_auv_k",
                "message": "Taco Bell implied AUV differs materially from the recorded AUV.",
                "brand_name": "Taco Bell",
                "field_name": "average_unit_volume_usd_thousands",
                "row_number": 2,
            }
        ]
    ).to_parquet(settings.data_gold / "validation_flags.parquet", index=False)

    validation_dir = settings.reports_dir / "validation"
    validation_dir.mkdir(parents=True, exist_ok=True)
    (validation_dir / "validation_results.json").write_text(
        json.dumps(
            {
                "passed": False,
                "counts": {"error": 1, "warning": 1, "info": 4},
                "findings": [
                    {
                        "severity": "error",
                        "category": "arithmetic_invariant",
                        "check_name": "implied_auv_k",
                        "message": "Taco Bell implied AUV differs materially from the recorded AUV.",
                        "brand_name": "Taco Bell",
                        "field_name": "average_unit_volume_usd_thousands",
                    },
                    {
                        "severity": "warning",
                        "category": "brand_alignment",
                        "check_name": "brand_alignment.extra_ai_brands",
                        "message": "AI sheet contains Sweetgreen, which is outside the Top 30 core table.",
                        "brand_name": None,
                        "field_name": None,
                    },
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def test_write_reports_writes_global_and_brand_outputs(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    _write_reporting_inputs(settings)

    artifacts = write_reports(settings.reports_dir, settings=settings)

    assert artifacts.global_markdown.exists()
    assert artifacts.global_html.exists()
    assert artifacts.global_json.exists()
    assert "taco-bell" in artifacts.brand_markdown_paths
    assert artifacts.brand_html_paths["taco-bell"].exists()
    assert artifacts.brand_json_paths["taco-bell"].exists()

    payload = json.loads(artifacts.global_json.read_text(encoding="utf-8"))
    assert payload["global_scorecard"]["total_brands"] == 2
    assert payload["global_scorecard"]["validation_counts"]["error"] == 1
    assert payload["global_scorecard"]["validation_failed_brands"] == 1
    assert len(payload["brand_scorecards"]) == 2

    global_markdown = artifacts.global_markdown.read_text(encoding="utf-8")
    assert "Global Credibility Scorecard" in global_markdown
    assert "Fields With Weakest Provenance" in global_markdown

    taco_markdown = artifacts.brand_markdown_paths["taco-bell"].read_text(encoding="utf-8")
    assert "Brand Scorecard: Taco Bell" in taco_markdown
    assert "Taco Bell implied AUV differs materially" in taco_markdown


def test_dashboard_prefers_generated_report_outputs(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    _write_reporting_inputs(settings)
    write_reports(settings.reports_dir, settings=settings)

    artifacts = load_dashboard_artifacts(settings.reports_dir)
    snapshot = build_dashboard_json(artifacts)
    html = render_dashboard_html(artifacts)

    assert snapshot["global_scorecard"]["total_brands"] == 2
    assert "QSR Workbook Credibility Scorecard" in html
    assert "Brand-Level Overview" in html


def test_cli_report_writes_outputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _build_settings(tmp_path)
    _write_reporting_inputs(settings)

    monkeypatch.setenv("QSR_DATA_RAW", str(settings.data_raw))
    monkeypatch.setenv("QSR_DATA_BRONZE", str(settings.data_bronze))
    monkeypatch.setenv("QSR_DATA_SILVER", str(settings.data_silver))
    monkeypatch.setenv("QSR_DATA_GOLD", str(settings.data_gold))
    monkeypatch.setenv("QSR_DATA_REFERENCE", str(settings.data_reference))
    monkeypatch.setenv("QSR_REPORTS_DIR", str(settings.reports_dir))
    monkeypatch.setenv("QSR_STRATEGY_DIR", str(tmp_path / "strategy"))

    runner = CliRunner()
    result = runner.invoke(app, ["report", "--output", str(settings.reports_dir)])

    assert result.exit_code == 0
    assert "Analyst reports generated" in result.stdout
    assert (settings.reports_dir / "index.md").exists()
    assert (settings.reports_dir / "index.html").exists()
    assert (settings.reports_dir / "index.json").exists()
    assert (settings.reports_dir / "brands" / "taco-bell.md").exists()
    assert (settings.strategy_dir / "recommendations.parquet").exists()
    assert (settings.strategy_dir / "recommendations.json").exists()
    assert (settings.reports_dir / "strategy" / "strategy_playbook.md").exists()
    assert (settings.reports_dir / "strategy" / "recommendations.json").exists()
