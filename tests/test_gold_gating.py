"""Tests for Gold publishing gates and scorecards."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from qsr_audit.cli import app
from qsr_audit.gold import gate_gold_publish
from typer.testing import CliRunner

from tests.helpers import build_settings


def _write_gate_inputs(
    tmp_path: Path,
    *,
    include_auv_mismatch: bool = True,
    include_orphan_ai_brand: bool = False,
    missing_mcd_provenance: bool = False,
) -> tuple[Path, object]:
    settings = build_settings(tmp_path)

    reconciled = pd.DataFrame(
        [
            {
                "brand_name": "McDonald's",
                "canonical_brand_name": "McDonald's",
                "rank": 1,
                "us_store_count_2024": 13559,
                "systemwide_revenue_usd_billions_2024": 53.5,
                "average_unit_volume_usd_thousands": 3946.0,
                "fte_mid": 30.0,
                "margin_mid_pct": 19.0,
                "reference_source_count": 1,
                "rank_reference_value": 1,
                "rank_relative_error": 0.0,
                "rank_absolute_error": 0.0,
                "rank_credibility_grade": "A",
                "rank_reference_source_name": "QSR 50",
                "rank_reference_source_type": "qsr50",
                "rank_reference_confidence_score": 0.95,
                "store_count_reference_value": 13559,
                "store_count_relative_error": 0.0,
                "store_count_absolute_error": 0.0,
                "store_count_credibility_grade": "A",
                "store_count_reference_source_name": "QSR 50",
                "store_count_reference_source_type": "qsr50",
                "store_count_reference_confidence_score": 0.95,
                "system_sales_reference_value": 53.5,
                "system_sales_relative_error": 0.0,
                "system_sales_absolute_error": 0.0,
                "system_sales_credibility_grade": "A",
                "system_sales_reference_source_name": "QSR 50",
                "system_sales_reference_source_type": "qsr50",
                "system_sales_reference_confidence_score": 0.95,
                "auv_reference_value": 3946.0,
                "auv_relative_error": 0.0,
                "auv_absolute_error": 0.0,
                "auv_credibility_grade": "A",
                "auv_reference_source_name": "QSR 50",
                "auv_reference_source_type": "qsr50",
                "auv_reference_confidence_score": 0.95,
            },
            {
                "brand_name": "Taco Bell",
                "canonical_brand_name": "Taco Bell",
                "rank": 2,
                "us_store_count_2024": 7604,
                "systemwide_revenue_usd_billions_2024": 15.0,
                "average_unit_volume_usd_thousands": 2100.0,
                "fte_mid": 17.5,
                "margin_mid_pct": 20.0,
                "reference_source_count": 1,
                "rank_reference_value": 2,
                "rank_relative_error": 0.0,
                "rank_absolute_error": 0.0,
                "rank_credibility_grade": "A",
                "rank_reference_source_name": "QSR 50",
                "rank_reference_source_type": "qsr50",
                "rank_reference_confidence_score": 0.9,
                "store_count_reference_value": 7604,
                "store_count_relative_error": 0.0,
                "store_count_absolute_error": 0.0,
                "store_count_credibility_grade": "A",
                "store_count_reference_source_name": "QSR 50",
                "store_count_reference_source_type": "qsr50",
                "store_count_reference_confidence_score": 0.9,
                "system_sales_reference_value": 15.0,
                "system_sales_relative_error": 0.0,
                "system_sales_absolute_error": 0.0,
                "system_sales_credibility_grade": "A",
                "system_sales_reference_source_name": "QSR 50",
                "system_sales_reference_source_type": "qsr50",
                "system_sales_reference_confidence_score": 0.9,
                "auv_reference_value": 1900.0 if include_auv_mismatch else 2100.0,
                "auv_relative_error": 0.105 if include_auv_mismatch else 0.0,
                "auv_absolute_error": 200.0 if include_auv_mismatch else 0.0,
                "auv_credibility_grade": "D" if include_auv_mismatch else "A",
                "auv_reference_source_name": "QSR 50",
                "auv_reference_source_type": "qsr50",
                "auv_reference_confidence_score": 0.9,
            },
        ]
    )
    reconciled.to_parquet(settings.data_gold / "reconciled_core_metrics.parquet", index=False)

    provenance_rows = [
        {
            "source_type": "workbook",
            "source_name": "fixture.xlsx",
            "source_url_or_doc_id": "local-workbook",
            "as_of_date": None,
            "method_reported_or_estimated": "reported_in_workbook",
            "confidence_score": 0.35,
            "notes": "Workbook hypothesis artifact",
            "extra": json.dumps({"canonical_brand_name": "McDonald's"}),
        },
        {
            "source_type": "workbook",
            "source_name": "fixture.xlsx",
            "source_url_or_doc_id": "local-workbook",
            "as_of_date": None,
            "method_reported_or_estimated": "reported_in_workbook",
            "confidence_score": 0.35,
            "notes": "Workbook hypothesis artifact",
            "extra": json.dumps({"canonical_brand_name": "Taco Bell"}),
        },
        {
            "source_type": "qsr50",
            "source_name": "QSR 50",
            "source_url_or_doc_id": None if missing_mcd_provenance else "doc-1",
            "as_of_date": None if missing_mcd_provenance else "2024-12-31",
            "method_reported_or_estimated": "reported",
            "confidence_score": 0.95,
            "notes": "Manual reference row",
            "extra": json.dumps({"canonical_brand_name": "McDonald's"}),
        },
        {
            "source_type": "qsr50",
            "source_name": "QSR 50",
            "source_url_or_doc_id": "doc-2",
            "as_of_date": "2024-12-31",
            "method_reported_or_estimated": "reported",
            "confidence_score": 0.9,
            "notes": "Manual reference row",
            "extra": json.dumps({"canonical_brand_name": "Taco Bell"}),
        },
    ]
    pd.DataFrame(provenance_rows).to_parquet(
        settings.data_gold / "provenance_registry.parquet",
        index=False,
    )

    validation_rows = []
    if include_auv_mismatch:
        validation_rows.append(
            {
                "severity": "error",
                "category": "arithmetic",
                "check_name": "implied_auv_k",
                "dataset": "core_brand_metrics",
                "message": "Taco Bell has implied AUV 1972.0k vs recorded 2100.0k (6.5% delta; tolerance 5%).",
                "sheet_name": "QSR Top30 核心数据",
                "field_name": "average_unit_volume_usd_thousands",
                "brand_name": "Taco Bell",
                "row_number": 3,
                "expected": None,
                "observed": None,
                "details": json.dumps({"brand_name": "Taco Bell"}),
            }
        )
    if include_orphan_ai_brand:
        validation_rows.append(
            {
                "severity": "warning",
                "category": "cross_sheet",
                "check_name": "brand_alignment.extra_ai_brands",
                "dataset": "ai_strategy_registry",
                "message": (
                    "AI strategy sheet includes brands that are not present in the Top 30 core table: Sweetgreen"
                ),
                "sheet_name": "AI策略与落地效果",
                "field_name": None,
                "brand_name": None,
                "row_number": None,
                "expected": None,
                "observed": None,
                "details": json.dumps({"extra_ai_brands": ["Sweetgreen"]}),
            }
        )
    pd.DataFrame(validation_rows).to_parquet(
        settings.data_gold / "validation_flags.parquet",
        index=False,
    )

    coverage_rows = [
        {
            "coverage_kind": "brand",
            "coverage_key": "McDonald's",
            "brand_name": "McDonald's",
            "canonical_brand_name": "McDonald's",
            "metric_name": None,
            "source_type": None,
            "is_covered": True,
            "reference_row_count": 1,
            "reference_source_count": 1,
            "covered_metrics_count": 4,
            "covered_brand_count": None,
            "missing_brand_count": None,
            "coverage_rate": 1.0,
            "missing_metrics": json.dumps([]),
            "missing_brands": None,
            "source_type_names": json.dumps(["qsr50"]),
            "provenance_completeness_score": 1.0,
            "provenance_completeness_summary": "All provenance fields populated.",
            "provenance_confidence_summary": "Average confidence 0.95 across 1 row(s); range 0.95-0.95.",
            "warning": None,
            "details": json.dumps({"source_names": ["QSR 50"]}),
        },
        {
            "coverage_kind": "brand",
            "coverage_key": "Taco Bell",
            "brand_name": "Taco Bell",
            "canonical_brand_name": "Taco Bell",
            "metric_name": None,
            "source_type": None,
            "is_covered": True,
            "reference_row_count": 1,
            "reference_source_count": 1,
            "covered_metrics_count": 4,
            "covered_brand_count": None,
            "missing_brand_count": None,
            "coverage_rate": 1.0,
            "missing_metrics": json.dumps([]),
            "missing_brands": None,
            "source_type_names": json.dumps(["qsr50"]),
            "provenance_completeness_score": 1.0,
            "provenance_completeness_summary": "All provenance fields populated.",
            "provenance_confidence_summary": "Average confidence 0.90 across 1 row(s); range 0.90-0.90.",
            "warning": None,
            "details": json.dumps({"source_names": ["QSR 50"]}),
        },
    ]
    for metric_name in ["rank", "store_count", "system_sales", "auv"]:
        coverage_rows.append(
            {
                "coverage_kind": "metric",
                "coverage_key": metric_name,
                "brand_name": None,
                "canonical_brand_name": None,
                "metric_name": metric_name,
                "source_type": None,
                "is_covered": True,
                "reference_row_count": 2,
                "reference_source_count": 1,
                "covered_metrics_count": None,
                "covered_brand_count": 2,
                "missing_brand_count": 0,
                "coverage_rate": 1.0,
                "missing_metrics": None,
                "missing_brands": json.dumps([]),
                "source_type_names": json.dumps(["qsr50"]),
                "provenance_completeness_score": None,
                "provenance_completeness_summary": None,
                "provenance_confidence_summary": None,
                "warning": None,
                "details": json.dumps({"covered_brands": ["McDonald's", "Taco Bell"]}),
            }
        )
    pd.DataFrame(coverage_rows).to_parquet(
        settings.data_gold / "reference_coverage.parquet",
        index=False,
    )

    pd.DataFrame(
        [
            {
                "signal_type": "outlier",
                "title": "Taco Bell AUV is a mild outlier",
                "plain_english": "Taco Bell AUV deserves analyst review before publication.",
                "strength": "moderate",
                "dataset": "core_brand_metrics",
                "field_name": "average_unit_volume_usd_thousands",
                "method": "iqr",
                "sample_size": 2,
                "score": 0.8,
                "benchmark": None,
                "p_value": None,
                "z_score": None,
                "threshold": None,
                "observed": None,
                "expected": None,
                "interpretation": "Analyst review recommended.",
                "caveat": "Tiny sample.",
                "details": json.dumps({"brand_name": "Taco Bell"}),
            }
        ]
    ).to_parquet(settings.data_gold / "syntheticness_signals.parquet", index=False)

    return settings.data_gold, settings


def test_gate_gold_blocks_auv_when_invariant_fails(tmp_path: Path) -> None:
    _, settings = _write_gate_inputs(tmp_path, include_auv_mismatch=True)

    run = gate_gold_publish(settings=settings)

    decisions = run.decisions
    taco_auv = decisions.loc[
        (decisions["canonical_brand_name"] == "Taco Bell") & (decisions["metric_name"] == "auv")
    ].iloc[0]
    assert taco_auv["publish_status"] == "blocked"
    assert any("implied AUV" in reason for reason in taco_auv["blocking_reasons"])


def test_gate_gold_flags_orphan_strategy_brands_in_scorecard(tmp_path: Path) -> None:
    _, settings = _write_gate_inputs(tmp_path, include_orphan_ai_brand=True)

    run = gate_gold_publish(settings=settings)

    markdown = run.artifacts.scorecard_markdown_path.read_text(encoding="utf-8")
    summary = json.loads(run.artifacts.summary_json_path.read_text(encoding="utf-8"))
    assert "Orphan AI rows not present in the core workbook" in markdown
    assert "Sweetgreen" in markdown
    assert summary["workbook_highlights"]["orphan_ai_brands"] == ["Sweetgreen"]


def test_gate_gold_makes_clean_external_row_publishable(tmp_path: Path) -> None:
    _, settings = _write_gate_inputs(tmp_path, include_auv_mismatch=False)

    run = gate_gold_publish(settings=settings)

    decisions = run.decisions
    mcd_rows = decisions.loc[decisions["canonical_brand_name"] == "McDonald's"]
    publishable_metrics = sorted(
        mcd_rows.loc[mcd_rows["publish_status"] == "publishable", "metric_name"].tolist()
    )
    assert publishable_metrics == ["auv", "rank", "store_count", "system_sales"]


def test_gate_gold_blocks_missing_provenance_fields(tmp_path: Path) -> None:
    _, settings = _write_gate_inputs(
        tmp_path, missing_mcd_provenance=True, include_auv_mismatch=False
    )

    run = gate_gold_publish(settings=settings)

    decision = run.decisions.loc[
        (run.decisions["canonical_brand_name"] == "McDonald's")
        & (run.decisions["metric_name"] == "store_count")
    ].iloc[0]
    assert decision["publish_status"] == "blocked"
    assert any(
        "Required provenance fields are missing" in reason
        for reason in decision["blocking_reasons"]
    )


def test_gate_gold_keeps_estimated_operational_metrics_advisory(tmp_path: Path) -> None:
    _, settings = _write_gate_inputs(tmp_path, include_auv_mismatch=False)

    run = gate_gold_publish(settings=settings)

    decisions = run.decisions
    operational_rows = decisions.loc[
        (decisions["canonical_brand_name"] == "McDonald's")
        & (decisions["metric_name"].isin(["fte_mid", "margin_mid_pct"]))
    ]
    assert set(operational_rows["publish_status"]) == {"advisory"}
    assert set(operational_rows["source_type"]) == {"workbook"}
    assert (
        operational_rows["warning_reasons"]
        .map(lambda reasons: any("advisory-only" in reason for reason in reasons))
        .all()
    )


def test_gold_publish_scorecard_is_deterministic_and_has_expected_sections(tmp_path: Path) -> None:
    _, settings = _write_gate_inputs(
        tmp_path, include_auv_mismatch=True, include_orphan_ai_brand=True
    )

    first_run = gate_gold_publish(settings=settings)
    first_markdown = first_run.artifacts.scorecard_markdown_path.read_text(encoding="utf-8")
    second_run = gate_gold_publish(settings=settings)
    second_markdown = second_run.artifacts.scorecard_markdown_path.read_text(encoding="utf-8")

    assert first_markdown == second_markdown
    for heading in [
        "Gold Publish Scorecard",
        "Block Reasons By Frequency",
        "Warning Reasons By Frequency",
        "Brand-Level Readiness Summary",
        "Metric-Level Readiness Summary",
        "Metrics With No External Evidence",
        "Workbook-Specific Highlights",
    ]:
        assert heading in first_markdown


def test_cli_gate_gold_writes_outputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, settings = _write_gate_inputs(
        tmp_path, include_auv_mismatch=True, include_orphan_ai_brand=True
    )

    monkeypatch.setenv("QSR_DATA_RAW", str(settings.data_raw))
    monkeypatch.setenv("QSR_DATA_BRONZE", str(settings.data_bronze))
    monkeypatch.setenv("QSR_DATA_SILVER", str(settings.data_silver))
    monkeypatch.setenv("QSR_DATA_GOLD", str(settings.data_gold))
    monkeypatch.setenv("QSR_DATA_REFERENCE", str(settings.data_reference))
    monkeypatch.setenv("QSR_REPORTS_DIR", str(settings.reports_dir))
    monkeypatch.setenv("QSR_STRATEGY_DIR", str(settings.strategy_dir))

    runner = CliRunner()
    result = runner.invoke(app, ["gate-gold"])

    assert result.exit_code == 0
    assert "Gold publishing gates complete" in result.stdout
    assert (settings.data_gold / "gold_publish_decisions.parquet").exists()
    assert (settings.data_gold / "publishable_kpis.parquet").exists()
    assert (settings.data_gold / "blocked_kpis.parquet").exists()
    assert (settings.reports_dir / "audit" / "gold_publish_scorecard.md").exists()
    assert (settings.reports_dir / "audit" / "gold_publish_scorecard.json").exists()
