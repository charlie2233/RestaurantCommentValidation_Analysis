"""Tests for the normalized workbook validation core."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from qsr_audit.cli import app
from qsr_audit.config import Settings
from qsr_audit.ingest import ingest_workbook
from qsr_audit.validate import validate_workbook
from typer.testing import CliRunner


def _build_settings(tmp_path: Path) -> Settings:
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    reference_dir = tmp_path / "reference"
    reports_dir = tmp_path / "reports"
    for directory in [raw_dir, bronze_dir, silver_dir, gold_dir, reference_dir, reports_dir]:
        directory.mkdir(parents=True, exist_ok=True)

    return Settings(
        data_raw=raw_dir,
        data_bronze=bronze_dir,
        data_silver=silver_dir,
        data_gold=gold_dir,
        data_reference=reference_dir,
        reports_dir=reports_dir,
    )


def _write_validation_fixture_workbook(path: Path, *, include_extra_ai_brand: bool = False) -> None:
    core_brand_metrics = pd.DataFrame(
        [
            {
                "排名": 1,
                "品牌": "McDonalds",
                "品类": "汉堡",
                "美国门店数\n(2024)": 100,
                "全系统营收\n($B, 2024)": 1.2,
                "店均AUV\n($K)": 12000,
                "店均日等效FTE\n(估算)": "25-35",
                "门店利润率\n(估算)": "18-20%",
                "央厨/供应链模式": "供应商冷冻预成型；门店完成烹饪",
                "所有制模式": "95%加盟",
            },
            {
                "排名": 2,
                "品牌": "Taco Bell",
                "品类": "墨西哥快餐",
                "美国门店数\n(2024)": 200,
                "全系统营收\n($B, 2024)": 1.5,
                "店均AUV\n($K)": 7500,
                "店均日等效FTE\n(估算)": "20-24",
                "门店利润率\n(估算)": "15-18%",
                "央厨/供应链模式": "区域预处理 + 门店复热",
                "所有制模式": "93%加盟",
            },
        ]
    )

    ai_rows = [
        {
            "品牌": "McDonald's",
            "AI/技术策略方向": "后台AI",
            "关键举措": "Drive-thru decision support",
            "部署规模": "Pilot",
            "落地效果/数据": "Order speed improved",
            "当前状态(2026Q1)": "推进中",
        },
        {
            "品牌": "Taco Bell",
            "AI/技术策略方向": "前台AI",
            "关键举措": "Voice ordering",
            "部署规模": "Regional",
            "落地效果/数据": "Higher check size",
            "当前状态(2026Q1)": "评估中",
        },
    ]
    if include_extra_ai_brand:
        ai_rows.append(
            {
                "品牌": "Sweetgreen",
                "AI/技术策略方向": "运营AI",
                "关键举措": "Kitchen forecasting",
                "部署规模": "Limited",
                "落地效果/数据": "Prep waste reduced",
                "当前状态(2026Q1)": "试点中",
            }
        )
    ai_strategy_registry = pd.DataFrame(ai_rows)

    data_notes = pd.DataFrame(
        [
            {"字段": "美国门店数", "说明": "2024年底美国门店数量"},
            {"字段": "店均AUV", "说明": "单位为千美元"},
            {"字段": None, "说明": None},
            {"字段": "关键发现", "说明": None},
            {"字段": "1", "说明": "自动化覆盖率与AUV存在正相关"},
            {"字段": "2", "说明": "高FTE品牌仍可维持强利润率"},
            {"字段": None, "说明": None},
            {"字段": "数据收集日期", "说明": "2026年4月3日"},
        ]
    )

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        core_brand_metrics.to_excel(writer, sheet_name="QSR Top30 核心数据", index=False)
        ai_strategy_registry.to_excel(writer, sheet_name="AI策略与落地效果", index=False)
        data_notes.to_excel(writer, sheet_name="数据说明与来源", index=False)


def test_validate_workbook_good_path_from_silver_directory(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    workbook_path = settings.data_raw / "fixture.xlsx"
    _write_validation_fixture_workbook(workbook_path)

    ingest_workbook(workbook_path, settings)
    run = validate_workbook(settings.data_silver, settings=settings, tolerance_auv=0.05)

    assert run.passed is True
    assert run.counts["error"] == 0
    assert run.counts["warning"] == 0
    assert run.counts["info"] >= 5
    assert run.artifacts is not None
    assert run.artifacts.summary_markdown.exists()
    assert run.artifacts.results_json.exists()
    assert run.artifacts.flags_parquet.exists()

    results_payload = json.loads(run.artifacts.results_json.read_text(encoding="utf-8"))
    assert results_payload["passed"] is True
    assert results_payload["counts"]["warning"] == 0


def test_validate_workbook_detects_failing_invariants(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    workbook_path = settings.data_raw / "fixture.xlsx"
    _write_validation_fixture_workbook(workbook_path)

    ingest_workbook(workbook_path, settings)
    core_path = settings.data_silver / "core_brand_metrics.parquet"
    core_metrics = pd.read_parquet(core_path)
    core_metrics.loc[0, "average_unit_volume_usd_thousands"] = 5000
    core_metrics.loc[1, "rank"] = 1
    core_metrics.loc[1, "brand_name"] = core_metrics.loc[0, "brand_name"]
    core_metrics.loc[1, "fte_min"] = 10.0
    core_metrics.loc[1, "fte_mid"] = 9.0
    core_metrics.loc[1, "fte_max"] = 8.0
    core_metrics.to_parquet(core_path, index=False)

    run = validate_workbook(settings.data_silver, settings=settings, tolerance_auv=0.05)

    assert run.passed is False
    error_checks = {finding.check_name for finding in run.findings if finding.severity == "error"}
    assert "core_brand_metrics.rank_unique" in error_checks
    assert "core_brand_metrics.brand_unique" in error_checks
    assert "implied_auv_k" in error_checks
    assert "fte_range_order" in error_checks


def test_validate_workbook_surfaces_schema_null_and_range_failures(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    workbook_path = settings.data_raw / "fixture.xlsx"
    _write_validation_fixture_workbook(workbook_path)

    ingest_workbook(workbook_path, settings)
    core_path = settings.data_silver / "core_brand_metrics.parquet"
    core_metrics = pd.read_parquet(core_path)
    core_metrics.loc[0, "brand_name"] = None
    core_metrics.loc[1, "margin_max_pct"] = 120.0
    core_metrics.to_parquet(core_path, index=False)

    run = validate_workbook(settings.data_silver, settings=settings, tolerance_auv=0.05)

    assert run.passed is False
    schema_row_findings = [
        finding for finding in run.findings if finding.check_name == "core_brand_metrics.schema.row"
    ]
    categories = {finding.category for finding in schema_row_findings}
    assert "null" in categories
    assert "allowed_range" in categories
    assert any("brand_name" in finding.message for finding in schema_row_findings)
    assert any("margin_max_pct" in finding.message for finding in schema_row_findings)


def test_validate_workbook_warns_for_extra_ai_brand_from_raw_workbook(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    workbook_path = settings.data_raw / "fixture.xlsx"
    _write_validation_fixture_workbook(workbook_path, include_extra_ai_brand=True)

    run = validate_workbook(workbook_path, settings=settings, tolerance_auv=0.05)

    assert run.passed is True
    warning_findings = [finding for finding in run.findings if finding.severity == "warning"]
    assert len(warning_findings) == 1
    assert warning_findings[0].check_name == "brand_alignment.extra_ai_brands"
    assert "Sweetgreen" in warning_findings[0].message


def test_cli_validate_workbook_writes_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _build_settings(tmp_path)
    workbook_path = settings.data_raw / "fixture.xlsx"
    _write_validation_fixture_workbook(workbook_path, include_extra_ai_brand=True)

    monkeypatch.setenv("QSR_DATA_RAW", str(settings.data_raw))
    monkeypatch.setenv("QSR_DATA_BRONZE", str(settings.data_bronze))
    monkeypatch.setenv("QSR_DATA_SILVER", str(settings.data_silver))
    monkeypatch.setenv("QSR_DATA_GOLD", str(settings.data_gold))
    monkeypatch.setenv("QSR_DATA_REFERENCE", str(settings.data_reference))
    monkeypatch.setenv("QSR_REPORTS_DIR", str(settings.reports_dir))

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["validate-workbook", "--input", str(workbook_path), "--tolerance-auv", "0.05"],
    )

    assert result.exit_code == 0
    assert "Validation passed" in result.stdout
    assert (settings.reports_dir / "validation" / "validation_summary.md").exists()
    assert (settings.reports_dir / "validation" / "validation_results.json").exists()
    assert (settings.data_gold / "validation_flags.parquet").exists()


def test_cli_validate_workbook_failure_still_writes_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _build_settings(tmp_path)
    workbook_path = settings.data_raw / "fixture.xlsx"
    _write_validation_fixture_workbook(workbook_path)

    ingest_workbook(workbook_path, settings)
    core_path = settings.data_silver / "core_brand_metrics.parquet"
    core_metrics = pd.read_parquet(core_path)
    core_metrics.loc[0, "average_unit_volume_usd_thousands"] = 1.0
    core_metrics.to_parquet(core_path, index=False)

    monkeypatch.setenv("QSR_DATA_RAW", str(settings.data_raw))
    monkeypatch.setenv("QSR_DATA_BRONZE", str(settings.data_bronze))
    monkeypatch.setenv("QSR_DATA_SILVER", str(settings.data_silver))
    monkeypatch.setenv("QSR_DATA_GOLD", str(settings.data_gold))
    monkeypatch.setenv("QSR_DATA_REFERENCE", str(settings.data_reference))
    monkeypatch.setenv("QSR_REPORTS_DIR", str(settings.reports_dir))

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["validate-workbook", "--input", str(settings.data_silver), "--tolerance-auv", "0.05"],
    )

    assert result.exit_code == 1
    assert "Validation failed" in result.stdout
    assert (settings.reports_dir / "validation" / "validation_summary.md").exists()
    assert (settings.reports_dir / "validation" / "validation_results.json").exists()
    assert (settings.data_gold / "validation_flags.parquet").exists()
