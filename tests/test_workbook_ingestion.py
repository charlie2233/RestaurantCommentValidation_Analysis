"""Tests for workbook ingestion and normalization."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from qsr_audit.cli import app
from qsr_audit.config import Settings
from qsr_audit.ingest import (
    canonicalize_brand_name,
    ingest_workbook,
    load_workbook_sheets,
    parse_fte_range,
    parse_margin_range,
)
from typer.testing import CliRunner


def _write_fixture_workbook(path: Path) -> None:
    core_brand_metrics = pd.DataFrame(
        [
            {
                "排名": 1,
                "品牌": "McDonalds",
                "品类": "汉堡",
                "美国门店数\n(2024)": 13559,
                "全系统营收\n($B, 2024)": 53.5,
                "店均AUV\n($K)": 4001,
                "店均日等效FTE\n(估算)": "25-35",
                "门店利润率\n(估算)": "18-20%",
                "央厨/供应链模式": "供应商冷冻预成型；门店完成烹饪",
                "所有制模式": "95%加盟",
            },
            {
                "排名": 2,
                "品牌": "In N Out",
                "品类": "汉堡",
                "美国门店数\n(2024)": 410,
                "全系统营收\n($B, 2024)": 1.25,
                "店均AUV\n($K)": 3130,
                "店均日等效FTE\n(估算)": "20-25",
                "门店利润率\n(估算)": "20-24%",
                "央厨/供应链模式": "门店现切现炸",
                "所有制模式": "100%直营",
            },
        ]
    )
    ai_strategy_registry = pd.DataFrame(
        [
            {
                "品牌": "McDonald's",
                "AI/技术策略方向": "后台AI",
                "关键举措": "测试后台AI工具",
                "部署规模": "数千家",
                "落地效果/数据": "会员频次提升",
                "当前状态(2026Q1)": "推进中",
            }
        ]
    )
    data_notes = pd.DataFrame(
        [
            {"字段": "美国门店数", "说明": "2024年底美国门店数量"},
            {"字段": "店均AUV", "说明": "AUV定义说明"},
            {"字段": None, "说明": None},
            {"字段": "关键发现", "说明": None},
            {"字段": "1", "说明": "自动化与AUV正相关"},
            {"字段": "2", "说明": "人力密度高的品牌也能胜出"},
            {"字段": None, "说明": None},
            {"字段": "数据收集日期", "说明": "2026年4月3日"},
        ]
    )

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        core_brand_metrics.to_excel(writer, sheet_name="QSR Top30 核心数据", index=False)
        ai_strategy_registry.to_excel(writer, sheet_name="AI策略与落地效果", index=False)
        data_notes.to_excel(writer, sheet_name="数据说明与来源", index=False)


def test_parse_fte_range() -> None:
    assert parse_fte_range("6-8") == (6.0, 8.0, 7.0)
    assert parse_fte_range("20") == (20.0, 20.0, 20.0)
    assert parse_fte_range(None) == (None, None, None)


def test_parse_margin_range() -> None:
    assert parse_margin_range("8-12%") == (8.0, 12.0, 10.0)
    assert parse_margin_range("15%") == (15.0, 15.0, 15.0)
    assert parse_margin_range("n/a") == (None, None, None)


def test_load_workbook_sheets(tmp_path: Path) -> None:
    workbook_path = tmp_path / "fixture.xlsx"
    _write_fixture_workbook(workbook_path)

    sheets = load_workbook_sheets(workbook_path)

    assert set(sheets) == {"QSR Top30 核心数据", "AI策略与落地效果", "数据说明与来源"}
    assert list(sheets["QSR Top30 核心数据"].columns) == [
        "排名",
        "品牌",
        "品类",
        "美国门店数\n(2024)",
        "全系统营收\n($B, 2024)",
        "店均AUV\n($K)",
        "店均日等效FTE\n(估算)",
        "门店利润率\n(估算)",
        "央厨/供应链模式",
        "所有制模式",
    ]
    assert len(sheets["QSR Top30 核心数据"]) == 2
    assert len(sheets["AI策略与落地效果"]) == 1
    assert len(sheets["数据说明与来源"]) == 8


def test_load_workbook_sheets_raises_when_required_sheet_is_missing(tmp_path: Path) -> None:
    workbook_path = tmp_path / "missing_sheet.xlsx"
    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        pd.DataFrame([{"排名": 1, "品牌": "Test"}]).to_excel(
            writer, sheet_name="QSR Top30 核心数据", index=False
        )
        pd.DataFrame([{"品牌": "Test"}]).to_excel(
            writer, sheet_name="AI策略与落地效果", index=False
        )

    with pytest.raises(ValueError, match="Workbook is missing required sheets"):
        load_workbook_sheets(workbook_path)


def test_canonicalize_brand_name() -> None:
    assert canonicalize_brand_name("McDonalds") == "McDonald's"
    assert canonicalize_brand_name("In N Out") == "In-N-Out"
    assert canonicalize_brand_name("  burger king ") == "Burger King"


def test_ingest_workbook_writes_expected_outputs(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    reference_dir = tmp_path / "reference"
    reports_dir = tmp_path / "reports"
    for directory in [raw_dir, bronze_dir, silver_dir, gold_dir, reference_dir, reports_dir]:
        directory.mkdir(parents=True, exist_ok=True)

    workbook_path = raw_dir / "fixture.xlsx"
    _write_fixture_workbook(workbook_path)

    settings = Settings(
        data_raw=raw_dir,
        data_bronze=bronze_dir,
        data_silver=silver_dir,
        data_gold=gold_dir,
        data_reference=reference_dir,
        reports_dir=reports_dir,
    )

    result = ingest_workbook(workbook_path, settings)

    assert result.workbook_copy_path.exists()
    assert (bronze_dir / "qsr_top30_core_data_raw.parquet").exists()
    assert (bronze_dir / "qsr_top30_core_data_raw.csv").exists()
    assert (bronze_dir / "ai_strategy_implementation_raw.parquet").exists()
    assert (bronze_dir / "data_notes_and_sources_raw.csv").exists()
    assert result.silver_artifacts.core_brand_metrics_path.exists()
    assert result.silver_artifacts.ai_strategy_registry_path.exists()
    assert result.silver_artifacts.data_notes_path.exists()
    assert result.silver_artifacts.key_findings_path.exists()

    core_metrics = pd.read_parquet(result.silver_artifacts.core_brand_metrics_path)
    assert core_metrics["brand_name"].tolist() == ["McDonald's", "In-N-Out"]
    assert core_metrics["fte_min"].tolist() == [25.0, 20.0]
    assert core_metrics["source_sheet"].tolist() == ["QSR Top30 核心数据", "QSR Top30 核心数据"]
    assert core_metrics["row_number"].tolist() == [2, 3]

    findings = pd.read_parquet(result.silver_artifacts.key_findings_path)
    assert findings["finding_number"].tolist() == [1, 2]
    assert findings["finding_text"].tolist() == ["自动化与AUV正相关", "人力密度高的品牌也能胜出"]


def test_cli_ingest_workbook_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    reference_dir = tmp_path / "reference"
    reports_dir = tmp_path / "reports"
    for directory in [raw_dir, bronze_dir, silver_dir, gold_dir, reference_dir, reports_dir]:
        directory.mkdir(parents=True, exist_ok=True)

    workbook_path = raw_dir / "fixture.xlsx"
    _write_fixture_workbook(workbook_path)

    monkeypatch.setenv("QSR_DATA_RAW", str(raw_dir))
    monkeypatch.setenv("QSR_DATA_BRONZE", str(bronze_dir))
    monkeypatch.setenv("QSR_DATA_SILVER", str(silver_dir))
    monkeypatch.setenv("QSR_DATA_GOLD", str(gold_dir))
    monkeypatch.setenv("QSR_DATA_REFERENCE", str(reference_dir))
    monkeypatch.setenv("QSR_REPORTS_DIR", str(reports_dir))

    runner = CliRunner()
    result = runner.invoke(app, ["ingest-workbook", "--input", str(workbook_path)])

    assert result.exit_code == 0
    assert (silver_dir / "core_brand_metrics.parquet").exists()
    assert (silver_dir / "ai_strategy_registry.parquet").exists()
    assert (silver_dir / "data_notes.parquet").exists()
    assert (silver_dir / "key_findings.parquet").exists()
