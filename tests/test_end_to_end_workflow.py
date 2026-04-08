"""End-to-end local workflow smoke test."""

from __future__ import annotations

import json

import pandas as pd
import pytest
from typer.testing import CliRunner

from qsr_audit.cli import app
from tests.helpers import build_settings


def _write_five_brand_demo_workbook(path) -> None:
    core_brand_metrics = pd.DataFrame(
        [
            {
                "排名": 1,
                "品牌": "McDonalds",
                "品类": "汉堡/Drive-Thru",
                "美国门店数\n(2024)": 13559,
                "全系统营收\n($B, 2024)": 53.5,
                "店均AUV\n($K)": 3946,
                "店均日等效FTE\n(估算)": "25-35",
                "门店利润率\n(估算)": "18-20%",
                "央厨/供应链模式": "供应商冷冻预成型；门店完成烹饪；Drive-Thru为主",
                "所有制模式": "95%加盟",
            },
            {
                "排名": 2,
                "品牌": "Dominos",
                "品类": "披萨",
                "美国门店数\n(2024)": 6800,
                "全系统营收\n($B, 2024)": 9.52,
                "店均AUV\n($K)": 1400,
                "店均日等效FTE\n(估算)": "12-15",
                "门店利润率\n(估算)": "15-19%",
                "央厨/供应链模式": "集中供应链和配送调度",
                "所有制模式": "98%加盟",
            },
            {
                "排名": 3,
                "品牌": "Taco Bell",
                "品类": "墨西哥快餐",
                "美国门店数\n(2024)": 7604,
                "全系统营收\n($B, 2024)": 15.0,
                "店均AUV\n($K)": 1974,
                "店均日等效FTE\n(估算)": "16-19",
                "门店利润率\n(估算)": "18-22%",
                "央厨/供应链模式": "区域备餐 + 门店组装",
                "所有制模式": "97%加盟",
            },
            {
                "排名": 4,
                "品牌": "Starbucks",
                "品类": "咖啡/饮品",
                "美国门店数\n(2024)": 17000,
                "全系统营收\n($B, 2024)": 35.7,
                "店均AUV\n($K)": 2100,
                "店均日等效FTE\n(估算)": "20-26",
                "门店利润率\n(估算)": "16-18%",
                "央厨/供应链模式": "饮品标准化 + 后台备料",
                "所有制模式": "混合",
            },
            {
                "排名": 5,
                "品牌": "Chick-fil-A",
                "品类": "鸡肉/Drive-Thru",
                "美国门店数\n(2024)": 3000,
                "全系统营收\n($B, 2024)": 21.0,
                "店均AUV\n($K)": 7000,
                "店均日等效FTE\n(估算)": "28-32",
                "门店利润率\n(估算)": "20-24%",
                "央厨/供应链模式": "菜单简单；高吞吐门店运作",
                "所有制模式": "特许经营",
            },
        ]
    )
    ai_strategy_registry = pd.DataFrame(
        [
            {
                "品牌": "McDonalds",
                "AI/技术策略方向": "后台AI",
                "关键举措": "厨房排班和Drive-Thru运营支持",
                "部署规模": "Pilot",
                "落地效果/数据": "订单速度提升",
                "当前状态(2026Q1)": "推进中",
            },
            {
                "品牌": "Dominos",
                "AI/技术策略方向": "配送优化",
                "关键举措": "ETA和路线优化",
                "部署规模": "Regional",
                "落地效果/数据": "准时率提高",
                "当前状态(2026Q1)": "推进中",
            },
            {
                "品牌": "Taco Bell",
                "AI/技术策略方向": "队列与备料",
                "关键举措": "备餐节拍和订单编排",
                "部署规模": "Pilot",
                "落地效果/数据": "高峰拥堵下降",
                "当前状态(2026Q1)": "推进中",
            },
            {
                "品牌": "Starbucks",
                "AI/技术策略方向": "数字化个性化",
                "关键举措": "移动端订单排序和推荐",
                "部署规模": "Enterprise",
                "落地效果/数据": "复购提升",
                "当前状态(2026Q1)": "推进中",
            },
            {
                "品牌": "Chick-fil-A",
                "AI/技术策略方向": "运营自动化",
                "关键举措": "Drive-Thru队列与调度",
                "部署规模": "Pilot",
                "落地效果/数据": "吞吐提升",
                "当前状态(2026Q1)": "推进中",
            },
        ]
    )
    data_notes = pd.DataFrame(
        [
            {"字段": "美国门店数", "说明": "2024年底美国门店数量"},
            {"字段": "店均AUV", "说明": "单位为千美元"},
            {"字段": None, "说明": None},
            {"字段": "关键发现", "说明": None},
            {"字段": "1", "说明": "后台AI与吞吐改善相关"},
            {"字段": "2", "说明": "数字化个性化提升复购体验"},
            {"字段": None, "说明": None},
        ]
    )

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        core_brand_metrics.to_excel(writer, sheet_name="QSR Top30 核心数据", index=False)
        ai_strategy_registry.to_excel(writer, sheet_name="AI策略与落地效果", index=False)
        data_notes.to_excel(writer, sheet_name="数据说明与来源", index=False)


def _write_partial_qsr50_reference_csv(path) -> None:
    pd.DataFrame(
        [
            {
                "brand_name": "McDonald's",
                "canonical_brand_name": "McDonald's",
                "source_type": "qsr50",
                "source_name": "QSR 50",
                "source_url_or_doc_id": "doc-1",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.95,
                "notes": "Annual ranking",
                "qsr50_rank": 1,
                "us_store_count_2024": 13559,
                "systemwide_revenue_usd_billions_2024": 53.5,
                "average_unit_volume_usd_thousands": 3946,
                "currency": "USD",
                "geography": "US",
                "source_page": "12",
                "source_excerpt": "McDonald's metrics",
            },
            {
                "brand_name": "Domino's",
                "canonical_brand_name": "Domino's",
                "source_type": "qsr50",
                "source_name": "QSR 50",
                "source_url_or_doc_id": "doc-2",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.92,
                "notes": "Annual ranking",
                "qsr50_rank": 2,
                "us_store_count_2024": 6800,
                "systemwide_revenue_usd_billions_2024": 9.52,
                "average_unit_volume_usd_thousands": 1400,
                "currency": "USD",
                "geography": "US",
                "source_page": "14",
                "source_excerpt": "Domino's metrics",
            },
            {
                "brand_name": "Taco Bell",
                "canonical_brand_name": "Taco Bell",
                "source_type": "qsr50",
                "source_name": "QSR 50",
                "source_url_or_doc_id": "doc-3",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.9,
                "notes": "Annual ranking",
                "qsr50_rank": 3,
                "us_store_count_2024": 7604,
                "systemwide_revenue_usd_billions_2024": 15.0,
                "average_unit_volume_usd_thousands": 1974,
                "currency": "USD",
                "geography": "US",
                "source_page": "15",
                "source_excerpt": "Taco Bell metrics",
            },
        ]
    ).to_csv(path, index=False)


def test_cli_end_to_end_workflow(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = build_settings(tmp_path)
    workbook_path = settings.data_raw / "workflow_fixture.xlsx"
    _write_five_brand_demo_workbook(workbook_path)
    _write_partial_qsr50_reference_csv(settings.data_reference / "qsr50_reference.csv")

    monkeypatch.setenv("QSR_DATA_RAW", str(settings.data_raw))
    monkeypatch.setenv("QSR_DATA_BRONZE", str(settings.data_bronze))
    monkeypatch.setenv("QSR_DATA_SILVER", str(settings.data_silver))
    monkeypatch.setenv("QSR_DATA_GOLD", str(settings.data_gold))
    monkeypatch.setenv("QSR_DATA_REFERENCE", str(settings.data_reference))
    monkeypatch.setenv("QSR_REPORTS_DIR", str(settings.reports_dir))
    monkeypatch.setenv("QSR_STRATEGY_DIR", str(settings.strategy_dir))
    monkeypatch.setenv("QSR_ARTIFACTS_DIR", str(settings.artifacts_dir))

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
    assert (settings.data_gold / "reference_coverage.parquet").exists()
    assert (settings.reports_dir / "index.md").exists()
    assert (settings.reports_dir / "validation" / "syntheticness_report.md").exists()
    assert (settings.reports_dir / "reference" / "reference_coverage.md").exists()
    assert (settings.reports_dir / "strategy" / "strategy_playbook.md").exists()
    assert (settings.strategy_dir / "recommendations.parquet").exists()

    reconciled = pd.read_parquet(settings.data_gold / "reconciled_core_metrics.parquet")
    assert reconciled["reference_source_count"].tolist() == [1, 1, 1, 0, 0]

    coverage = pd.read_parquet(settings.data_gold / "reference_coverage.parquet")
    qsr50_row = coverage.loc[
        (coverage["coverage_kind"] == "source_type") & (coverage["source_type"] == "qsr50")
    ].iloc[0]
    details = json.loads(qsr50_row["details"])
    assert int(qsr50_row["covered_brand_count"]) == 3
    assert int(qsr50_row["missing_brand_count"]) == 2
    assert float(qsr50_row["coverage_rate"]) == pytest.approx(0.6)
    assert details["extra_reference_brands"] == []
