"""Shared test helpers for fixture workbooks and environment-scoped settings."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from qsr_audit.config import Settings


def build_settings(tmp_path: Path) -> Settings:
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


def write_sample_workbook(path: Path) -> None:
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
        ]
    )
    ai_strategy_registry = pd.DataFrame(
        [
            {
                "品牌": "McDonald's",
                "AI/技术策略方向": "后台AI",
                "关键举措": "厨房排班和Drive-Thru运营支持",
                "部署规模": "Pilot",
                "落地效果/数据": "订单速度提升",
                "当前状态(2026Q1)": "推进中",
            },
            {
                "品牌": "Domino's",
                "AI/技术策略方向": "配送优化",
                "关键举措": "ETA和路线优化",
                "部署规模": "Regional",
                "落地效果/数据": "准时率提高",
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
            {"字段": "1", "说明": "自动化覆盖率与AUV存在正相关"},
            {"字段": "2", "说明": "配送和队列能力影响履约体验"},
        ]
    )

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        core_brand_metrics.to_excel(writer, sheet_name="QSR Top30 核心数据", index=False)
        ai_strategy_registry.to_excel(writer, sheet_name="AI策略与落地效果", index=False)
        data_notes.to_excel(writer, sheet_name="数据说明与来源", index=False)


def write_sample_reference_csv(path: Path) -> None:
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
        ]
    ).to_csv(path, index=False)
