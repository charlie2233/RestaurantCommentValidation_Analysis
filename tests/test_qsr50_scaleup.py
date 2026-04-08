"""Regression coverage for QSR50-only broader reconciliation artifacts."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from qsr_audit.cli import app
from qsr_audit.ingest import ingest_workbook
from typer.testing import CliRunner

from tests.helpers import build_settings


def _write_qsr50_scaleup_workbook(path: Path) -> None:
    core_brand_metrics = pd.DataFrame(
        [
            {
                "排名": 1,
                "品牌": "McDonalds",
                "品类": "汉堡/Drive-Thru",
                "美国门店数\n(2024)": 13559,
                "全系统营收\n($B, 2024)": 53.469,
                "店均AUV\n($K)": 4002,
                "店均日等效FTE\n(估算)": "25-35",
                "门店利润率\n(估算)": "18-20%",
                "央厨/供应链模式": "Frozen / finished in store",
                "所有制模式": "95%加盟",
            },
            {
                "排名": 4,
                "品牌": "Taco Bell",
                "品类": "Mexican",
                "美国门店数\n(2024)": 7604,
                "全系统营收\n($B, 2024)": 16.2,
                "店均AUV\n($K)": 2130,
                "店均日等效FTE\n(估算)": "16-19",
                "门店利润率\n(估算)": "18-22%",
                "央厨/供应链模式": "Regional prep",
                "所有制模式": "Franchise",
            },
            {
                "排名": 10,
                "品牌": "Dominos",
                "品类": "Pizza",
                "美国门店数\n(2024)": 7014,
                "全系统营收\n($B, 2024)": 9.5,
                "店均AUV\n($K)": 1354,
                "店均日等效FTE\n(估算)": "12-15",
                "门店利润率\n(估算)": "15-19%",
                "央厨/供应链模式": "Central commissary",
                "所有制模式": "Franchise",
            },
            {
                "排名": 11,
                "品牌": "Panda Express",
                "品类": "Asian",
                "美国门店数\n(2024)": 2505,
                "全系统营收\n($B, 2024)": 6.199,
                "店均AUV\n($K)": 2592,
                "店均日等效FTE\n(估算)": "14-18",
                "门店利润率\n(估算)": "17-21%",
                "央厨/供应链模式": "Central prep",
                "所有制模式": "Mixed",
            },
            {
                "排名": 19,
                "品牌": "Wingstop",
                "品类": "Chicken",
                "美国门店数\n(2024)": 2204,
                "全系统营收\n($B, 2024)": 4.765,
                "店均AUV\n($K)": 2138,
                "店均日等效FTE\n(估算)": "11-14",
                "门店利润率\n(估算)": "18-22%",
                "央厨/供应链模式": "Sauced in store",
                "所有制模式": "Franchise",
            },
            {
                "排名": 15,
                "品牌": "Sonic Drive-In",
                "品类": "Burger/Drive-In",
                "美国门店数\n(2024)": 3461,
                "全系统营收\n($B, 2024)": 5.384,
                "店均AUV\n($K)": 1500,
                "店均日等效FTE\n(估算)": "14-18",
                "门店利润率\n(估算)": "15-18%",
                "央厨/供应链模式": "Regional distribution",
                "所有制模式": "Franchise",
            },
        ]
    )
    ai_strategy_registry = pd.DataFrame(
        [
            {
                "品牌": brand_name,
                "AI/技术策略方向": "后台AI",
                "关键举措": "Ops support",
                "部署规模": "Pilot",
                "落地效果/数据": "Improved throughput",
                "当前状态(2026Q1)": "推进中",
            }
            for brand_name in [
                "McDonald's",
                "Taco Bell",
                "Domino's",
                "Panda Express",
                "Wingstop",
                "Sonic Drive-In",
            ]
        ]
    )
    data_notes = pd.DataFrame(
        [
            {"字段": "美国门店数", "说明": "2024年底美国门店数量"},
            {"字段": "店均AUV", "说明": "单位为千美元"},
            {"字段": None, "说明": None},
            {"字段": "关键发现", "说明": None},
            {"字段": "1", "说明": "Reference-backed rows are safer than workbook-only claims."},
            {"字段": "2", "说明": "Missing external evidence remains a gap."},
        ]
    )

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        core_brand_metrics.to_excel(writer, sheet_name="QSR Top30 核心数据", index=False)
        ai_strategy_registry.to_excel(writer, sheet_name="AI策略与落地效果", index=False)
        data_notes.to_excel(writer, sheet_name="数据说明与来源", index=False)


def _write_qsr50_reference_file(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "brand_name": "McDonald's",
                "canonical_brand_name": "McDonald's",
                "source_type": "qsr50",
                "source_name": "QSR 50 2025",
                "source_url_or_doc_id": "qsr50-pdf",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.95,
                "notes": "Chart row",
                "qsr50_rank": 1,
                "us_store_count_2024": 13559,
                "systemwide_revenue_usd_billions_2024": 53.469,
                "average_unit_volume_usd_thousands": 4002,
                "currency": "USD",
                "geography": "US",
                "source_page": "QSR 50 chart",
                "source_excerpt": "McDonald's chart row",
            },
            {
                "brand_name": "Taco Bell",
                "canonical_brand_name": "Taco Bell",
                "source_type": "qsr50",
                "source_name": "QSR 50 2025",
                "source_url_or_doc_id": "qsr50-pdf",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.95,
                "notes": "Chart row",
                "qsr50_rank": 4,
                "us_store_count_2024": 7604,
                "systemwide_revenue_usd_billions_2024": 16.2,
                "average_unit_volume_usd_thousands": 2130,
                "currency": "USD",
                "geography": "US",
                "source_page": "QSR 50 chart",
                "source_excerpt": "Taco Bell chart row",
            },
            {
                "brand_name": "Domino's",
                "canonical_brand_name": "Domino's",
                "source_type": "qsr50",
                "source_name": "QSR 50 2025",
                "source_url_or_doc_id": "qsr50-pdf",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.95,
                "notes": "Chart row",
                "qsr50_rank": 10,
                "us_store_count_2024": 7014,
                "systemwide_revenue_usd_billions_2024": 9.5,
                "average_unit_volume_usd_thousands": 1354,
                "currency": "USD",
                "geography": "US",
                "source_page": "QSR 50 chart",
                "source_excerpt": "Domino's chart row",
            },
            {
                "brand_name": "Panda Express",
                "canonical_brand_name": "Panda Express",
                "source_type": "qsr50",
                "source_name": "QSR 50 2025",
                "source_url_or_doc_id": "qsr50-pdf",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.95,
                "notes": "Chart row",
                "qsr50_rank": 11,
                "us_store_count_2024": 2505,
                "systemwide_revenue_usd_billions_2024": 6.199,
                "average_unit_volume_usd_thousands": 2592,
                "currency": "USD",
                "geography": "US",
                "source_page": "QSR 50 chart",
                "source_excerpt": "Panda Express chart row",
            },
            {
                "brand_name": "Wingstop",
                "canonical_brand_name": "Wingstop",
                "source_type": "qsr50",
                "source_name": "QSR 50 2025",
                "source_url_or_doc_id": "qsr50-pdf",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.95,
                "notes": "Chart row",
                "qsr50_rank": 19,
                "us_store_count_2024": 2204,
                "systemwide_revenue_usd_billions_2024": 4.765,
                "average_unit_volume_usd_thousands": 2138,
                "currency": "USD",
                "geography": "US",
                "source_page": "QSR 50 chart",
                "source_excerpt": "Wingstop chart row",
            },
        ]
    ).to_csv(path, index=False)


def _write_non_qsr50_reference_file(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "brand_name": "McDonald's",
                "canonical_brand_name": "McDonald's",
                "source_type": "sec_filings",
                "source_name": "SEC filing",
                "source_url_or_doc_id": "sec-doc",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.99,
                "notes": "Conflicting control row",
                "filing_type": "10-K",
                "filing_date": "2025-02-01",
                "us_store_count": 99999,
                "systemwide_revenue_usd_billions": 1.0,
                "revenue_segment_notes": "Should be ignored by the QSR50-only command",
                "currency": "USD",
                "geography": "US",
                "source_page": "1",
                "source_excerpt": "Conflicting SEC control row",
            }
        ]
    ).to_csv(path, index=False)


def _configure_env(monkeypatch: pytest.MonkeyPatch, settings) -> None:
    monkeypatch.setenv("QSR_DATA_RAW", str(settings.data_raw))
    monkeypatch.setenv("QSR_DATA_BRONZE", str(settings.data_bronze))
    monkeypatch.setenv("QSR_DATA_SILVER", str(settings.data_silver))
    monkeypatch.setenv("QSR_DATA_GOLD", str(settings.data_gold))
    monkeypatch.setenv("QSR_DATA_REFERENCE", str(settings.data_reference))
    monkeypatch.setenv("QSR_REPORTS_DIR", str(settings.reports_dir))
    monkeypatch.setenv("QSR_STRATEGY_DIR", str(settings.strategy_dir))
    monkeypatch.setenv("QSR_ARTIFACTS_DIR", str(settings.artifacts_dir))


def test_reconcile_qsr50_command_generates_broader_slice_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = build_settings(tmp_path)
    workbook_path = settings.data_raw / "qsr50-scaleup.xlsx"
    _write_qsr50_scaleup_workbook(workbook_path)
    ingest_workbook(workbook_path, settings)
    _write_qsr50_reference_file(settings.data_reference / "qsr50_reference.csv")
    _write_non_qsr50_reference_file(settings.data_reference / "sec_filings_reference.csv")
    _configure_env(monkeypatch, settings)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "reconcile-qsr50",
            "--core",
            str(settings.data_silver / "core_brand_metrics.parquet"),
            "--reference-dir",
            str(settings.data_reference),
        ],
    )

    assert result.exit_code == 0
    assert "QSR50 reconciliation slice complete" in result.stdout
    assert (settings.reports_dir / "reconciliation" / "qsr50_coverage.md").exists()
    assert (settings.reports_dir / "reconciliation" / "brand_deltas_full.csv").exists()
    assert (settings.reports_dir / "summary" / "unresolved_reference_gaps.md").exists()
    assert (settings.data_gold / "qsr50_gold_candidates.parquet").exists()

    delta_frame = pd.read_csv(settings.reports_dir / "reconciliation" / "brand_deltas_full.csv")
    assert {"brand_name", "source_locator", "publish_status_candidate"} <= set(delta_frame.columns)
    matched_rows = delta_frame.loc[
        delta_frame["canonical_brand_name"].isin(["McDonald's", "Domino's"])
    ]
    assert set(matched_rows["source_type"].dropna()) == {"qsr50"}
    assert "sec-doc" not in matched_rows["source_locator"].fillna("").tolist()

    coverage_markdown = (settings.reports_dir / "reconciliation" / "qsr50_coverage.md").read_text(
        encoding="utf-8"
    )
    assert "Brands with QSR50 coverage" in coverage_markdown
    assert "Sonic Drive-In" in coverage_markdown


def test_reconcile_qsr50_command_reports_unresolved_brands_explicitly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = build_settings(tmp_path)
    workbook_path = settings.data_raw / "qsr50-scaleup.xlsx"
    _write_qsr50_scaleup_workbook(workbook_path)
    ingest_workbook(workbook_path, settings)
    _write_qsr50_reference_file(settings.data_reference / "qsr50_reference.csv")
    _configure_env(monkeypatch, settings)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "reconcile-qsr50",
            "--core",
            str(settings.data_silver / "core_brand_metrics.parquet"),
            "--reference-dir",
            str(settings.data_reference),
        ],
    )

    assert result.exit_code == 0
    unresolved_markdown = (
        settings.reports_dir / "summary" / "unresolved_reference_gaps.md"
    ).read_text(encoding="utf-8")
    assert "Brands Still Missing QSR50 Coverage" in unresolved_markdown
    assert "Sonic Drive-In" in unresolved_markdown
