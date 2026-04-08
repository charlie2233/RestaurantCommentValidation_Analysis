"""Regression coverage for primary-source-backed reconciliation."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from qsr_audit.cli import app
from qsr_audit.ingest import ingest_workbook
from typer.testing import CliRunner

from tests.helpers import build_settings


def _write_primary_source_workbook(path: Path) -> None:
    core_brand_metrics = pd.DataFrame(
        [
            {
                "排名": 1,
                "品牌": "McDonalds",
                "品类": "Burger",
                "美国门店数\n(2024)": 13559,
                "全系统营收\n($B, 2024)": 53.469,
                "店均AUV\n($K)": 4002,
                "店均日等效FTE\n(估算)": "25-35",
                "门店利润率\n(估算)": "18-20%",
                "央厨/供应链模式": "Frozen / finished in store",
                "所有制模式": "Franchise",
            },
            {
                "排名": 10,
                "品牌": "Dominos",
                "品类": "Pizza",
                "美国门店数\n(2024)": 7014,
                "全系统营收\n($B, 2024)": 9.5001,
                "店均AUV\n($K)": 1354,
                "店均日等效FTE\n(估算)": "12-15",
                "门店利润率\n(估算)": "15-19%",
                "央厨/供应链模式": "Central commissary",
                "所有制模式": "Franchise",
            },
            {
                "排名": 17,
                "品牌": "Chipotle",
                "品类": "Mexican",
                "美国门店数\n(2024)": 3680,
                "全系统营收\n($B, 2024)": 11.3,
                "店均AUV\n($K)": 3036,
                "店均日等效FTE\n(估算)": "18-22",
                "门店利润率\n(估算)": "18-23%",
                "央厨/供应链模式": "Prep-forward",
                "所有制模式": "Company-owned",
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
            for brand_name in ["McDonald's", "Domino's", "Chipotle"]
        ]
    )
    data_notes = pd.DataFrame(
        [
            {"字段": "美国门店数", "说明": "2024年底美国门店数量"},
            {"字段": "店均AUV", "说明": "单位为千美元"},
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
                "notes": "Conflicting secondary row",
                "qsr50_rank": 1,
                "us_store_count_2024": 13540,
                "systemwide_revenue_usd_billions_2024": 53.469,
                "average_unit_volume_usd_thousands": 4002,
                "currency": "USD",
                "geography": "US",
                "source_page": "QSR 50 chart",
                "source_excerpt": "McDonald's chart row",
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
                "notes": "Conflicting secondary row",
                "qsr50_rank": 10,
                "us_store_count_2024": 7014,
                "systemwide_revenue_usd_billions_2024": 9.2,
                "average_unit_volume_usd_thousands": 1354,
                "currency": "USD",
                "geography": "US",
                "source_page": "QSR 50 chart",
                "source_excerpt": "Domino's chart row",
            },
            {
                "brand_name": "Chipotle",
                "canonical_brand_name": "Chipotle",
                "source_type": "qsr50",
                "source_name": "QSR 50 2025",
                "source_url_or_doc_id": "qsr50-pdf",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.95,
                "notes": "Secondary fallback row",
                "qsr50_rank": 17,
                "us_store_count_2024": 3680,
                "systemwide_revenue_usd_billions_2024": 11.3,
                "average_unit_volume_usd_thousands": 3036,
                "currency": "USD",
                "geography": "US",
                "source_page": "QSR 50 chart",
                "source_excerpt": "Chipotle chart row",
            },
        ]
    ).to_csv(path, index=False)


def _write_primary_source_reference_file(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "brand_name": "McDonald's",
                "canonical_brand_name": "McDonald's",
                "source_type": "sec_filings",
                "source_name": "McDonald's 2024 Annual Report",
                "source_url_or_doc_id": "mcd-2024-annual-report",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.99,
                "notes": "Direct primary-source store count",
                "filing_type": "10-K",
                "filing_date": "",
                "issuer_name": "McDonald's Corporation",
                "issuer_ticker": "MCD",
                "us_store_count": 13557,
                "us_store_count_scope": "direct_comparable",
                "systemwide_revenue_usd_billions": "",
                "systemwide_revenue_scope": "not_available",
                "average_unit_volume_usd_thousands": "",
                "average_unit_volume_scope": "not_available",
                "revenue_segment_notes": "U.S. restaurants by market",
                "scope_notes": "Direct U.S. brand store count.",
                "provenance_grade": "A",
                "currency": "USD",
                "geography": "US",
                "source_page": "21",
                "source_excerpt": "U.S. 13,557 restaurants at year end 2024.",
            },
            {
                "brand_name": "Domino's",
                "canonical_brand_name": "Domino's",
                "source_type": "investor_relations",
                "source_name": "Domino's Pizza Announces Fourth Quarter and Fiscal 2024 Financial Results",
                "source_url_or_doc_id": "dpz-2024-results",
                "as_of_date": "2024-12-29",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.98,
                "notes": "Direct primary-source U.S. retail sales",
                "filing_type": "Investor release",
                "filing_date": "",
                "issuer_name": "Domino's Pizza Inc.",
                "issuer_ticker": "DPZ",
                "us_store_count": 7014,
                "us_store_count_scope": "direct_comparable",
                "systemwide_revenue_usd_billions": 9.5001,
                "systemwide_revenue_scope": "direct_comparable",
                "average_unit_volume_usd_thousands": "",
                "average_unit_volume_scope": "not_available",
                "revenue_segment_notes": "Release breaks out U.S. stores and U.S. retail sales separately.",
                "scope_notes": "Direct U.S. brand retail sales.",
                "provenance_grade": "A",
                "currency": "USD",
                "geography": "US",
                "source_page": "",
                "source_excerpt": "U.S. stores retail sales 9,500.1 and store count 7,014 at December 29, 2024.",
            },
            {
                "brand_name": "Chipotle",
                "canonical_brand_name": "Chipotle",
                "source_type": "investor_relations",
                "source_name": "CHIPOTLE ANNOUNCES FOURTH QUARTER AND FULL YEAR 2024 RESULTS",
                "source_url_or_doc_id": "cmg-2024-results",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.93,
                "notes": "Scope mismatch control row",
                "filing_type": "Investor release",
                "filing_date": "",
                "issuer_name": "Chipotle Mexican Grill Inc.",
                "issuer_ticker": "CMG",
                "us_store_count": 3726,
                "us_store_count_scope": "scope_mismatch",
                "systemwide_revenue_usd_billions": "",
                "systemwide_revenue_scope": "not_available",
                "average_unit_volume_usd_thousands": "",
                "average_unit_volume_scope": "not_available",
                "revenue_segment_notes": "Total restaurants include international licensed units.",
                "scope_notes": "Not directly comparable to U.S.-only store count.",
                "provenance_grade": "A",
                "currency": "USD",
                "geography": "Global",
                "source_page": "",
                "source_excerpt": "Total restaurant count at year-end was 3,726 including international licensed restaurants.",
            },
            {
                "brand_name": "Restaurant Brands US",
                "canonical_brand_name": "",
                "source_type": "sec_filings",
                "source_name": "Unresolved issuer control row",
                "source_url_or_doc_id": "control-row",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.80,
                "notes": "Should warn instead of silently matching.",
                "filing_type": "10-K",
                "filing_date": "",
                "issuer_name": "Restaurant Brands International",
                "issuer_ticker": "QSR",
                "us_store_count": 1000,
                "us_store_count_scope": "scope_mismatch",
                "systemwide_revenue_usd_billions": "",
                "systemwide_revenue_scope": "not_available",
                "average_unit_volume_usd_thousands": "",
                "average_unit_volume_scope": "not_available",
                "revenue_segment_notes": "Synthetic unresolved mapping row.",
                "scope_notes": "Control row for unresolved mapping coverage.",
                "provenance_grade": "A",
                "currency": "USD",
                "geography": "US",
                "source_page": "",
                "source_excerpt": "Control row.",
            },
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


def test_reconcile_primary_source_command_prefers_primary_rows_when_comparable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = build_settings(tmp_path)
    workbook_path = settings.data_raw / "primary-source-scaleup.xlsx"
    _write_primary_source_workbook(workbook_path)
    ingest_workbook(workbook_path, settings)
    _write_qsr50_reference_file(settings.data_reference / "qsr50_reference.csv")
    _write_primary_source_reference_file(settings.data_reference / "sec_filings_reference.csv")
    _configure_env(monkeypatch, settings)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "reconcile-primary-source",
            "--core",
            str(settings.data_silver / "core_brand_metrics.parquet"),
            "--reference-dir",
            str(settings.data_reference),
        ],
    )

    assert result.exit_code == 0
    assert "Primary-source reconciliation slice complete" in result.stdout
    assert (settings.reports_dir / "reconciliation" / "primary_source_coverage.md").exists()
    assert (settings.reports_dir / "reconciliation" / "primary_source_deltas.csv").exists()
    assert (settings.data_gold / "primary_source_gold_candidates.parquet").exists()

    delta_frame = pd.read_csv(settings.reports_dir / "reconciliation" / "primary_source_deltas.csv")
    mcd_store = delta_frame.loc[
        (delta_frame["canonical_brand_name"] == "McDonald's")
        & (delta_frame["metric_name"] == "store_count")
    ].iloc[0]
    dominos_sales = delta_frame.loc[
        (delta_frame["canonical_brand_name"] == "Domino's")
        & (delta_frame["metric_name"] == "system_sales")
    ].iloc[0]

    assert mcd_store["source_type"] == "sec_filings"
    assert mcd_store["source_locator"] == "mcd-2024-annual-report"
    assert mcd_store["selected_source_tier"] == "primary_source"
    assert dominos_sales["source_type"] == "investor_relations"
    assert dominos_sales["source_locator"] == "dpz-2024-results"
    assert dominos_sales["selected_source_tier"] == "primary_source"

    gold_candidates = pd.read_parquet(settings.data_gold / "primary_source_gold_candidates.parquet")
    assert set(gold_candidates["source_type"]) <= {"sec_filings", "investor_relations"}


def test_reconcile_primary_source_command_surfaces_scope_mismatches_and_unresolved_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = build_settings(tmp_path)
    workbook_path = settings.data_raw / "primary-source-scaleup.xlsx"
    _write_primary_source_workbook(workbook_path)
    ingest_workbook(workbook_path, settings)
    _write_qsr50_reference_file(settings.data_reference / "qsr50_reference.csv")
    _write_primary_source_reference_file(settings.data_reference / "sec_filings_reference.csv")
    _configure_env(monkeypatch, settings)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "reconcile-primary-source",
            "--core",
            str(settings.data_silver / "core_brand_metrics.parquet"),
            "--reference-dir",
            str(settings.data_reference),
        ],
    )

    assert result.exit_code == 0
    coverage_markdown = (
        settings.reports_dir / "reconciliation" / "primary_source_coverage.md"
    ).read_text(encoding="utf-8")
    assert "Scope Mismatches" in coverage_markdown
    assert "Chipotle" in coverage_markdown
    assert "Restaurant Brands US" in coverage_markdown

    delta_frame = pd.read_csv(settings.reports_dir / "reconciliation" / "primary_source_deltas.csv")
    chipotle_store = delta_frame.loc[
        (delta_frame["canonical_brand_name"] == "Chipotle")
        & (delta_frame["metric_name"] == "store_count")
    ].iloc[0]
    assert chipotle_store["primary_source_status"] == "scope_mismatch"
    assert chipotle_store["source_type"] == "qsr50"
    assert chipotle_store["scope_notes"] == "Not directly comparable to U.S.-only store count."
