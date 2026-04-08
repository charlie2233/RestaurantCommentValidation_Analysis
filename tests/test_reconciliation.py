"""Tests for provenance, entity resolution, and reconciliation."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from qsr_audit.cli import app
from qsr_audit.config import Settings
from qsr_audit.reconcile import (
    audit_reference_coverage,
    load_reference_catalog,
    reconcile_core_metrics,
    resolve_brand_name,
)
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


def _write_core_metrics(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "rank": 1,
                "brand_name": "McDonalds",
                "canonical_brand_name": "McDonald's",
                "category": "Burger",
                "us_store_count_2024": 13559,
                "systemwide_revenue_usd_billions_2024": 53.5,
                "average_unit_volume_usd_thousands": 4001,
                "store_daily_equivalent_fte_range": "25-35",
                "store_margin_range_pct": "18-20%",
                "central_kitchen_supply_chain_model": "Frozen / finished in store",
                "ownership_model": "Franchise",
                "source_sheet": "QSR Top30 核心数据",
                "row_number": 2,
                "fte_min": 25.0,
                "fte_max": 35.0,
                "fte_mid": 30.0,
                "margin_min_pct": 18.0,
                "margin_max_pct": 20.0,
                "margin_mid_pct": 19.0,
            },
            {
                "rank": 2,
                "brand_name": "Taco Bell",
                "canonical_brand_name": "Taco Bell",
                "category": "Mexican",
                "us_store_count_2024": 7604,
                "systemwide_revenue_usd_billions_2024": 15.0,
                "average_unit_volume_usd_thousands": 2100,
                "store_daily_equivalent_fte_range": "16-19",
                "store_margin_range_pct": "18-22%",
                "central_kitchen_supply_chain_model": "Regional prep",
                "ownership_model": "Franchise",
                "source_sheet": "QSR Top30 核心数据",
                "row_number": 3,
                "fte_min": 16.0,
                "fte_max": 19.0,
                "fte_mid": 17.5,
                "margin_min_pct": 18.0,
                "margin_max_pct": 22.0,
                "margin_mid_pct": 20.0,
            },
        ]
    ).to_parquet(path, index=False)


def _write_reference_file(path: Path) -> None:
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
                "average_unit_volume_usd_thousands": 4001,
                "currency": "USD",
                "geography": "US",
                "source_page": "12",
                "source_excerpt": "McDonald's metrics",
            }
        ]
    ).to_csv(path, index=False)


def _write_reference_file_with_extra_brand(path: Path) -> None:
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
                "average_unit_volume_usd_thousands": 4001,
                "currency": "USD",
                "geography": "US",
                "source_page": "12",
                "source_excerpt": "McDonald's metrics",
            },
            {
                "brand_name": "Burger King",
                "canonical_brand_name": "Burger King",
                "source_type": "qsr50",
                "source_name": "QSR 50",
                "source_url_or_doc_id": "doc-3",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.85,
                "notes": "Extra brand not present in audited core",
                "qsr50_rank": 7,
                "us_store_count_2024": 6800,
                "systemwide_revenue_usd_billions_2024": 11.2,
                "average_unit_volume_usd_thousands": 1650,
                "currency": "USD",
                "geography": "US",
                "source_page": "14",
                "source_excerpt": "Extra brand row for coverage regression",
            },
        ]
    ).to_csv(path, index=False)


def _write_partial_reference_file(path: Path) -> None:
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
                "average_unit_volume_usd_thousands": 4001,
                "currency": "USD",
                "geography": "US",
                "source_page": "12",
                "source_excerpt": "Full reference row for control coverage",
            },
            {
                "brand_name": "Taco Bell",
                "canonical_brand_name": "Taco Bell",
                "source_type": "technomic",
                "source_name": "Technomic",
                "source_url_or_doc_id": "doc-2",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "estimated",
                "confidence_score": 0.55,
                "notes": "Coverage is intentionally partial for regression testing",
                "qsr50_rank": 1,
                "us_store_count_2024": "",
                "systemwide_revenue_usd_billions_2024": "",
                "average_unit_volume_usd_thousands": "",
                "currency": "USD",
                "geography": "US",
                "source_page": "15",
                "source_excerpt": "Intentionally partial reference row",
            },
        ]
    ).to_csv(path, index=False)


def _normalize_metric_list(value: object) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if hasattr(value, "tolist") and not isinstance(value, str | bytes):
        value = value.tolist()
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return [part.strip() for part in text.split(",") if part.strip()]
        if isinstance(parsed, list):
            return [str(item) for item in parsed if str(item).strip()]
        return [str(parsed)]
    return [str(value)]


def test_resolve_brand_name_alias_support() -> None:
    resolution = resolve_brand_name("McDonalds")

    assert resolution.is_matched is True
    assert resolution.canonical_brand_name == "McDonald's"
    assert resolution.match_method == "alias_exact"
    assert resolution.match_confidence == 1.0


def test_resolve_brand_name_does_not_fuzzy_match_by_default() -> None:
    resolution = resolve_brand_name("McDonlds")

    assert resolution.is_matched is False
    assert resolution.canonical_brand_name is None
    assert resolution.match_method == "unmatched"
    assert resolution.match_confidence == 0.0


def test_load_reference_catalog_warns_on_unresolved_reference_brand(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    pd.DataFrame(
        [
            {
                "brand_name": "McDonlds",
                "canonical_brand_name": "",
                "source_type": "qsr50",
                "source_name": "QSR 50",
                "source_url_or_doc_id": "doc-ambiguous",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.7,
                "notes": "Typo row for warning coverage",
                "qsr50_rank": 1,
                "us_store_count_2024": 13559,
                "systemwide_revenue_usd_billions_2024": 53.5,
                "average_unit_volume_usd_thousands": 4001,
                "currency": "USD",
                "geography": "US",
                "source_page": "12",
                "source_excerpt": "Typo row",
            }
        ]
    ).to_csv(settings.data_reference / "qsr50_reference.csv", index=False)

    _catalog, warnings, _registry = load_reference_catalog(settings.data_reference)

    assert any(
        "did not exact-resolve to a known canonical brand" in warning for warning in warnings
    )


def test_reconcile_core_metrics_writes_gold_outputs_and_warnings(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    core_path = settings.data_silver / "core_brand_metrics.parquet"
    _write_core_metrics(core_path)
    _write_reference_file(settings.data_reference / "qsr50_reference.csv")

    run = reconcile_core_metrics(core_path, settings.data_reference, settings=settings)

    assert run.artifacts.reconciled_core_metrics_path.exists()
    assert run.artifacts.provenance_registry_path.exists()
    assert run.artifacts.reconciliation_summary_path.exists()

    reconciled = pd.read_parquet(run.artifacts.reconciled_core_metrics_path)
    assert reconciled["canonical_brand_name"].tolist() == ["McDonald's", "Taco Bell"]
    assert reconciled.loc[0, "overall_credibility_grade"] == "A"
    assert reconciled.loc[1, "reference_source_count"] == 0
    assert "covered_metrics_count" in reconciled.columns
    assert "missing_metrics" in reconciled.columns
    assert "provenance_completeness_summary" in reconciled.columns
    assert "provenance_confidence_summary" in reconciled.columns
    assert reconciled.loc[0, "covered_metrics_count"] >= 1
    assert reconciled.loc[1, "covered_metrics_count"] == 0
    assert _normalize_metric_list(reconciled.loc[0, "missing_metrics"]) == []
    assert "rank" in _normalize_metric_list(reconciled.loc[1, "missing_metrics"])
    assert (
        "provenance fields populated"
        in str(reconciled.loc[0, "provenance_completeness_summary"]).lower()
    )
    assert "average confidence" in str(reconciled.loc[0, "provenance_confidence_summary"]).lower()
    assert "No reference coverage found for `Taco Bell`." in run.warnings

    provenance = pd.read_parquet(run.artifacts.provenance_registry_path)
    assert len(provenance) == 3
    assert set(provenance["source_type"]) == {"workbook", "qsr50"}
    assert run.artifacts.reference_coverage_parquet_path.exists()
    assert run.artifacts.reference_coverage_markdown_path.exists()


def test_load_reference_catalog_warns_on_partially_filled_reference_csv(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    _write_partial_reference_file(settings.data_reference / "qsr50_reference.csv")

    catalog, warnings, registry = load_reference_catalog(settings.data_reference)

    assert not catalog.empty
    assert len(registry.records) >= 1
    assert warnings
    assert any(
        "missing" in warning.lower() or "incomplete" in warning.lower() for warning in warnings
    )
    assert "Taco Bell" in catalog["canonical_brand_name"].tolist()


def test_audit_reference_coverage_normalizes_core_aliases_before_matching(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    core_path = settings.data_silver / "core_brand_metrics.parquet"
    _write_core_metrics(core_path)
    _write_reference_file(settings.data_reference / "qsr50_reference.csv")

    core_frame = pd.read_parquet(core_path).drop(columns=["canonical_brand_name"])
    core_frame.to_parquet(core_path, index=False)

    run = audit_reference_coverage(core_path, settings.data_reference, settings=settings)

    coverage = pd.read_parquet(run.artifacts.coverage_parquet_path)
    brand_row = coverage.loc[
        (coverage["coverage_kind"] == "brand") & (coverage["brand_name"] == "McDonalds")
    ].iloc[0]
    assert brand_row["canonical_brand_name"] == "McDonald's"
    assert bool(brand_row["is_covered"]) is True
    assert int(brand_row["covered_metrics_count"]) == 4
    assert not any(
        "McDonald's" in warning and "No populated manual reference rows matched" in warning
        for warning in run.warnings
    )


def test_audit_reference_coverage_writes_outputs_and_reports_empty_reference(
    tmp_path: Path,
) -> None:
    settings = _build_settings(tmp_path)
    core_path = settings.data_silver / "core_brand_metrics.parquet"
    _write_core_metrics(core_path)
    _write_partial_reference_file(settings.data_reference / "qsr50_reference.csv")

    partial_run = audit_reference_coverage(
        core_path,
        settings.data_reference,
        settings=settings,
    )
    assert partial_run.artifacts.coverage_parquet_path.exists()
    assert partial_run.artifacts.coverage_markdown_path.exists()
    assert any(
        "missing" in warning.lower() or "incomplete" in warning.lower()
        for warning in partial_run.warnings
    )

    coverage = pd.read_parquet(partial_run.artifacts.coverage_parquet_path)
    assert "brand" in set(coverage["coverage_kind"].dropna())
    assert "metric" in set(coverage["coverage_kind"].dropna())

    empty_reference_dir = tmp_path / "empty_reference"
    empty_reference_dir.mkdir(parents=True, exist_ok=True)
    empty_run = audit_reference_coverage(
        core_path,
        empty_reference_dir,
        settings=settings,
    )
    assert any("no populated reference rows" in warning.lower() for warning in empty_run.warnings)
    empty_markdown = empty_run.artifacts.coverage_markdown_path.read_text(encoding="utf-8")
    assert "No reference coverage warnings were emitted." not in empty_markdown


def test_source_type_coverage_ignores_reference_brands_outside_core(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    core_path = settings.data_silver / "core_brand_metrics.parquet"
    _write_core_metrics(core_path)
    _write_reference_file_with_extra_brand(settings.data_reference / "qsr50_reference.csv")

    run = audit_reference_coverage(core_path, settings.data_reference, settings=settings)

    coverage = pd.read_parquet(run.artifacts.coverage_parquet_path)
    source_row = coverage.loc[
        (coverage["coverage_kind"] == "source_type") & (coverage["source_type"] == "qsr50")
    ].iloc[0]
    details = json.loads(source_row["details"])
    assert int(source_row["covered_brand_count"]) == 1
    assert int(source_row["missing_brand_count"]) == 1
    assert float(source_row["coverage_rate"]) == pytest.approx(0.5)
    assert float(source_row["coverage_rate"]) <= 1.0
    assert details["extra_reference_brands"] == ["Burger King"]


def test_reference_coverage_rate_is_bounded_even_with_many_extra_reference_brands(
    tmp_path: Path,
) -> None:
    settings = _build_settings(tmp_path)
    core_path = settings.data_silver / "core_brand_metrics.parquet"
    _write_core_metrics(core_path)
    _write_reference_file_with_extra_brand(settings.data_reference / "qsr50_reference.csv")
    existing_rows = pd.read_csv(settings.data_reference / "qsr50_reference.csv")
    extra_rows = pd.DataFrame(
        [
            {
                "brand_name": f"Extra Brand {index}",
                "canonical_brand_name": f"Extra Brand {index}",
                "source_type": "qsr50",
                "source_name": "QSR 50",
                "source_url_or_doc_id": f"doc-extra-{index}",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.8,
                "notes": "Coverage-rate guardrail regression row",
                "qsr50_rank": 20 + index,
                "us_store_count_2024": 1000 + index,
                "systemwide_revenue_usd_billions_2024": 1.0 + index,
                "average_unit_volume_usd_thousands": 1000 + index,
                "currency": "USD",
                "geography": "US",
                "source_page": str(20 + index),
                "source_excerpt": "Synthetic extra reference row",
            }
            for index in range(5)
        ]
    )
    pd.concat([existing_rows, extra_rows], ignore_index=True).to_csv(
        settings.data_reference / "qsr50_reference.csv",
        index=False,
    )

    run = audit_reference_coverage(core_path, settings.data_reference, settings=settings)

    coverage = pd.read_parquet(run.artifacts.coverage_parquet_path)
    source_rows = coverage.loc[coverage["coverage_kind"] == "source_type"]
    assert source_rows["coverage_rate"].fillna(0.0).le(1.0).all()
    assert source_rows["covered_brand_count"].fillna(0).le(2).all()


def test_reconcile_core_metrics_explicitly_reports_empty_reference_coverage(
    tmp_path: Path,
) -> None:
    settings = _build_settings(tmp_path)
    core_path = settings.data_silver / "core_brand_metrics.parquet"
    _write_core_metrics(core_path)

    run = reconcile_core_metrics(core_path, settings.data_reference, settings=settings)

    reconciled = pd.read_parquet(run.artifacts.reconciled_core_metrics_path)
    assert reconciled["reference_source_count"].tolist() == [0, 0]
    assert all(
        _normalize_metric_list(value) == [] for value in reconciled["missing_metrics"].tolist()
    ) or all(reconciled["reference_source_count"] == 0)
    assert "No reference coverage found for `McDonalds`." in run.warnings
    assert "No reference coverage found for `Taco Bell`." in run.warnings
    summary = run.artifacts.reconciliation_summary_path.read_text(encoding="utf-8")
    assert "Brands without reference coverage" in summary
    assert "All workbook brands had at least one reference source" not in summary
    assert "No reconciliation warnings were emitted" not in summary


def test_cli_reconcile_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _build_settings(tmp_path)
    core_path = settings.data_silver / "core_brand_metrics.parquet"
    _write_core_metrics(core_path)
    _write_reference_file(settings.data_reference / "qsr50_reference.csv")

    monkeypatch.setenv("QSR_DATA_RAW", str(settings.data_raw))
    monkeypatch.setenv("QSR_DATA_BRONZE", str(settings.data_bronze))
    monkeypatch.setenv("QSR_DATA_SILVER", str(settings.data_silver))
    monkeypatch.setenv("QSR_DATA_GOLD", str(settings.data_gold))
    monkeypatch.setenv("QSR_DATA_REFERENCE", str(settings.data_reference))
    monkeypatch.setenv("QSR_REPORTS_DIR", str(settings.reports_dir))

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "reconcile",
            "--core",
            str(core_path),
            "--reference-dir",
            str(settings.data_reference),
        ],
    )

    assert result.exit_code == 0
    assert "Reconciliation complete" in result.stdout
    assert (settings.data_gold / "reconciled_core_metrics.parquet").exists()
    assert (settings.data_gold / "provenance_registry.parquet").exists()
    assert (settings.reports_dir / "reconciliation" / "reconciliation_summary.md").exists()
    assert (settings.data_gold / "reference_coverage.parquet").exists()
    assert (settings.reports_dir / "reference" / "reference_coverage.md").exists()


def test_cli_audit_reference_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _build_settings(tmp_path)
    core_path = settings.data_silver / "core_brand_metrics.parquet"
    _write_core_metrics(core_path)
    _write_partial_reference_file(settings.data_reference / "qsr50_reference.csv")

    monkeypatch.setenv("QSR_DATA_RAW", str(settings.data_raw))
    monkeypatch.setenv("QSR_DATA_BRONZE", str(settings.data_bronze))
    monkeypatch.setenv("QSR_DATA_SILVER", str(settings.data_silver))
    monkeypatch.setenv("QSR_DATA_GOLD", str(settings.data_gold))
    monkeypatch.setenv("QSR_DATA_REFERENCE", str(settings.data_reference))
    monkeypatch.setenv("QSR_REPORTS_DIR", str(settings.reports_dir))

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "audit-reference",
            "--core",
            str(core_path),
            "--reference-dir",
            str(settings.data_reference),
        ],
    )

    assert result.exit_code == 0
    assert "Reference coverage audit complete" in result.stdout
    assert (settings.data_gold / "reference_coverage.parquet").exists()
    assert (settings.reports_dir / "reference" / "reference_coverage.md").exists()
