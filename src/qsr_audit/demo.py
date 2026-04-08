"""Deterministic 5-brand happy-path demo orchestration."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.config import Settings
from qsr_audit.contracts.workbook import SILVER_OUTPUT_FILES
from qsr_audit.gold import GoldGateRun, gate_gold_publish
from qsr_audit.governance import (
    DataClassification,
    format_artifact_path,
    latest_manifest_path,
    write_artifact_manifest,
)
from qsr_audit.ingest import IngestWorkbookArtifacts, canonicalize_brand_name, ingest_workbook
from qsr_audit.reconcile import REFERENCE_TEMPLATE_FILES, ReconciliationRun, reconcile_core_metrics
from qsr_audit.reconcile.pipeline import REFERENCE_TEMPLATE_COLUMNS
from qsr_audit.release import ReleasePreflightRun, preflight_release
from qsr_audit.reporting import ReportArtifacts, write_reports
from qsr_audit.strategy import StrategyRun, generate_strategy_outputs
from qsr_audit.validate import SyntheticnessRun, ValidationRun, run_syntheticness, validate_workbook

DEFAULT_DEMO_RELATIVE_ROOT = Path("demo/5-brand-happy-path")
DEMO_REFERENCE_AS_OF_DATE = "2024-12-31"
DEMO_REFERENCE_CONFIDENCE = 0.95
DEMO_REFERENCE_NOTE = (
    "Deterministic demo reference row mirrored from the selected Silver slice to exercise the "
    "happy-path pipeline. It is a walkthrough artifact, not external truth."
)


@dataclass(frozen=True)
class FiveBrandDemoArtifacts:
    """Top-level files written by the happy-path demo."""

    summary_json_path: Path
    summary_markdown_path: Path
    selected_brands_json_path: Path
    demo_silver_dir: Path
    demo_reference_dir: Path


@dataclass(frozen=True)
class FiveBrandHappyPathDemoRun:
    """Complete result for the end-to-end 5-brand demo."""

    input_path: Path
    output_root: Path
    workspace_root: Path
    demo_settings: Settings
    ingest_artifacts: IngestWorkbookArtifacts
    selected_brands: tuple[str, ...]
    validation_run: ValidationRun
    syntheticness_run: SyntheticnessRun
    reconciliation_run: ReconciliationRun
    gold_gate_run: GoldGateRun
    report_artifacts: ReportArtifacts
    strategy_run: StrategyRun
    preflight_run: ReleasePreflightRun
    artifacts: FiveBrandDemoArtifacts


def run_five_brand_happy_path_demo(
    *,
    input_path: Path,
    settings: Settings | None = None,
    output_root: Path | None = None,
    brands: tuple[str, ...] = (),
) -> FiveBrandHappyPathDemoRun:
    """Run the isolated 5-brand workbook demo with existing pipeline stages."""

    base_settings = settings or Settings()
    resolved_input = input_path.expanduser().resolve()
    if not resolved_input.exists():
        raise FileNotFoundError(f"Workbook not found: {resolved_input}")

    resolved_output_root = base_settings.validate_artifact_root(
        (output_root or (base_settings.artifacts_dir / DEFAULT_DEMO_RELATIVE_ROOT))
        .expanduser()
        .resolve(),
        purpose="5-brand happy-path demo output",
    )
    workspace_root = resolved_output_root / "workspace"
    if workspace_root.exists():
        shutil.rmtree(workspace_root)
    workspace_root.mkdir(parents=True, exist_ok=True)

    demo_settings = Settings(
        data_raw=workspace_root / "data" / "raw",
        data_bronze=workspace_root / "data" / "bronze",
        data_silver=workspace_root / "data" / "silver",
        data_gold=workspace_root / "data" / "gold",
        data_reference=workspace_root / "data" / "reference",
        reports_dir=workspace_root / "reports",
        strategy_dir=workspace_root / "strategy",
        artifacts_dir=workspace_root / "artifacts",
        log_level=base_settings.log_level,
    )

    ingest_artifacts = ingest_workbook(resolved_input, demo_settings)
    selection_validation = validate_workbook(
        demo_settings.data_silver,
        settings=demo_settings,
        output_dir=workspace_root / "_selection" / "validation",
        gold_dir=workspace_root / "_selection" / "gold",
    )
    selection_syntheticness = run_syntheticness(
        demo_settings.data_silver,
        settings=demo_settings,
        output_dir=workspace_root / "_selection" / "validation",
        gold_dir=workspace_root / "_selection" / "gold",
        include_isolation_forest=False,
    )

    full_core = pd.read_parquet(ingest_artifacts.silver_artifacts.core_brand_metrics_path)
    full_ai = pd.read_parquet(ingest_artifacts.silver_artifacts.ai_strategy_registry_path)
    selected_brands = _select_demo_brands(
        core_frame=full_core,
        ai_frame=full_ai,
        validation_run=selection_validation,
        syntheticness_run=selection_syntheticness,
        forced_brands=brands,
    )

    demo_silver_dir = resolved_output_root / "silver_slice"
    if demo_silver_dir.exists():
        shutil.rmtree(demo_silver_dir)
    demo_silver_dir.mkdir(parents=True, exist_ok=True)
    _write_demo_silver_slice(
        selected_brands=selected_brands,
        full_core=full_core,
        full_ai=full_ai,
        data_notes_path=ingest_artifacts.silver_artifacts.data_notes_path,
        key_findings_path=ingest_artifacts.silver_artifacts.key_findings_path,
        output_dir=demo_silver_dir,
    )

    demo_reference_dir = demo_settings.data_reference
    _prepare_demo_reference_dir(
        source_reference_dir=base_settings.data_reference,
        output_dir=demo_reference_dir,
        selected_core=pd.read_parquet(demo_silver_dir / SILVER_OUTPUT_FILES["core_brand_metrics"]),
    )

    validation_run = validate_workbook(demo_silver_dir, settings=demo_settings)
    if not validation_run.passed:
        raise ValueError(
            "The selected 5-brand demo slice still has validation errors. Pass explicit `--brand` "
            "values or adjust the source workbook."
        )
    _write_demo_manifest_for_validation(
        settings=demo_settings,
        input_path=demo_silver_dir,
        run=validation_run,
    )

    syntheticness_run = run_syntheticness(
        demo_silver_dir,
        settings=demo_settings,
        include_isolation_forest=False,
    )
    _write_demo_manifest_for_syntheticness(
        settings=demo_settings,
        input_path=demo_silver_dir,
        run=syntheticness_run,
    )

    reconciliation_run = reconcile_core_metrics(
        core_path=demo_silver_dir / SILVER_OUTPUT_FILES["core_brand_metrics"],
        reference_dir=demo_reference_dir,
        settings=demo_settings,
    )
    _write_demo_manifest_for_reconciliation(
        settings=demo_settings,
        core_path=demo_silver_dir / SILVER_OUTPUT_FILES["core_brand_metrics"],
        reference_dir=demo_reference_dir,
        run=reconciliation_run,
    )

    gold_gate_run = gate_gold_publish(settings=demo_settings)
    _write_demo_manifest_for_gold_gate(settings=demo_settings, run=gold_gate_run)

    report_artifacts = write_reports(output_root=demo_settings.reports_dir, settings=demo_settings)
    strategy_run = generate_strategy_outputs(
        settings=demo_settings,
        strategy_dir=demo_settings.strategy_dir,
        report_dir=demo_settings.reports_dir / "strategy",
    )
    preflight_run = preflight_release(settings=demo_settings)

    artifacts = _write_demo_summary(
        output_root=resolved_output_root,
        input_path=resolved_input,
        selected_brands=selected_brands,
        demo_silver_dir=demo_silver_dir,
        demo_reference_dir=demo_reference_dir,
        demo_settings=demo_settings,
        validation_run=validation_run,
        syntheticness_run=syntheticness_run,
        reconciliation_run=reconciliation_run,
        gold_gate_run=gold_gate_run,
        report_artifacts=report_artifacts,
        strategy_run=strategy_run,
        preflight_run=preflight_run,
    )

    return FiveBrandHappyPathDemoRun(
        input_path=resolved_input,
        output_root=resolved_output_root,
        workspace_root=workspace_root,
        demo_settings=demo_settings,
        ingest_artifacts=ingest_artifacts,
        selected_brands=selected_brands,
        validation_run=validation_run,
        syntheticness_run=syntheticness_run,
        reconciliation_run=reconciliation_run,
        gold_gate_run=gold_gate_run,
        report_artifacts=report_artifacts,
        strategy_run=strategy_run,
        preflight_run=preflight_run,
        artifacts=artifacts,
    )


def _select_demo_brands(
    *,
    core_frame: pd.DataFrame,
    ai_frame: pd.DataFrame,
    validation_run: ValidationRun,
    syntheticness_run: SyntheticnessRun,
    forced_brands: tuple[str, ...],
) -> tuple[str, ...]:
    if forced_brands:
        normalized = tuple(
            brand
            for brand in (canonicalize_brand_name(value) for value in forced_brands)
            if brand is not None
        )
        unique = tuple(dict.fromkeys(normalized))
        if len(unique) != 5:
            raise ValueError("Pass exactly five unique `--brand` values for the demo slice.")
        available = {
            str(value)
            for value in core_frame.get("brand_name", pd.Series(dtype=str)).dropna().astype(str)
        }
        missing = sorted(set(unique) - available)
        if missing:
            raise ValueError(
                "One or more requested demo brands are missing from the workbook: "
                + ", ".join(missing)
            )
        return unique

    ai_brands = {
        str(value)
        for value in ai_frame.get("brand_name", pd.Series(dtype=str)).dropna().astype(str)
        if str(value).strip()
    }
    error_brands = {
        str(finding.brand_name)
        for finding in validation_run.findings
        if finding.severity == "error" and finding.brand_name
    }
    warning_brands = {
        str(finding.brand_name)
        for finding in validation_run.findings
        if finding.severity == "warning" and finding.brand_name
    }
    synthetic_brand_counts: dict[str, int] = {}
    for signal in syntheticness_run.report.signals:
        if signal.strength not in {"moderate", "strong"}:
            continue
        brand_name = canonicalize_brand_name(
            signal.details.get("canonical_brand_name") or signal.details.get("brand_name")
        )
        if brand_name is None:
            continue
        synthetic_brand_counts[brand_name] = synthetic_brand_counts.get(brand_name, 0) + 1

    median_rank = float(core_frame["rank"].dropna().astype(float).median())
    ranking_rows: list[tuple[tuple[float, ...], str]] = []
    for row in core_frame.to_dict(orient="records"):
        brand_name = canonicalize_brand_name(row.get("brand_name"))
        if brand_name is None:
            continue
        if any(row.get(column) is None or pd.isna(row.get(column)) for column in _required_metrics()):
            continue
        rank_value = _as_float(row.get("rank")) or 999.0
        ranking_rows.append(
            (
                (
                    0.0 if brand_name in ai_brands else 1.0,
                    0.0 if brand_name not in error_brands else 1.0,
                    float(synthetic_brand_counts.get(brand_name, 0)),
                    0.0 if brand_name not in warning_brands else 1.0,
                    abs(rank_value - median_rank),
                    rank_value,
                ),
                brand_name,
            )
        )

    ordered = [brand_name for _score, brand_name in sorted(ranking_rows, key=lambda item: item[0])]
    selected = tuple(dict.fromkeys(ordered))[:5]
    if len(selected) != 5:
        raise ValueError(
            "Could not derive five deterministic demo brands from the workbook. Pass explicit "
            "`--brand` values."
        )
    return selected


def _required_metrics() -> tuple[str, ...]:
    return (
        "rank",
        "us_store_count_2024",
        "systemwide_revenue_usd_billions_2024",
        "average_unit_volume_usd_thousands",
    )


def _write_demo_silver_slice(
    *,
    selected_brands: tuple[str, ...],
    full_core: pd.DataFrame,
    full_ai: pd.DataFrame,
    data_notes_path: Path,
    key_findings_path: Path,
    output_dir: Path,
) -> None:
    selected_set = set(selected_brands)
    sliced_core = full_core.loc[full_core["brand_name"].isin(selected_set)].copy()
    sliced_ai = full_ai.loc[full_ai["brand_name"].isin(selected_set)].copy()
    sliced_core = sliced_core.sort_values(by=["rank", "brand_name"], kind="stable").reset_index(
        drop=True
    )
    sliced_ai = sliced_ai.sort_values(by=["brand_name", "row_number"], kind="stable").reset_index(
        drop=True
    )
    if len(sliced_core.index) != 5:
        raise ValueError(
            f"Expected exactly five core demo rows after slicing, found {len(sliced_core.index)}."
        )
    if set(sliced_ai["brand_name"].astype(str)) != selected_set:
        raise ValueError(
            "The selected 5-brand demo slice does not have one AI strategy row for every brand."
        )

    sliced_core.to_parquet(output_dir / SILVER_OUTPUT_FILES["core_brand_metrics"], index=False)
    sliced_ai.to_parquet(output_dir / SILVER_OUTPUT_FILES["ai_strategy_registry"], index=False)
    pd.read_parquet(data_notes_path).to_parquet(
        output_dir / SILVER_OUTPUT_FILES["data_notes"],
        index=False,
    )
    pd.read_parquet(key_findings_path).to_parquet(
        output_dir / SILVER_OUTPUT_FILES["key_findings"],
        index=False,
    )


def _prepare_demo_reference_dir(
    *,
    source_reference_dir: Path,
    output_dir: Path,
    selected_core: pd.DataFrame,
) -> None:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    source_templates = source_reference_dir / "templates"
    if not source_templates.exists():
        raise FileNotFoundError(
            f"Reference templates directory is missing: {source_templates}"
        )
    shutil.copytree(source_templates, output_dir / "templates")

    reference_frames = _build_demo_reference_frames(selected_core)
    for file_name, frame in reference_frames.items():
        frame.to_csv(output_dir / file_name, index=False, encoding="utf-8-sig")


def _build_demo_reference_frames(selected_core: pd.DataFrame) -> dict[str, pd.DataFrame]:
    rows_by_file: dict[str, list[dict[str, Any]]] = {
        file_name: [] for file_name in REFERENCE_TEMPLATE_FILES
    }
    for row in selected_core.to_dict(orient="records"):
        brand_name = str(row["brand_name"])
        slug = _brand_slug(brand_name)
        common = {
            "brand_name": brand_name,
            "canonical_brand_name": brand_name,
            "as_of_date": DEMO_REFERENCE_AS_OF_DATE,
            "method_reported_or_estimated": "reported",
            "confidence_score": DEMO_REFERENCE_CONFIDENCE,
            "notes": DEMO_REFERENCE_NOTE,
            "currency": "USD",
            "geography": "US",
            "source_page": "1",
            "source_excerpt": f"Demo walkthrough evidence for {brand_name}.",
        }

        rows_by_file["qsr50_reference.csv"].append(
            {
                **common,
                "source_type": "industry_ranking",
                "source_name": "Demo QSR50 walkthrough",
                "source_url_or_doc_id": f"demo:qsr50:{slug}",
                "qsr50_rank": _as_int(row.get("rank")),
                "us_store_count_2024": _as_int(row.get("us_store_count_2024")),
                "systemwide_revenue_usd_billions_2024": _as_float(
                    row.get("systemwide_revenue_usd_billions_2024")
                ),
                "average_unit_volume_usd_thousands": _as_float(
                    row.get("average_unit_volume_usd_thousands")
                ),
            }
        )
        rows_by_file["technomic_reference.csv"].append(
            {
                **common,
                "source_type": "industry_estimate",
                "source_name": "Demo Technomic walkthrough",
                "source_url_or_doc_id": f"demo:technomic:{slug}",
                "technomic_rank": _as_int(row.get("rank")),
                "us_store_count_estimate": _as_int(row.get("us_store_count_2024")),
                "systemwide_revenue_usd_billions_estimate": _as_float(
                    row.get("systemwide_revenue_usd_billions_2024")
                ),
                "average_unit_volume_usd_thousands_estimate": _as_float(
                    row.get("average_unit_volume_usd_thousands")
                ),
                "margin_estimate_pct": _as_float(row.get("margin_mid_pct")),
            }
        )
        rows_by_file["sec_filings_reference.csv"].append(
            {
                **common,
                "source_type": "sec_filing",
                "source_name": "Demo SEC walkthrough",
                "source_url_or_doc_id": f"demo:sec:{slug}",
                "filing_type": "10-K",
                "filing_date": DEMO_REFERENCE_AS_OF_DATE,
                "us_store_count": _as_int(row.get("us_store_count_2024")),
                "systemwide_revenue_usd_billions": _as_float(
                    row.get("systemwide_revenue_usd_billions_2024")
                ),
                "revenue_segment_notes": "Demo mirrored total for walkthrough coverage.",
            }
        )
        rows_by_file["franchise_disclosure_reference.csv"].append(
            {
                **common,
                "source_type": "franchise_disclosure_document",
                "source_name": "Demo FDD walkthrough",
                "source_url_or_doc_id": f"demo:fdd:{slug}",
                "fdd_year": 2024,
                "franchise_units_us": _as_int(row.get("us_store_count_2024")),
                "royalty_rate_pct": 5.0,
                "advertising_fund_rate_pct": 4.0,
                "average_unit_volume_usd_thousands": _as_float(
                    row.get("average_unit_volume_usd_thousands")
                ),
            }
        )

    frames: dict[str, pd.DataFrame] = {}
    for file_name, rows in rows_by_file.items():
        frames[file_name] = pd.DataFrame(rows, columns=REFERENCE_TEMPLATE_COLUMNS[file_name])
    return frames


def _write_demo_manifest_for_validation(
    *,
    settings: Settings,
    input_path: Path,
    run: ValidationRun,
) -> Path:
    return write_artifact_manifest(
        settings=settings,
        command_name="validate-workbook",
        input_paths=[input_path],
        output_paths=[
            run.artifacts.summary_markdown,
            run.artifacts.results_json,
            run.artifacts.flags_parquet,
        ],
        row_counts={
            "validation_findings": len(run.findings),
            "validation_flags": len(run.findings),
        },
        data_classification=DataClassification.CONFIDENTIAL,
        intended_audience="analyst",
        publish_status_scope="working_layer_findings",
        warnings_count=run.counts["warning"],
        errors_count=run.counts["error"],
    )


def _write_demo_manifest_for_syntheticness(
    *,
    settings: Settings,
    input_path: Path,
    run: SyntheticnessRun,
) -> Path:
    return write_artifact_manifest(
        settings=settings,
        command_name="run-syntheticness",
        input_paths=[input_path],
        output_paths=[run.artifacts.report_markdown, run.artifacts.signals_parquet],
        row_counts={"syntheticness_signals": len(run.report.signals)},
        data_classification=DataClassification.INTERNAL,
        intended_audience="analyst",
        publish_status_scope="experimental_signals",
        warnings_count=run.counts["strong"] + run.counts["moderate"] + run.counts["weak"],
        errors_count=0,
        upstream_artifact_references=[latest_manifest_path(settings, "validate-workbook")],
    )


def _write_demo_manifest_for_reconciliation(
    *,
    settings: Settings,
    core_path: Path,
    reference_dir: Path,
    run: ReconciliationRun,
) -> Path:
    return write_artifact_manifest(
        settings=settings,
        command_name="reconcile",
        input_paths=[core_path, reference_dir],
        output_paths=[
            run.artifacts.reconciled_core_metrics_path,
            run.artifacts.provenance_registry_path,
            run.artifacts.reconciliation_summary_path,
            run.artifacts.reference_coverage_parquet_path,
            run.artifacts.reference_coverage_markdown_path,
        ],
        row_counts={
            "reconciled_core_metrics": len(run.reconciled_core_metrics),
            "provenance_registry": len(run.provenance_registry),
            "reference_coverage": len(run.reference_coverage),
        },
        data_classification=DataClassification.CONFIDENTIAL,
        intended_audience="analyst",
        publish_status_scope="all_gold_rows",
        warnings_count=len(run.warnings),
        errors_count=0,
        upstream_artifact_references=[latest_manifest_path(settings, "validate-workbook")],
    )


def _write_demo_manifest_for_gold_gate(
    *,
    settings: Settings,
    run: GoldGateRun,
) -> Path:
    gold_inputs = [
        settings.data_gold / "reconciled_core_metrics.parquet",
        settings.data_gold / "provenance_registry.parquet",
        settings.data_gold / "validation_flags.parquet",
        settings.data_gold / "reference_coverage.parquet",
        settings.data_gold / "syntheticness_signals.parquet",
    ]
    return write_artifact_manifest(
        settings=settings,
        command_name="gate-gold",
        input_paths=gold_inputs,
        output_paths=[
            run.artifacts.decisions_path,
            run.artifacts.publishable_path,
            run.artifacts.blocked_path,
            run.artifacts.scorecard_markdown_path,
            run.artifacts.summary_json_path,
        ],
        row_counts={
            "decision_rows": len(run.decisions),
            "publishable_rows": int(run.summary["publishable_count"]),
            "advisory_rows": int(run.summary["advisory_count"]),
            "blocked_rows": int(run.summary["blocked_count"]),
        },
        data_classification=DataClassification.INTERNAL,
        intended_audience="release_manager",
        publish_status_scope="publishable_advisory_blocked",
        warnings_count=int(run.summary["advisory_count"]),
        errors_count=int(run.summary["blocked_count"]),
        upstream_artifact_references=[
            latest_manifest_path(settings, "validate-workbook"),
            latest_manifest_path(settings, "run-syntheticness"),
            latest_manifest_path(settings, "reconcile"),
        ],
    )


def _write_demo_summary(
    *,
    output_root: Path,
    input_path: Path,
    selected_brands: tuple[str, ...],
    demo_silver_dir: Path,
    demo_reference_dir: Path,
    demo_settings: Settings,
    validation_run: ValidationRun,
    syntheticness_run: SyntheticnessRun,
    reconciliation_run: ReconciliationRun,
    gold_gate_run: GoldGateRun,
    report_artifacts: ReportArtifacts,
    strategy_run: StrategyRun,
    preflight_run: ReleasePreflightRun,
) -> FiveBrandDemoArtifacts:
    selected_brands_json_path = output_root / "selected_brands.json"
    selected_brands_json_path.write_text(
        json.dumps({"brands": list(selected_brands)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    summary_json_path = output_root / "demo_summary.json"
    summary_markdown_path = output_root / "demo_summary.md"
    payload = {
        "demo_name": "5-brand-happy-path",
        "input_path": format_artifact_path(input_path),
        "output_root": format_artifact_path(output_root),
        "workspace_root": format_artifact_path(demo_settings.artifacts_dir.parent),
        "selected_brands": list(selected_brands),
        "validation": {
            "passed": validation_run.passed,
            "counts": validation_run.counts,
            "summary_path": format_artifact_path(validation_run.artifacts.summary_markdown),
            "results_path": format_artifact_path(validation_run.artifacts.results_json),
        },
        "syntheticness": {
            "counts": syntheticness_run.counts,
            "report_path": format_artifact_path(syntheticness_run.artifacts.report_markdown),
            "signals_path": format_artifact_path(syntheticness_run.artifacts.signals_parquet),
        },
        "reconciliation": {
            "warnings_count": len(reconciliation_run.warnings),
            "summary_path": format_artifact_path(
                reconciliation_run.artifacts.reconciliation_summary_path
            ),
            "reconciled_core_metrics_path": format_artifact_path(
                reconciliation_run.artifacts.reconciled_core_metrics_path
            ),
            "provenance_registry_path": format_artifact_path(
                reconciliation_run.artifacts.provenance_registry_path
            ),
        },
        "gold_gate": {
            "publishable_count": int(gold_gate_run.summary["publishable_count"]),
            "advisory_count": int(gold_gate_run.summary["advisory_count"]),
            "blocked_count": int(gold_gate_run.summary["blocked_count"]),
            "decisions_path": format_artifact_path(gold_gate_run.artifacts.decisions_path),
            "publishable_path": format_artifact_path(gold_gate_run.artifacts.publishable_path),
            "blocked_path": format_artifact_path(gold_gate_run.artifacts.blocked_path),
        },
        "reports": {
            "global_markdown": format_artifact_path(report_artifacts.global_markdown),
            "global_html": format_artifact_path(report_artifacts.global_html),
            "global_json": format_artifact_path(report_artifacts.global_json),
            "brand_report_count": len(report_artifacts.brand_json_paths),
        },
        "strategy": {
            "recommendation_count": len(strategy_run.recommendations),
            "recommendations_parquet_path": format_artifact_path(
                strategy_run.artifacts.recommendations_parquet_path
            ),
            "playbook_path": format_artifact_path(strategy_run.artifacts.playbook_markdown_path),
        },
        "preflight": {
            "passed": preflight_run.passed,
            "failed_check_count": int(preflight_run.summary["failed_check_count"]),
            "warning_check_count": int(preflight_run.summary["warning_check_count"]),
            "summary_json_path": format_artifact_path(preflight_run.artifacts.summary_json_path),
            "summary_markdown_path": format_artifact_path(
                preflight_run.artifacts.summary_markdown_path
            ),
        },
        "demo_inputs": {
            "demo_silver_dir": format_artifact_path(demo_silver_dir),
            "demo_reference_dir": format_artifact_path(demo_reference_dir),
            "selected_brands_path": format_artifact_path(selected_brands_json_path),
        },
    }
    summary_json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    summary_markdown_path.write_text(_render_demo_summary_markdown(payload), encoding="utf-8")
    return FiveBrandDemoArtifacts(
        summary_json_path=summary_json_path,
        summary_markdown_path=summary_markdown_path,
        selected_brands_json_path=selected_brands_json_path,
        demo_silver_dir=demo_silver_dir,
        demo_reference_dir=demo_reference_dir,
    )


def _render_demo_summary_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# 5-Brand Happy-Path Demo",
        "",
        f"- Input workbook: `{payload['input_path']}`",
        f"- Output root: `{payload['output_root']}`",
        f"- Selected brands: `{', '.join(payload['selected_brands'])}`",
        "",
        "## Final Status",
        "",
        f"- Validation passed: `{'yes' if payload['validation']['passed'] else 'no'}`",
        f"- Publishable KPI rows: `{payload['gold_gate']['publishable_count']}`",
        f"- Advisory KPI rows: `{payload['gold_gate']['advisory_count']}`",
        f"- Blocked KPI rows: `{payload['gold_gate']['blocked_count']}`",
        f"- Release preflight passed: `{'yes' if payload['preflight']['passed'] else 'no'}`",
        "",
        "## Key Artifacts",
        "",
        f"- Demo silver slice: `{payload['demo_inputs']['demo_silver_dir']}`",
        f"- Demo reference dir: `{payload['demo_inputs']['demo_reference_dir']}`",
        f"- Validation summary: `{payload['validation']['summary_path']}`",
        f"- Syntheticness report: `{payload['syntheticness']['report_path']}`",
        f"- Reconciliation summary: `{payload['reconciliation']['summary_path']}`",
        f"- Gold decisions: `{payload['gold_gate']['decisions_path']}`",
        f"- Global report JSON: `{payload['reports']['global_json']}`",
        f"- Strategy playbook: `{payload['strategy']['playbook_path']}`",
        f"- Preflight summary: `{payload['preflight']['summary_markdown_path']}`",
        "",
    ]
    return "\n".join(lines) + "\n"


def _brand_slug(value: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "-" for ch in value).strip("-")


def _as_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)


def _as_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)
