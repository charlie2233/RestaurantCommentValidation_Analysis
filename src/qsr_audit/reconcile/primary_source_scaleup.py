"""Primary-source-backed reconciliation over public-chain reference rows."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.config import Settings
from qsr_audit.gold.pipeline import gate_gold_publish
from qsr_audit.reconcile.pipeline import load_reference_catalog, reconcile_core_metrics
from qsr_audit.reconcile.reconciliation import select_best_reference_row
from qsr_audit.reconcile.reference_audit import (
    PRIMARY_SOURCE_DIRECT_SCOPE,
    PRIMARY_SOURCE_SOURCE_TYPES,
)
from qsr_audit.validate import run_syntheticness, validate_workbook

PRIMARY_SOURCE_REFERENCE_FILENAME = "sec_filings_reference.csv"
QSR50_REFERENCE_FILENAME = "qsr50_reference.csv"
PRIMARY_SOURCE_WORKSPACE_DIRNAME = "primary_source_scaleup"
PRIMARY_SOURCE_SOURCE_PRIORITY: dict[str, int] = {
    "sec_filings": 0,
    "investor_relations": 1,
    "qsr50": 2,
    "technomic": 3,
    "franchise_disclosure": 4,
}
PRIMARY_SOURCE_METRIC_SPECS: tuple[tuple[str, str, str, str, str, str], ...] = (
    (
        "store_count",
        "us_store_count_2024",
        "reference_us_store_count_2024",
        "store_count",
        "raw_reference_us_store_count",
        "us_store_count_scope",
    ),
    (
        "system_sales",
        "systemwide_revenue_usd_billions_2024",
        "reference_systemwide_revenue_usd_billions_2024",
        "system_sales",
        "raw_reference_systemwide_revenue_usd_billions",
        "systemwide_revenue_scope",
    ),
    (
        "auv",
        "average_unit_volume_usd_thousands",
        "reference_average_unit_volume_usd_thousands",
        "auv",
        "raw_reference_average_unit_volume_usd_thousands",
        "average_unit_volume_scope",
    ),
)
PRIMARY_SOURCE_DEFAULT_PROVENANCE_GRADES = {
    "sec_filings": "A",
    "investor_relations": "A",
    "qsr50": "B",
    "franchise_disclosure": "B",
    "technomic": "C",
}


@dataclass(frozen=True)
class PrimarySourceArtifacts:
    """Final primary-source reconciliation artifact locations."""

    primary_source_coverage_markdown_path: Path
    primary_source_deltas_csv_path: Path
    primary_source_gold_candidates_parquet_path: Path


@dataclass(frozen=True)
class PrimarySourceRun:
    """Complete primary-source reconciliation slice result."""

    artifacts: PrimarySourceArtifacts
    primary_source_gold_candidates: pd.DataFrame
    primary_source_deltas: pd.DataFrame
    reference_coverage: pd.DataFrame
    warnings: tuple[str, ...]


def run_primary_source_scaleup(
    *,
    core_path: Path,
    reference_dir: Path,
    settings: Settings | None = None,
) -> PrimarySourceRun:
    """Run a primary-source-first reconciliation slice over the current core metrics."""

    resolved_settings = settings or Settings()
    resolved_core = core_path.expanduser().resolve()
    resolved_reference_dir = reference_dir.expanduser().resolve()

    workspace_root = resolved_settings.validate_artifact_root(
        resolved_settings.artifacts_dir / PRIMARY_SOURCE_WORKSPACE_DIRNAME,
        purpose="primary-source reconciliation workspace",
    )
    if workspace_root.exists():
        shutil.rmtree(workspace_root)

    workspace_gold = workspace_root / "gold"
    workspace_reports = workspace_root / "reports"
    primary_reference_dir = _build_primary_source_reference_dir(
        source_reference_dir=resolved_reference_dir,
        workspace_root=workspace_root,
    )

    validate_workbook(
        _validation_source_for_core(resolved_core),
        settings=None,
        output_dir=workspace_reports / "validation",
        gold_dir=workspace_gold,
    )
    run_syntheticness(
        resolved_core,
        settings=None,
        output_dir=workspace_reports / "validation",
        gold_dir=workspace_gold,
    )
    reconciliation_run = reconcile_core_metrics(
        core_path=resolved_core,
        reference_dir=primary_reference_dir,
        settings=None,
        gold_dir=workspace_gold,
        report_dir=workspace_reports / "reconciliation",
        source_priority=PRIMARY_SOURCE_SOURCE_PRIORITY,
    )
    gold_run = gate_gold_publish(
        settings=None,
        gold_dir=workspace_gold,
        report_dir=workspace_reports / "audit",
    )
    reference_frame, _reference_warnings, _ = load_reference_catalog(primary_reference_dir)

    primary_source_deltas = _build_primary_source_deltas(
        reconciled_core_metrics=reconciliation_run.reconciled_core_metrics,
        reference_frame=reference_frame,
        gold_decisions=gold_run.decisions,
    )
    primary_source_gold_candidates = _build_primary_source_gold_candidates(
        gold_decisions=gold_run.decisions,
        primary_source_deltas=primary_source_deltas,
    )
    artifacts = _write_primary_source_outputs(
        settings=resolved_settings,
        primary_source_deltas=primary_source_deltas,
        primary_source_gold_candidates=primary_source_gold_candidates,
        warnings=reconciliation_run.warnings,
    )

    return PrimarySourceRun(
        artifacts=artifacts,
        primary_source_gold_candidates=primary_source_gold_candidates,
        primary_source_deltas=primary_source_deltas,
        reference_coverage=reconciliation_run.reference_coverage,
        warnings=reconciliation_run.warnings,
    )


def _validation_source_for_core(core_path: Path) -> Path:
    if core_path.suffix.lower() == ".parquet":
        return core_path.parent
    return core_path


def _build_primary_source_reference_dir(
    *, source_reference_dir: Path, workspace_root: Path
) -> Path:
    reference_dir = workspace_root / "reference"
    reference_dir.mkdir(parents=True, exist_ok=True)

    qsr50_source_path = source_reference_dir / QSR50_REFERENCE_FILENAME
    if qsr50_source_path.exists():
        shutil.copy2(qsr50_source_path, reference_dir / QSR50_REFERENCE_FILENAME)

    primary_source_path = source_reference_dir / PRIMARY_SOURCE_REFERENCE_FILENAME
    if not primary_source_path.exists():
        raise FileNotFoundError(
            f"Primary-source reconciliation requires `{PRIMARY_SOURCE_REFERENCE_FILENAME}` under "
            f"`{source_reference_dir}`."
        )
    shutil.copy2(primary_source_path, reference_dir / PRIMARY_SOURCE_REFERENCE_FILENAME)
    return reference_dir


def _build_primary_source_gold_candidates(
    *,
    gold_decisions: pd.DataFrame,
    primary_source_deltas: pd.DataFrame,
) -> pd.DataFrame:
    if gold_decisions.empty:
        return pd.DataFrame(
            columns=[
                "brand_name",
                "canonical_brand_name",
                "metric_name",
                "metric_value",
                "source_type",
                "source_name",
                "source_locator",
                "as_of_date",
                "method_reported_or_estimated",
                "confidence_score",
                "provenance_grade",
                "reconciliation_grade",
                "reconciliation_relative_error",
                "reconciliation_absolute_error",
                "reference_evidence_present",
                "reference_source_count",
                "publish_status_candidate",
            ]
        )

    deltas_lookup = primary_source_deltas.set_index(
        ["canonical_brand_name", "metric_name"]
    ).to_dict(orient="index")
    frame = gold_decisions.loc[
        gold_decisions["metric_name"].isin(["store_count", "system_sales", "auv"])
    ].copy()
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "brand_name",
                "canonical_brand_name",
                "metric_name",
                "metric_value",
                "source_type",
                "source_name",
                "source_locator",
                "as_of_date",
                "method_reported_or_estimated",
                "confidence_score",
                "provenance_grade",
                "reconciliation_grade",
                "reconciliation_relative_error",
                "reconciliation_absolute_error",
                "reference_evidence_present",
                "reference_source_count",
                "publish_status_candidate",
            ]
        )

    frame["selected_source_tier"] = frame.apply(
        lambda row: deltas_lookup.get((row["canonical_brand_name"], row["metric_name"]), {}).get(
            "selected_source_tier"
        ),
        axis=1,
    )
    frame = frame.loc[frame["selected_source_tier"].eq("primary_source")].copy()
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "brand_name",
                "canonical_brand_name",
                "metric_name",
                "metric_value",
                "source_type",
                "source_name",
                "source_locator",
                "as_of_date",
                "method_reported_or_estimated",
                "confidence_score",
                "provenance_grade",
                "reconciliation_grade",
                "reconciliation_relative_error",
                "reconciliation_absolute_error",
                "reference_evidence_present",
                "reference_source_count",
                "publish_status_candidate",
            ]
        )

    frame = frame.rename(columns={"publish_status": "publish_status_candidate"})
    for destination, source in (
        ("source_type", "source_type"),
        ("source_name", "source_name"),
        ("source_locator", "source_locator"),
        ("as_of_date", "as_of_date"),
        ("method_reported_or_estimated", "method_reported_or_estimated"),
        ("confidence_score", "confidence_score"),
        ("provenance_grade", "provenance_grade"),
    ):
        frame[destination] = frame.apply(
            lambda row, field_name=source: deltas_lookup.get(
                (row["canonical_brand_name"], row["metric_name"]), {}
            ).get(field_name),
            axis=1,
        )
    selected_columns = [
        "brand_name",
        "canonical_brand_name",
        "metric_name",
        "metric_value",
        "source_type",
        "source_name",
        "source_locator",
        "as_of_date",
        "method_reported_or_estimated",
        "confidence_score",
        "provenance_grade",
        "reconciliation_grade",
        "reconciliation_relative_error",
        "reconciliation_absolute_error",
        "reference_evidence_present",
        "reference_source_count",
        "publish_status_candidate",
    ]
    return frame.loc[:, selected_columns].sort_values(
        by=["canonical_brand_name", "metric_name"],
        kind="stable",
        ignore_index=True,
    )


def _build_primary_source_deltas(
    *,
    reconciled_core_metrics: pd.DataFrame,
    reference_frame: pd.DataFrame,
    gold_decisions: pd.DataFrame,
) -> pd.DataFrame:
    decision_lookup = gold_decisions.set_index(["canonical_brand_name", "metric_name"])[
        "publish_status"
    ].to_dict()
    rows: list[dict[str, Any]] = []

    for record in reconciled_core_metrics.to_dict(orient="records"):
        canonical_name = str(record["canonical_brand_name"])
        matched_refs = reference_frame.loc[
            reference_frame["canonical_brand_name"].eq(canonical_name)
        ].copy()
        primary_refs = matched_refs.loc[
            matched_refs["source_type"].isin(PRIMARY_SOURCE_SOURCE_TYPES)
        ].copy()

        for (
            metric_name,
            workbook_column,
            reference_column,
            prefix,
            raw_metric_column,
            scope_column,
        ) in PRIMARY_SOURCE_METRIC_SPECS:
            best_reference = select_best_reference_row(
                matched_refs,
                field_name=reference_column,
                source_priority=PRIMARY_SOURCE_SOURCE_PRIORITY,
            )
            primary_metric_rows = primary_refs.loc[primary_refs[raw_metric_column].notna()].copy()
            direct_primary_rows = primary_metric_rows.loc[
                primary_metric_rows[scope_column].eq(PRIMARY_SOURCE_DIRECT_SCOPE)
            ].copy()
            scope_mismatch_rows = primary_metric_rows.loc[
                primary_metric_rows[scope_column].eq("scope_mismatch")
            ].copy()

            if (
                best_reference is not None
                and str(best_reference.get("source_type") or "") in PRIMARY_SOURCE_SOURCE_TYPES
            ):
                primary_source_status = "used_primary_source"
            elif not scope_mismatch_rows.empty:
                primary_source_status = "scope_mismatch"
            elif not direct_primary_rows.empty:
                primary_source_status = "primary_source_available"
            else:
                primary_source_status = "no_primary_source"

            rows.append(
                {
                    "brand_name": record["brand_name"],
                    "canonical_brand_name": canonical_name,
                    "metric_name": metric_name,
                    "workbook_value": record.get(workbook_column),
                    "reference_value": record.get(f"{prefix}_reference_value"),
                    "absolute_error": record.get(f"{prefix}_absolute_error"),
                    "relative_error": record.get(f"{prefix}_relative_error"),
                    "source_type": None
                    if best_reference is None
                    else best_reference.get("source_type"),
                    "source_name": None
                    if best_reference is None
                    else best_reference.get("source_name"),
                    "source_locator": None
                    if best_reference is None
                    else best_reference.get("source_url_or_doc_id"),
                    "as_of_date": None
                    if best_reference is None
                    else best_reference.get("as_of_date"),
                    "method_reported_or_estimated": None
                    if best_reference is None
                    else best_reference.get("method_reported_or_estimated"),
                    "confidence_score": None
                    if best_reference is None
                    else best_reference.get("confidence_score"),
                    "provenance_grade": _provenance_grade_for_row(best_reference),
                    "publish_status_candidate": decision_lookup.get((canonical_name, metric_name)),
                    "selected_source_tier": _selected_source_tier(best_reference),
                    "primary_source_status": primary_source_status,
                    "scope_notes": _scope_notes_for_metric(
                        primary_metric_rows=primary_metric_rows,
                        direct_primary_rows=direct_primary_rows,
                        scope_mismatch_rows=scope_mismatch_rows,
                    ),
                }
            )

    return pd.DataFrame(rows).sort_values(
        by=["canonical_brand_name", "metric_name"],
        kind="stable",
        ignore_index=True,
    )


def _write_primary_source_outputs(
    *,
    settings: Settings,
    primary_source_deltas: pd.DataFrame,
    primary_source_gold_candidates: pd.DataFrame,
    warnings: tuple[str, ...],
) -> PrimarySourceArtifacts:
    reconciliation_dir = settings.reports_dir / "reconciliation"
    reconciliation_dir.mkdir(parents=True, exist_ok=True)
    settings.data_gold.mkdir(parents=True, exist_ok=True)

    artifacts = PrimarySourceArtifacts(
        primary_source_coverage_markdown_path=reconciliation_dir / "primary_source_coverage.md",
        primary_source_deltas_csv_path=reconciliation_dir / "primary_source_deltas.csv",
        primary_source_gold_candidates_parquet_path=settings.data_gold
        / "primary_source_gold_candidates.parquet",
    )

    primary_source_deltas.to_csv(
        artifacts.primary_source_deltas_csv_path,
        index=False,
        encoding="utf-8",
    )
    primary_source_gold_candidates.to_parquet(
        artifacts.primary_source_gold_candidates_parquet_path,
        index=False,
    )
    artifacts.primary_source_coverage_markdown_path.write_text(
        _render_primary_source_coverage(
            primary_source_deltas=primary_source_deltas,
            primary_source_gold_candidates=primary_source_gold_candidates,
            warnings=warnings,
        ),
        encoding="utf-8",
    )
    return artifacts


def _render_primary_source_coverage(
    *,
    primary_source_deltas: pd.DataFrame,
    primary_source_gold_candidates: pd.DataFrame,
    warnings: tuple[str, ...],
) -> str:
    if primary_source_deltas.empty:
        return (
            "# Primary-Source Coverage Summary\n\n"
            "- No primary-source-backed reconciliation rows were produced.\n"
        )

    brand_names = sorted(primary_source_deltas["canonical_brand_name"].dropna().unique())
    used_primary = primary_source_deltas.loc[
        primary_source_deltas["selected_source_tier"].eq("primary_source")
    ].copy()
    scope_mismatch_rows = primary_source_deltas.loc[
        primary_source_deltas["primary_source_status"].eq("scope_mismatch")
    ].copy()
    publishable_rows = primary_source_gold_candidates.loc[
        primary_source_gold_candidates["publish_status_candidate"].eq("publishable")
    ].copy()
    unresolved_warnings = [
        warning for warning in warnings if "did not exact-resolve" in warning.lower()
    ]

    lines = [
        "# Primary-Source Coverage Summary",
        "",
        f"- Core brands audited: `{len(brand_names)}`",
        "- Brands with directly comparable primary-source coverage: "
        f"`{used_primary['canonical_brand_name'].nunique()}`",
        "- Brands still missing directly comparable primary-source coverage: "
        f"`{len(set(brand_names) - set(used_primary['canonical_brand_name'].dropna().unique()))}`",
        f"- Metrics with scope mismatches: `{len(scope_mismatch_rows)}`",
        f"- Primary-source-backed publishable candidate rows: `{len(publishable_rows)}`",
        "",
        "## Brand Coverage",
        "",
        "| Brand | Primary-backed metrics | Scope-mismatch metrics | Publishable candidate metrics |",
        "| --- | --- | --- | --- |",
    ]

    publishable_by_brand = (
        publishable_rows.groupby("canonical_brand_name", sort=True)["metric_name"]
        .agg(lambda series: ", ".join(sorted(str(value) for value in series.tolist())))
        .to_dict()
    )
    for canonical_name in brand_names:
        brand_rows = primary_source_deltas.loc[
            primary_source_deltas["canonical_brand_name"].eq(canonical_name)
        ].copy()
        primary_metrics = (
            ", ".join(
                sorted(
                    brand_rows.loc[
                        brand_rows["selected_source_tier"].eq("primary_source"),
                        "metric_name",
                    ]
                    .dropna()
                    .astype(str)
                    .unique()
                )
            )
            or "none"
        )
        mismatch_metrics = (
            ", ".join(
                sorted(
                    brand_rows.loc[
                        brand_rows["primary_source_status"].eq("scope_mismatch"),
                        "metric_name",
                    ]
                    .dropna()
                    .astype(str)
                    .unique()
                )
            )
            or "none"
        )
        publishable_metrics = publishable_by_brand.get(canonical_name, "none")
        lines.append(
            f"| {canonical_name} | {primary_metrics} | {mismatch_metrics} | {publishable_metrics} |"
        )

    lines.extend(["", "## Brands Still Missing Direct Primary Coverage", ""])
    missing_brand_rows = primary_source_deltas.loc[
        ~primary_source_deltas["canonical_brand_name"].isin(
            used_primary["canonical_brand_name"].dropna().unique()
        )
    ].copy()
    if missing_brand_rows.empty:
        lines.append("- None.")
    else:
        for canonical_name, frame in missing_brand_rows.groupby("canonical_brand_name", sort=True):
            missing_metrics = ", ".join(sorted(frame["metric_name"].astype(str).unique()))
            lines.append(
                f"- `{canonical_name}`: no directly comparable primary-source row for `{missing_metrics}`."
            )

    lines.extend(["", "## Scope Mismatches", ""])
    if scope_mismatch_rows.empty:
        lines.append("- None.")
    else:
        for row in scope_mismatch_rows.to_dict(orient="records"):
            lines.append(
                f"- `{row['canonical_brand_name']}` `{row['metric_name']}`: "
                f"{row.get('scope_notes') or 'Primary source is not directly comparable to workbook scope.'}"
            )

    lines.extend(["", "## Publishable Candidate Rows Backed By Primary Sources", ""])
    if publishable_rows.empty:
        lines.append("- None.")
    else:
        for row in publishable_rows.to_dict(orient="records"):
            lines.append(
                f"- `{row['canonical_brand_name']}` `{row['metric_name']}` via "
                f"`{row['source_type']}` ({row['source_name']})."
            )

    lines.extend(["", "## Ambiguous / Unresolved Mappings", ""])
    if unresolved_warnings:
        for warning in unresolved_warnings:
            lines.append(f"- {warning}")
    else:
        lines.append("- None.")

    lines.extend(["", "## Contradictions Requiring Review", ""])
    contradiction_rows = primary_source_deltas.copy()
    contradiction_rows["abs_relative_error"] = pd.to_numeric(
        contradiction_rows["relative_error"],
        errors="coerce",
    ).abs()
    contradiction_rows = contradiction_rows.loc[
        contradiction_rows["reference_value"].notna()
        & contradiction_rows["abs_relative_error"].gt(0.05)
    ].sort_values(
        by=["abs_relative_error", "canonical_brand_name", "metric_name"],
        ascending=[False, True, True],
        kind="stable",
    )
    if contradiction_rows.empty:
        lines.append("- None above the 5% relative-error review threshold.")
    else:
        for row in contradiction_rows.head(10).to_dict(orient="records"):
            lines.append(
                f"- `{row['canonical_brand_name']}` `{row['metric_name']}`: workbook "
                f"`{row['workbook_value']}` vs reference `{row['reference_value']}` "
                f"({row['abs_relative_error']:.1%} relative error)."
            )

    return "\n".join(lines) + "\n"


def _selected_source_tier(best_reference: pd.Series | None) -> str:
    if best_reference is None:
        return "missing"
    if str(best_reference.get("source_type") or "") in PRIMARY_SOURCE_SOURCE_TYPES:
        return "primary_source"
    return "secondary_fallback"


def _provenance_grade_for_row(best_reference: pd.Series | None) -> str | None:
    if best_reference is None:
        return None
    provenance_grade = best_reference.get("provenance_grade")
    if provenance_grade is not None and str(provenance_grade).strip():
        return str(provenance_grade)
    return PRIMARY_SOURCE_DEFAULT_PROVENANCE_GRADES.get(
        str(best_reference.get("source_type") or ""), "UNKNOWN"
    )


def _scope_notes_for_metric(
    *,
    primary_metric_rows: pd.DataFrame,
    direct_primary_rows: pd.DataFrame,
    scope_mismatch_rows: pd.DataFrame,
) -> str | None:
    if not direct_primary_rows.empty:
        notes = [
            str(value).strip()
            for value in direct_primary_rows["scope_notes"].dropna().tolist()
            if str(value).strip()
        ]
        return (
            " | ".join(sorted(dict.fromkeys(notes))) or "Directly comparable primary-source metric."
        )
    if not scope_mismatch_rows.empty:
        notes = [
            str(value).strip()
            for value in scope_mismatch_rows["scope_notes"].dropna().tolist()
            if str(value).strip()
        ]
        return (
            " | ".join(sorted(dict.fromkeys(notes)))
            or "Primary-source row exists but is not directly comparable."
        )
    if primary_metric_rows.empty:
        return None
    notes = [
        str(value).strip()
        for value in primary_metric_rows["scope_notes"].dropna().tolist()
        if str(value).strip()
    ]
    return " | ".join(sorted(dict.fromkeys(notes))) or None


__all__ = [
    "PRIMARY_SOURCE_SOURCE_PRIORITY",
    "PrimarySourceArtifacts",
    "PrimarySourceRun",
    "run_primary_source_scaleup",
]
