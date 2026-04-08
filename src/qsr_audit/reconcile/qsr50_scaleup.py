"""QSR50-only scale-up reconciliation over a broader workbook slice."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.config import Settings
from qsr_audit.gold import gate_gold_publish
from qsr_audit.reconcile.pipeline import load_reference_catalog, reconcile_core_metrics
from qsr_audit.reconcile.reconciliation import select_best_reference_row
from qsr_audit.validate import run_syntheticness, validate_workbook

QSR50_REFERENCE_FILENAME = "qsr50_reference.csv"
QSR50_WORKSPACE_DIRNAME = "qsr50_scaleup"
QSR50_METRIC_NAMES: tuple[str, ...] = ("rank", "store_count", "system_sales", "auv")
DELTA_FIELD_SPECS: tuple[tuple[str, str, str], ...] = (
    ("rank", "reference_rank", "rank"),
    ("us_store_count_2024", "reference_us_store_count_2024", "store_count"),
    (
        "systemwide_revenue_usd_billions_2024",
        "reference_systemwide_revenue_usd_billions_2024",
        "system_sales",
    ),
    ("average_unit_volume_usd_thousands", "reference_average_unit_volume_usd_thousands", "auv"),
)


@dataclass(frozen=True)
class QSR50ScaleupArtifacts:
    """Final QSR50 scale-up artifact locations."""

    qsr50_coverage_markdown_path: Path
    brand_deltas_full_csv_path: Path
    qsr50_gold_candidates_parquet_path: Path
    unresolved_reference_gaps_markdown_path: Path


@dataclass(frozen=True)
class QSR50ScaleupRun:
    """Complete QSR50 scale-up run result."""

    artifacts: QSR50ScaleupArtifacts
    qsr50_gold_candidates: pd.DataFrame
    brand_deltas_full: pd.DataFrame
    reference_coverage: pd.DataFrame
    warnings: tuple[str, ...]


def run_qsr50_scaleup(
    *,
    core_path: Path,
    reference_dir: Path,
    settings: Settings | None = None,
) -> QSR50ScaleupRun:
    """Run a QSR50-only reconciliation slice without mutating the general Gold state."""

    resolved_settings = settings or Settings()
    resolved_core = core_path.expanduser().resolve()
    resolved_reference_dir = reference_dir.expanduser().resolve()

    workspace_root = resolved_settings.validate_artifact_root(
        resolved_settings.artifacts_dir / QSR50_WORKSPACE_DIRNAME,
        purpose="QSR50 scale-up workspace",
    )
    if workspace_root.exists():
        shutil.rmtree(workspace_root)

    workspace_gold = workspace_root / "gold"
    workspace_reports = workspace_root / "reports"
    qsr50_reference_dir = _build_qsr50_reference_dir(
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
        reference_dir=qsr50_reference_dir,
        settings=None,
        gold_dir=workspace_gold,
        report_dir=workspace_reports / "reconciliation",
    )
    gold_run = gate_gold_publish(
        settings=None,
        gold_dir=workspace_gold,
        report_dir=workspace_reports / "audit",
    )
    reference_frame, _reference_warnings, _ = load_reference_catalog(qsr50_reference_dir)

    qsr50_gold_candidates = _build_qsr50_gold_candidates(gold_run.decisions)
    brand_deltas_full = _build_brand_deltas_full(
        reconciled_core_metrics=reconciliation_run.reconciled_core_metrics,
        reference_frame=reference_frame,
        gold_decisions=gold_run.decisions,
    )
    artifacts = _write_qsr50_outputs(
        settings=resolved_settings,
        reference_coverage=reconciliation_run.reference_coverage,
        qsr50_gold_candidates=qsr50_gold_candidates,
        brand_deltas_full=brand_deltas_full,
        warnings=reconciliation_run.warnings,
    )

    return QSR50ScaleupRun(
        artifacts=artifacts,
        qsr50_gold_candidates=qsr50_gold_candidates,
        brand_deltas_full=brand_deltas_full,
        reference_coverage=reconciliation_run.reference_coverage,
        warnings=reconciliation_run.warnings,
    )


def _validation_source_for_core(core_path: Path) -> Path:
    if core_path.suffix.lower() == ".parquet":
        return core_path.parent
    return core_path


def _build_qsr50_reference_dir(*, source_reference_dir: Path, workspace_root: Path) -> Path:
    reference_dir = workspace_root / "reference"
    reference_dir.mkdir(parents=True, exist_ok=True)

    source_path = source_reference_dir / QSR50_REFERENCE_FILENAME
    template_path = source_reference_dir / "templates" / QSR50_REFERENCE_FILENAME
    if source_path.exists():
        shutil.copy2(source_path, reference_dir / QSR50_REFERENCE_FILENAME)
        return reference_dir
    if template_path.exists():
        (reference_dir / "templates").mkdir(parents=True, exist_ok=True)
        shutil.copy2(template_path, reference_dir / "templates" / QSR50_REFERENCE_FILENAME)
        return reference_dir
    raise FileNotFoundError(
        f"QSR50 scale-up requires `{QSR50_REFERENCE_FILENAME}` under `{source_reference_dir}` "
        "or its `templates/` directory."
    )


def _build_qsr50_gold_candidates(decisions: pd.DataFrame) -> pd.DataFrame:
    if decisions.empty:
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
                "reconciliation_grade",
                "reconciliation_relative_error",
                "reconciliation_absolute_error",
                "reference_evidence_present",
                "reference_source_count",
                "publish_status_candidate",
            ]
        )

    frame = decisions.loc[
        decisions["metric_name"].isin(QSR50_METRIC_NAMES) & decisions["source_type"].eq("qsr50")
    ].copy()
    if frame.empty:
        return frame

    frame = frame.rename(
        columns={
            "source_url_or_doc_id": "source_locator",
            "publish_status": "publish_status_candidate",
        }
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


def _brand_publish_recommendations(decisions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if decisions.empty:
        return pd.DataFrame(
            columns=[
                "canonical_brand_name",
                "brand_publish_status_recommendation",
                "publishable_metric_count",
                "advisory_metric_count",
                "blocked_metric_count",
            ]
        )

    for canonical_brand_name, frame in decisions.groupby("canonical_brand_name", sort=True):
        counts = frame["publish_status"].value_counts().to_dict()
        publishable = int(counts.get("publishable", 0))
        advisory = int(counts.get("advisory", 0))
        blocked = int(counts.get("blocked", 0))
        if blocked and publishable:
            recommendation = "publishable_subset_only"
        elif blocked:
            recommendation = "blocked_for_external_use"
        elif advisory and publishable:
            recommendation = "publishable_subset_only"
        elif advisory:
            recommendation = "advisory_only"
        else:
            recommendation = "publishable"
        rows.append(
            {
                "canonical_brand_name": canonical_brand_name,
                "brand_publish_status_recommendation": recommendation,
                "publishable_metric_count": publishable,
                "advisory_metric_count": advisory,
                "blocked_metric_count": blocked,
            }
        )

    return pd.DataFrame(rows).sort_values(
        by="canonical_brand_name",
        kind="stable",
        ignore_index=True,
    )


def _build_brand_deltas_full(
    *,
    reconciled_core_metrics: pd.DataFrame,
    reference_frame: pd.DataFrame,
    gold_decisions: pd.DataFrame,
) -> pd.DataFrame:
    decision_lookup = gold_decisions.set_index(["canonical_brand_name", "metric_name"])[
        "publish_status"
    ].to_dict()
    recommendation_lookup = (
        _brand_publish_recommendations(gold_decisions)
        .set_index("canonical_brand_name")["brand_publish_status_recommendation"]
        .to_dict()
    )

    rows: list[dict[str, Any]] = []
    for record in reconciled_core_metrics.to_dict(orient="records"):
        canonical_name = str(record["canonical_brand_name"])
        matched_refs = reference_frame.loc[
            reference_frame["canonical_brand_name"].eq(canonical_name)
        ].copy()
        for workbook_column, reference_column, metric_name in DELTA_FIELD_SPECS:
            best_reference = select_best_reference_row(matched_refs, field_name=reference_column)
            prefix = metric_name
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
                    "publish_status_candidate": decision_lookup.get((canonical_name, metric_name)),
                    "brand_publish_status_recommendation": recommendation_lookup.get(
                        canonical_name
                    ),
                }
            )

    return pd.DataFrame(rows).sort_values(
        by=["canonical_brand_name", "metric_name"],
        kind="stable",
        ignore_index=True,
    )


def _write_qsr50_outputs(
    *,
    settings: Settings,
    reference_coverage: pd.DataFrame,
    qsr50_gold_candidates: pd.DataFrame,
    brand_deltas_full: pd.DataFrame,
    warnings: tuple[str, ...],
) -> QSR50ScaleupArtifacts:
    reconciliation_dir = settings.reports_dir / "reconciliation"
    summary_dir = settings.reports_dir / "summary"
    reconciliation_dir.mkdir(parents=True, exist_ok=True)
    summary_dir.mkdir(parents=True, exist_ok=True)
    settings.data_gold.mkdir(parents=True, exist_ok=True)

    artifacts = QSR50ScaleupArtifacts(
        qsr50_coverage_markdown_path=reconciliation_dir / "qsr50_coverage.md",
        brand_deltas_full_csv_path=reconciliation_dir / "brand_deltas_full.csv",
        qsr50_gold_candidates_parquet_path=settings.data_gold / "qsr50_gold_candidates.parquet",
        unresolved_reference_gaps_markdown_path=summary_dir / "unresolved_reference_gaps.md",
    )

    qsr50_gold_candidates.to_parquet(artifacts.qsr50_gold_candidates_parquet_path, index=False)
    brand_deltas_full.to_csv(
        artifacts.brand_deltas_full_csv_path,
        index=False,
        encoding="utf-8",
    )
    artifacts.qsr50_coverage_markdown_path.write_text(
        _render_qsr50_coverage(
            reference_coverage=reference_coverage,
            qsr50_gold_candidates=qsr50_gold_candidates,
            warnings=warnings,
        ),
        encoding="utf-8",
    )
    artifacts.unresolved_reference_gaps_markdown_path.write_text(
        _render_unresolved_reference_gaps(
            reference_coverage=reference_coverage,
            qsr50_gold_candidates=qsr50_gold_candidates,
            brand_deltas_full=brand_deltas_full,
            warnings=warnings,
        ),
        encoding="utf-8",
    )
    return artifacts


def _render_qsr50_coverage(
    *,
    reference_coverage: pd.DataFrame,
    qsr50_gold_candidates: pd.DataFrame,
    warnings: tuple[str, ...],
) -> str:
    brand_rows = reference_coverage.loc[reference_coverage["coverage_kind"].eq("brand")].copy()
    covered_rows = brand_rows.loc[brand_rows["reference_source_count"].fillna(0).astype(int).gt(0)]
    missing_rows = brand_rows.loc[brand_rows["reference_source_count"].fillna(0).astype(int).eq(0)]
    publishable_rows = qsr50_gold_candidates.loc[
        qsr50_gold_candidates["publish_status_candidate"].eq("publishable")
    ].copy()
    unresolved_warnings = [warning for warning in warnings if "did not exact-resolve" in warning]

    lines = [
        "# QSR50 Coverage Summary",
        "",
        f"- Core brands audited: `{len(brand_rows)}`",
        f"- Brands with QSR50 coverage: `{len(covered_rows)}`",
        f"- Brands still missing QSR50 coverage: `{len(missing_rows)}`",
        f"- QSR50-backed publishable candidate rows: `{len(publishable_rows)}`",
        f"- Ambiguous or unresolved reference rows: `{len(unresolved_warnings)}`",
        "",
        "## Covered Brands",
        "",
        "| Brand | QSR50 rows | Covered metrics | Missing metrics | Publishable candidate metrics |",
        "| --- | ---: | ---: | --- | --- |",
    ]

    if covered_rows.empty:
        lines.append("| _(none)_ | 0 | 0 | all | none |")
    else:
        publishable_by_brand = (
            publishable_rows.groupby("canonical_brand_name", sort=True)["metric_name"]
            .agg(lambda series: ", ".join(sorted(str(value) for value in series.tolist())))
            .to_dict()
        )
        for row in covered_rows.sort_values("canonical_brand_name", kind="stable").to_dict(
            orient="records"
        ):
            missing_metrics = ", ".join(row.get("missing_metrics") or []) or "-"
            publishable_metrics = publishable_by_brand.get(row["canonical_brand_name"], "none")
            lines.append(
                "| "
                + f"{row['canonical_brand_name']} | "
                + f"{int(row['reference_row_count'])} | "
                + f"{int(row['covered_metrics_count'])} | "
                + f"{missing_metrics} | "
                + f"{publishable_metrics} |"
            )

    lines.extend(["", "## Missing QSR50 Coverage", ""])
    if missing_rows.empty:
        lines.append("- None.")
    else:
        for row in missing_rows.sort_values("canonical_brand_name", kind="stable").to_dict(
            orient="records"
        ):
            missing_metrics = ", ".join(row.get("missing_metrics") or []) or "all"
            lines.append(f"- `{row['canonical_brand_name']}`: missing metrics `{missing_metrics}`.")

    lines.extend(["", "## Ambiguous / Unresolved Reference Rows", ""])
    if unresolved_warnings:
        for warning in unresolved_warnings:
            lines.append(f"- {warning}")
    else:
        lines.append("- None.")

    return "\n".join(lines) + "\n"


def _render_unresolved_reference_gaps(
    *,
    reference_coverage: pd.DataFrame,
    qsr50_gold_candidates: pd.DataFrame,
    brand_deltas_full: pd.DataFrame,
    warnings: tuple[str, ...],
) -> str:
    brand_rows = reference_coverage.loc[reference_coverage["coverage_kind"].eq("brand")].copy()
    missing_rows = brand_rows.loc[brand_rows["reference_source_count"].fillna(0).astype(int).eq(0)]
    partial_rows = brand_rows.loc[
        brand_rows["reference_source_count"].fillna(0).astype(int).gt(0)
        & brand_rows["missing_metrics"].map(bool)
    ]
    unresolved_warnings = [warning for warning in warnings if "did not exact-resolve" in warning]

    contradiction_rows = brand_deltas_full.copy()
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

    publishable_rows = qsr50_gold_candidates.loc[
        qsr50_gold_candidates["publish_status_candidate"].eq("publishable")
    ]

    lines = [
        "# Unresolved QSR50 Reference Gaps",
        "",
        "## Brands Still Missing QSR50 Coverage",
        "",
    ]
    if missing_rows.empty:
        lines.append("- None.")
    else:
        for row in missing_rows.sort_values("canonical_brand_name", kind="stable").to_dict(
            orient="records"
        ):
            missing_metrics = ", ".join(row.get("missing_metrics") or []) or "all"
            lines.append(f"- `{row['canonical_brand_name']}`: missing metrics `{missing_metrics}`.")

    lines.extend(["", "## Partial QSR50 Coverage", ""])
    if partial_rows.empty:
        lines.append("- None.")
    else:
        for row in partial_rows.sort_values("canonical_brand_name", kind="stable").to_dict(
            orient="records"
        ):
            lines.append(
                f"- `{row['canonical_brand_name']}`: still missing `{', '.join(row.get('missing_metrics') or [])}`."
            )

    lines.extend(["", "## Ambiguous / Unresolved Mappings", ""])
    if unresolved_warnings:
        for warning in unresolved_warnings:
            lines.append(f"- {warning}")
    else:
        lines.append("- None.")

    lines.extend(["", "## Contradictions Requiring Review", ""])
    if contradiction_rows.empty:
        lines.append("- None above the 5% relative-error review threshold.")
    else:
        for row in contradiction_rows.head(10).to_dict(orient="records"):
            lines.append(
                f"- `{row['canonical_brand_name']}` `{row['metric_name']}`: "
                f"workbook `{row['workbook_value']}` vs reference `{row['reference_value']}` "
                f"({row['abs_relative_error']:.1%} relative error)."
            )

    lines.extend(["", "## Publishable Candidates After QSR50 Reconciliation", ""])
    if publishable_rows.empty:
        lines.append("- None yet.")
    else:
        grouped = (
            publishable_rows.groupby("canonical_brand_name", sort=True)["metric_name"]
            .agg(lambda series: ", ".join(sorted(str(value) for value in series.tolist())))
            .to_dict()
        )
        for brand_name, metrics in grouped.items():
            lines.append(f"- `{brand_name}`: publishable candidate metrics `{metrics}`.")

    return "\n".join(lines) + "\n"
