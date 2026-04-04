"""Gold-layer reconciliation orchestration."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.config import Settings
from qsr_audit.reconcile.entity_resolution import canonical_brand_dictionary, resolve_brand_name
from qsr_audit.reconcile.provenance import (
    ProvenanceRecord,
    ProvenanceRegistry,
    build_provenance_record,
)
from qsr_audit.reconcile.reconciliation import (
    compare_numeric_field,
    compare_rank_field,
    overall_reconciliation_grade,
    select_best_reference_row,
)

REFERENCE_TEMPLATE_FILES = (
    "qsr50_reference.csv",
    "technomic_reference.csv",
    "sec_filings_reference.csv",
    "franchise_disclosure_reference.csv",
)


@dataclass(frozen=True)
class ReconciliationArtifacts:
    """Files produced by the reconciliation workflow."""

    reconciled_core_metrics_path: Path
    provenance_registry_path: Path
    reconciliation_summary_path: Path


@dataclass(frozen=True)
class ReconciliationRun:
    """Complete reconciliation result."""

    core_path: Path
    reference_dir: Path
    reconciled_core_metrics: pd.DataFrame
    provenance_registry: pd.DataFrame
    warnings: tuple[str, ...]
    artifacts: ReconciliationArtifacts


def reconcile_core_metrics(
    core_path: Path,
    reference_dir: Path,
    settings: Settings | None = None,
    *,
    gold_dir: Path | None = None,
    report_dir: Path | None = None,
) -> ReconciliationRun:
    """Reconcile normalized core metrics against manual reference files."""

    resolved_core = core_path.expanduser().resolve()
    resolved_reference_dir = reference_dir.expanduser().resolve()
    core_frame = pd.read_parquet(resolved_core)

    reference_frame, reference_warnings, reference_registry = load_reference_catalog(
        resolved_reference_dir
    )
    reconciled_frame, row_warnings, workbook_registry = build_reconciled_core_metrics(
        core_frame,
        reference_frame,
        core_source_path=resolved_core,
    )

    warnings = tuple(reference_warnings + row_warnings)
    registry = workbook_registry.extend(reference_registry.records)
    provenance_frame = registry.to_frame()

    resolved_gold_dir = gold_dir or (
        settings.data_gold if settings is not None else Path("data/gold")
    )
    resolved_report_dir = report_dir or (
        (settings.reports_dir / "reconciliation")
        if settings is not None
        else Path("reports/reconciliation")
    )
    artifacts = write_reconciliation_outputs(
        reconciled_frame,
        provenance_frame,
        warnings=warnings,
        core_path=resolved_core,
        reference_dir=resolved_reference_dir,
        gold_dir=resolved_gold_dir,
        report_dir=resolved_report_dir,
    )
    return ReconciliationRun(
        core_path=resolved_core,
        reference_dir=resolved_reference_dir,
        reconciled_core_metrics=reconciled_frame,
        provenance_registry=provenance_frame,
        warnings=warnings,
        artifacts=artifacts,
    )


def load_reference_catalog(
    reference_dir: Path,
) -> tuple[pd.DataFrame, list[str], ProvenanceRegistry]:
    """Load manual reference rows from the reference directory."""

    frames: list[pd.DataFrame] = []
    warnings: list[str] = []
    registry = ProvenanceRegistry()

    for file_name in REFERENCE_TEMPLATE_FILES:
        source_path = reference_dir / file_name
        template_path = reference_dir / "templates" / file_name
        load_path = source_path if source_path.exists() else template_path

        if not load_path.exists():
            warnings.append(
                f"Reference file `{file_name}` is missing, so that source contributes no coverage."
            )
            continue

        frame = pd.read_csv(load_path, dtype=str)
        if frame.empty:
            if load_path == template_path:
                warnings.append(
                    f"No populated `{file_name}` was found in `{reference_dir}`. The committed template is available but contains no reference rows."
                )
            else:
                warnings.append(
                    f"Reference file `{source_path.name}` is present but has no populated rows."
                )
            continue

        standardized = standardize_reference_frame(frame, source_file_name=file_name)
        frames.append(standardized)
        registry = registry.extend(reference_provenance_records(standardized, load_path=load_path))

    if not frames:
        return pd.DataFrame(columns=_reference_columns()), warnings, registry

    catalog = pd.concat(frames, ignore_index=True)
    return catalog, warnings, registry


def standardize_reference_frame(
    frame: pd.DataFrame,
    *,
    source_file_name: str,
) -> pd.DataFrame:
    """Map source-specific reference templates into a common schema."""

    standardized_rows: list[dict[str, Any]] = []
    source_stem = source_file_name.removesuffix(".csv")
    known_brands = canonical_brand_dictionary().keys()

    for row_number, row in enumerate(frame.to_dict(orient="records"), start=2):
        brand_name = _clean_optional_text(row.get("canonical_brand_name")) or _clean_optional_text(
            row.get("brand_name")
        )
        if not brand_name:
            continue

        resolution = resolve_brand_name(brand_name, candidate_brands=known_brands)
        canonical_name = (
            _clean_optional_text(row.get("canonical_brand_name")) or resolution.canonical_brand_name
        )

        standardized_rows.append(
            {
                "brand_name": _clean_optional_text(row.get("brand_name")) or brand_name,
                "canonical_brand_name": canonical_name,
                "brand_match_confidence": resolution.match_confidence,
                "brand_match_method": resolution.match_method,
                "source_file_name": source_file_name,
                "source_type": _clean_optional_text(row.get("source_type")) or source_stem,
                "source_name": _clean_optional_text(row.get("source_name")) or source_stem,
                "source_url_or_doc_id": _clean_optional_text(row.get("source_url_or_doc_id")),
                "as_of_date": _clean_optional_text(row.get("as_of_date")),
                "method_reported_or_estimated": _clean_optional_text(
                    row.get("method_reported_or_estimated")
                )
                or "reported",
                "confidence_score": _to_float(row.get("confidence_score")),
                "notes": _clean_optional_text(row.get("notes")),
                "source_row_number": row_number,
                "reference_rank": _to_int(_first_nonblank(row, "qsr50_rank", "technomic_rank")),
                "reference_us_store_count_2024": _to_float(
                    _first_nonblank(
                        row,
                        "us_store_count_2024",
                        "us_store_count_estimate",
                        "us_store_count",
                        "franchise_units_us",
                    )
                ),
                "reference_systemwide_revenue_usd_billions_2024": _to_float(
                    _first_nonblank(
                        row,
                        "systemwide_revenue_usd_billions_2024",
                        "systemwide_revenue_usd_billions_estimate",
                        "systemwide_revenue_usd_billions",
                    )
                ),
                "reference_average_unit_volume_usd_thousands": _to_float(
                    _first_nonblank(
                        row,
                        "average_unit_volume_usd_thousands",
                        "average_unit_volume_usd_thousands_estimate",
                    )
                ),
                "raw_source_page": _clean_optional_text(row.get("source_page")),
                "raw_source_excerpt": _clean_optional_text(row.get("source_excerpt")),
            }
        )

    return pd.DataFrame(standardized_rows, columns=_reference_columns())


def build_reconciled_core_metrics(
    core_frame: pd.DataFrame,
    reference_frame: pd.DataFrame,
    *,
    core_source_path: Path,
) -> tuple[pd.DataFrame, list[str], ProvenanceRegistry]:
    """Build reconciled Gold-layer core metrics with field-level comparisons."""

    warnings: list[str] = []
    provenance_registry = ProvenanceRegistry()
    rows: list[dict[str, Any]] = []
    known_brands = canonical_brand_dictionary().keys()

    for core_row in core_frame.to_dict(orient="records"):
        resolution = resolve_brand_name(core_row.get("brand_name"), candidate_brands=known_brands)
        canonical_name = resolution.canonical_brand_name or str(core_row["brand_name"])
        matched_refs = reference_frame[
            reference_frame["canonical_brand_name"].fillna("") == canonical_name
        ].copy()

        row_warning_messages: list[str] = []
        if matched_refs.empty:
            row_warning_messages.append(
                f"No reference coverage found for `{core_row['brand_name']}`."
            )
            warnings.append(row_warning_messages[-1])

        output_row = dict(core_row)
        output_row.update(
            {
                "canonical_brand_name": canonical_name,
                "brand_match_confidence": resolution.match_confidence,
                "brand_match_method": resolution.match_method,
                "reference_source_count": int(matched_refs["source_name"].nunique())
                if not matched_refs.empty
                else 0,
                "reference_source_names": ", ".join(
                    sorted(str(name) for name in matched_refs["source_name"].dropna().unique())
                ),
            }
        )

        field_grades: list[str] = []
        field_specs = [
            ("rank", "reference_rank", "rank"),
            ("us_store_count_2024", "reference_us_store_count_2024", "store_count"),
            (
                "systemwide_revenue_usd_billions_2024",
                "reference_systemwide_revenue_usd_billions_2024",
                "system_sales",
            ),
            (
                "average_unit_volume_usd_thousands",
                "reference_average_unit_volume_usd_thousands",
                "auv",
            ),
        ]

        for core_field, reference_field, prefix in field_specs:
            best_reference = select_best_reference_row(matched_refs, field_name=reference_field)
            if best_reference is None:
                comparison = (
                    compare_rank_field(
                        workbook_value=core_row.get(core_field), reference_value=None
                    )
                    if core_field == "rank"
                    else compare_numeric_field(
                        field_name=core_field,
                        workbook_value=core_row.get(core_field),
                        reference_value=None,
                    )
                )
                row_warning_messages.append(
                    f"No `{reference_field}` reference value was available for `{canonical_name}`."
                )
            else:
                comparison = (
                    compare_rank_field(
                        workbook_value=core_row.get(core_field),
                        reference_value=best_reference.get(reference_field),
                    )
                    if core_field == "rank"
                    else compare_numeric_field(
                        field_name=core_field,
                        workbook_value=core_row.get(core_field),
                        reference_value=best_reference.get(reference_field),
                    )
                )

            field_grades.append(comparison.credibility_grade)
            output_row[f"{prefix}_reference_value"] = comparison.reference_value
            output_row[f"{prefix}_absolute_error"] = comparison.absolute_error
            output_row[f"{prefix}_relative_error"] = comparison.relative_error
            output_row[f"{prefix}_credibility_grade"] = comparison.credibility_grade
            output_row[f"{prefix}_reference_source_name"] = (
                None if best_reference is None else best_reference.get("source_name")
            )
            output_row[f"{prefix}_reference_source_type"] = (
                None if best_reference is None else best_reference.get("source_type")
            )
            output_row[f"{prefix}_reference_confidence_score"] = (
                None if best_reference is None else best_reference.get("confidence_score")
            )

        output_row["overall_credibility_grade"] = overall_reconciliation_grade(field_grades)
        output_row["reconciliation_warning"] = " | ".join(row_warning_messages) or None
        rows.append(output_row)

        provenance_registry = provenance_registry.add(
            build_provenance_record(
                source_type="workbook",
                source_name=core_source_path.name,
                source_url_or_doc_id=str(core_source_path),
                method_reported_or_estimated="reported_in_workbook",
                confidence_score=0.35,
                notes="Workbook is treated as a hypothesis artifact rather than a source of truth.",
                canonical_brand_name=canonical_name,
                source_row_number=core_row.get("row_number"),
                source_layer="silver_core_metrics",
            )
        )

    reconciled_frame = pd.DataFrame(rows)
    return reconciled_frame, warnings, provenance_registry


def reference_provenance_records(
    reference_frame: pd.DataFrame,
    *,
    load_path: Path,
) -> list[ProvenanceRecord]:
    """Convert standardized reference rows into provenance records."""

    records: list[ProvenanceRecord] = []
    for row in reference_frame.to_dict(orient="records"):
        records.append(
            build_provenance_record(
                source_type=str(row.get("source_type") or load_path.stem),
                source_name=str(row.get("source_name") or load_path.stem),
                source_url_or_doc_id=_clean_optional_text(row.get("source_url_or_doc_id"))
                or str(load_path),
                as_of_date=row.get("as_of_date"),
                method_reported_or_estimated=str(
                    row.get("method_reported_or_estimated") or "reported"
                ),
                confidence_score=row.get("confidence_score"),
                notes=row.get("notes"),
                canonical_brand_name=row.get("canonical_brand_name"),
                source_row_number=row.get("source_row_number"),
                source_file_name=load_path.name,
                raw_source_page=row.get("raw_source_page"),
                raw_source_excerpt=row.get("raw_source_excerpt"),
            )
        )
    return records


def write_reconciliation_outputs(
    reconciled_frame: pd.DataFrame,
    provenance_frame: pd.DataFrame,
    *,
    warnings: tuple[str, ...],
    core_path: Path,
    reference_dir: Path,
    gold_dir: Path,
    report_dir: Path,
) -> ReconciliationArtifacts:
    """Write Gold parquet outputs and markdown summary."""

    gold_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    reconciled_path = gold_dir / "reconciled_core_metrics.parquet"
    provenance_path = gold_dir / "provenance_registry.parquet"
    summary_path = report_dir / "reconciliation_summary.md"

    reconciled_frame.to_parquet(reconciled_path, index=False)
    provenance_output = provenance_frame.copy()
    if "extra" in provenance_output.columns:
        provenance_output["extra"] = provenance_output["extra"].map(
            lambda value: json.dumps(value, ensure_ascii=False, default=str)
        )
    provenance_output.to_parquet(provenance_path, index=False)
    summary_path.write_text(
        render_reconciliation_summary(
            reconciled_frame,
            warnings=warnings,
            core_path=core_path,
            reference_dir=reference_dir,
        ),
        encoding="utf-8",
    )
    return ReconciliationArtifacts(
        reconciled_core_metrics_path=reconciled_path,
        provenance_registry_path=provenance_path,
        reconciliation_summary_path=summary_path,
    )


def render_reconciliation_summary(
    reconciled_frame: pd.DataFrame,
    *,
    warnings: tuple[str, ...],
    core_path: Path,
    reference_dir: Path,
) -> str:
    """Render a human-readable reconciliation summary."""

    matched_brands = (
        int((reconciled_frame["reference_source_count"] > 0).sum())
        if not reconciled_frame.empty
        else 0
    )
    missing_brands = (
        reconciled_frame.loc[reconciled_frame["reference_source_count"] == 0, "brand_name"].tolist()
        if not reconciled_frame.empty
        else []
    )

    lines = [
        "# Reconciliation Summary",
        "",
        f"- Core input: `{core_path}`",
        f"- Reference directory: `{reference_dir}`",
        f"- Core rows: `{len(reconciled_frame)}`",
        f"- Brands with reference coverage: `{matched_brands}`",
        f"- Brands without reference coverage: `{len(missing_brands)}`",
        "",
        "## Overall Grade Distribution",
        "",
    ]

    if reconciled_frame.empty:
        lines.append("No core rows were available for reconciliation.")
    else:
        grade_counts = reconciled_frame["overall_credibility_grade"].value_counts(dropna=False)
        for grade, count in grade_counts.items():
            lines.append(f"- `{grade}`: {int(count)}")

        lines.extend(["", "## Missing Coverage", ""])
        if missing_brands:
            for brand_name in missing_brands:
                lines.append(f"- {brand_name}")
        else:
            lines.append("- All workbook brands had at least one reference source.")

        lines.extend(["", "## Field-Level Grade Snapshots", ""])
        for column in [
            "rank_credibility_grade",
            "store_count_credibility_grade",
            "system_sales_credibility_grade",
            "auv_credibility_grade",
        ]:
            counts = reconciled_frame[column].value_counts(dropna=False)
            formatted = ", ".join(f"{grade}: {int(count)}" for grade, count in counts.items())
            lines.append(f"- `{column}` -> {formatted}")

        contradictions = reconciled_frame[
            reconciled_frame["overall_credibility_grade"].isin(["D", "F"])
        ][["brand_name", "overall_credibility_grade", "reconciliation_warning"]]
        lines.extend(["", "## Largest Credibility Gaps", ""])
        if contradictions.empty:
            lines.append("- No rows landed in overall grades `D` or `F`.")
        else:
            for row in contradictions.head(10).to_dict(orient="records"):
                lines.append(
                    f"- {row['brand_name']}: overall grade `{row['overall_credibility_grade']}`"
                    + (
                        f" ({row['reconciliation_warning']})"
                        if row.get("reconciliation_warning")
                        else ""
                    )
                )

    lines.extend(["", "## Warnings", ""])
    if warnings:
        for warning in warnings:
            lines.append(f"- {warning}")
    else:
        lines.append("- No reconciliation warnings were emitted.")

    return "\n".join(lines) + "\n"


def _reference_columns() -> list[str]:
    return [
        "brand_name",
        "canonical_brand_name",
        "brand_match_confidence",
        "brand_match_method",
        "source_file_name",
        "source_type",
        "source_name",
        "source_url_or_doc_id",
        "as_of_date",
        "method_reported_or_estimated",
        "confidence_score",
        "notes",
        "source_row_number",
        "reference_rank",
        "reference_us_store_count_2024",
        "reference_systemwide_revenue_usd_billions_2024",
        "reference_average_unit_volume_usd_thousands",
        "raw_source_page",
        "raw_source_excerpt",
    ]


def _clean_optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_float(value: object) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: object) -> int | None:
    number = _to_float(value)
    if number is None:
        return None
    return int(round(number))


def _first_nonblank(row: dict[str, Any], *columns: str) -> Any:
    for column in columns:
        value = row.get(column)
        if value is not None and str(value).strip() != "":
            return value
    return None


__all__ = [
    "REFERENCE_TEMPLATE_FILES",
    "ReconciliationArtifacts",
    "ReconciliationRun",
    "build_reconciled_core_metrics",
    "load_reference_catalog",
    "reconcile_core_metrics",
    "render_reconciliation_summary",
    "standardize_reference_frame",
    "write_reconciliation_outputs",
]
