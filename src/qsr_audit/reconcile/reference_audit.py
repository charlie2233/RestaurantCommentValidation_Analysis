"""Reference intake validation and coverage audit helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.reconcile.entity_resolution import canonical_brand_dictionary, resolve_brand_name

REFERENCE_TEMPLATE_COLUMNS: dict[str, tuple[str, ...]] = {
    "qsr50_reference.csv": (
        "brand_name",
        "canonical_brand_name",
        "source_type",
        "source_name",
        "source_url_or_doc_id",
        "as_of_date",
        "method_reported_or_estimated",
        "confidence_score",
        "notes",
        "qsr50_rank",
        "us_store_count_2024",
        "systemwide_revenue_usd_billions_2024",
        "average_unit_volume_usd_thousands",
        "currency",
        "geography",
        "source_page",
        "source_excerpt",
    ),
    "technomic_reference.csv": (
        "brand_name",
        "canonical_brand_name",
        "source_type",
        "source_name",
        "source_url_or_doc_id",
        "as_of_date",
        "method_reported_or_estimated",
        "confidence_score",
        "notes",
        "technomic_rank",
        "us_store_count_estimate",
        "systemwide_revenue_usd_billions_estimate",
        "average_unit_volume_usd_thousands_estimate",
        "margin_estimate_pct",
        "currency",
        "geography",
        "source_page",
        "source_excerpt",
    ),
    "sec_filings_reference.csv": (
        "brand_name",
        "canonical_brand_name",
        "source_type",
        "source_name",
        "source_url_or_doc_id",
        "as_of_date",
        "method_reported_or_estimated",
        "confidence_score",
        "notes",
        "filing_type",
        "filing_date",
        "issuer_name",
        "issuer_ticker",
        "us_store_count",
        "us_store_count_scope",
        "systemwide_revenue_usd_billions",
        "systemwide_revenue_scope",
        "average_unit_volume_usd_thousands",
        "average_unit_volume_scope",
        "revenue_segment_notes",
        "scope_notes",
        "provenance_grade",
        "currency",
        "geography",
        "source_page",
        "source_excerpt",
    ),
    "franchise_disclosure_reference.csv": (
        "brand_name",
        "canonical_brand_name",
        "source_type",
        "source_name",
        "source_url_or_doc_id",
        "as_of_date",
        "method_reported_or_estimated",
        "confidence_score",
        "notes",
        "fdd_year",
        "franchise_units_us",
        "royalty_rate_pct",
        "advertising_fund_rate_pct",
        "average_unit_volume_usd_thousands",
        "currency",
        "geography",
        "source_page",
        "source_excerpt",
    ),
}

REFERENCE_TEMPLATE_FILES: tuple[str, ...] = tuple(REFERENCE_TEMPLATE_COLUMNS)

REFERENCE_NUMERIC_COLUMNS: dict[str, tuple[str, ...]] = {
    "qsr50_reference.csv": (
        "confidence_score",
        "qsr50_rank",
        "us_store_count_2024",
        "systemwide_revenue_usd_billions_2024",
        "average_unit_volume_usd_thousands",
    ),
    "technomic_reference.csv": (
        "confidence_score",
        "technomic_rank",
        "us_store_count_estimate",
        "systemwide_revenue_usd_billions_estimate",
        "average_unit_volume_usd_thousands_estimate",
        "margin_estimate_pct",
    ),
    "sec_filings_reference.csv": (
        "confidence_score",
        "us_store_count",
        "systemwide_revenue_usd_billions",
        "average_unit_volume_usd_thousands",
    ),
    "franchise_disclosure_reference.csv": (
        "confidence_score",
        "fdd_year",
        "franchise_units_us",
        "royalty_rate_pct",
        "advertising_fund_rate_pct",
        "average_unit_volume_usd_thousands",
    ),
}

REFERENCE_METRIC_COLUMNS: dict[str, str] = {
    "rank": "reference_rank",
    "store_count": "reference_us_store_count_2024",
    "system_sales": "reference_systemwide_revenue_usd_billions_2024",
    "auv": "reference_average_unit_volume_usd_thousands",
}

REFERENCE_FILE_SOURCE_TYPES: dict[str, str] = {
    "qsr50_reference.csv": "qsr50",
    "technomic_reference.csv": "technomic",
    "sec_filings_reference.csv": "sec_filings",
    "franchise_disclosure_reference.csv": "franchise_disclosure",
}

PROVENANCE_COMPLETENESS_COLUMNS: tuple[str, ...] = (
    "source_type",
    "source_name",
    "source_url_or_doc_id",
    "as_of_date",
    "method_reported_or_estimated",
    "confidence_score",
)

ALLOWED_METHOD_VALUES = {
    "reported",
    "estimated",
    "reported_and_estimated",
    "mixed",
    "derived",
    "unknown",
}

PRIMARY_SOURCE_SCOPE_COLUMNS: dict[str, str] = {
    "us_store_count": "us_store_count_scope",
    "systemwide_revenue_usd_billions": "systemwide_revenue_scope",
    "average_unit_volume_usd_thousands": "average_unit_volume_scope",
}

ALLOWED_PRIMARY_SOURCE_SCOPE_VALUES = {
    "direct_comparable",
    "scope_mismatch",
    "not_available",
}

PRIMARY_SOURCE_DIRECT_SCOPE = "direct_comparable"
PRIMARY_SOURCE_SOURCE_TYPES = ("sec_filings", "investor_relations")
ALLOWED_PROVENANCE_GRADES = {"A", "B", "C", "D", "F"}


@dataclass(frozen=True)
class ReferenceCoverageArtifacts:
    """Coverage audit outputs produced alongside reconciliation."""

    reference_coverage_path: Path
    reference_coverage_report_path: Path


def validate_reference_file(
    frame: pd.DataFrame,
    *,
    file_name: str,
) -> list[str]:
    """Validate a manual reference CSV against the committed template conventions."""

    expected_columns = REFERENCE_TEMPLATE_COLUMNS[file_name]
    missing_columns = [column for column in expected_columns if column not in frame.columns]
    if missing_columns:
        raise ValueError(
            f"Reference file `{file_name}` is missing required template columns: "
            + ", ".join(missing_columns)
            + ". Copy the committed template header exactly and leave unknown values blank."
        )

    warnings: list[str] = []
    unexpected_columns = [column for column in frame.columns if column not in expected_columns]
    if unexpected_columns:
        warnings.append(
            f"Reference file `{file_name}` has unexpected columns that will be ignored: "
            + ", ".join(unexpected_columns)
            + ". Stick to the committed template schema."
        )

    metric_columns = _template_metric_columns(file_name)
    for row_number, row in enumerate(frame.to_dict(orient="records"), start=2):
        if _row_is_blank(row):
            continue

        brand_name = _clean_optional_text(row.get("brand_name")) or _clean_optional_text(
            row.get("canonical_brand_name")
        )
        if not brand_name:
            warnings.append(
                f"Reference file `{file_name}` row {row_number} has no brand_name or "
                "canonical_brand_name and will be skipped."
            )
            continue

        missing_provenance = [
            column
            for column in (
                "source_name",
                "source_url_or_doc_id",
                "as_of_date",
                "method_reported_or_estimated",
            )
            if _clean_optional_text(row.get(column)) is None
        ]
        if missing_provenance:
            warnings.append(
                f"Reference file `{file_name}` row {row_number} for `{brand_name}` is missing "
                "provenance fields: "
                + ", ".join(missing_provenance)
                + ". Leave unknowns blank, but do not infer them."
            )

        method_value = _clean_optional_text(row.get("method_reported_or_estimated"))
        if method_value is not None and method_value.lower() not in ALLOWED_METHOD_VALUES:
            warnings.append(
                f"Reference file `{file_name}` row {row_number} for `{brand_name}` uses "
                f"`{method_value}` in `method_reported_or_estimated`. Prefer one of: "
                + ", ".join(sorted(ALLOWED_METHOD_VALUES))
                + "."
            )

        confidence_value = _clean_optional_text(row.get("confidence_score"))
        if confidence_value is not None:
            confidence_score = _to_float(confidence_value)
            if confidence_score is None or not 0 <= confidence_score <= 1:
                warnings.append(
                    f"Reference file `{file_name}` row {row_number} for `{brand_name}` has "
                    f"`confidence_score={confidence_value}` outside the supported 0-1 range."
                )

        as_of_date = _clean_optional_text(row.get("as_of_date"))
        if as_of_date is not None and pd.isna(pd.to_datetime(as_of_date, errors="coerce")):
            warnings.append(
                f"Reference file `{file_name}` row {row_number} for `{brand_name}` has "
                f"an invalid `as_of_date` value `{as_of_date}`."
            )

        for column_name in REFERENCE_NUMERIC_COLUMNS[file_name]:
            text_value = _clean_optional_text(row.get(column_name))
            if text_value is not None and _to_float(text_value) is None:
                warnings.append(
                    f"Reference file `{file_name}` row {row_number} for `{brand_name}` has "
                    f"a non-numeric `{column_name}` value `{text_value}`."
                )

        if file_name == "sec_filings_reference.csv":
            for metric_column, scope_column in PRIMARY_SOURCE_SCOPE_COLUMNS.items():
                metric_value = _clean_optional_text(row.get(metric_column))
                scope_value = _clean_optional_text(row.get(scope_column))
                if (
                    scope_value is not None
                    and scope_value.lower() not in ALLOWED_PRIMARY_SOURCE_SCOPE_VALUES
                ):
                    warnings.append(
                        f"Reference file `{file_name}` row {row_number} for `{brand_name}` uses "
                        f"`{scope_value}` in `{scope_column}`. Prefer one of: "
                        + ", ".join(sorted(ALLOWED_PRIMARY_SOURCE_SCOPE_VALUES))
                        + "."
                    )
                if metric_value is not None and scope_value is None:
                    warnings.append(
                        f"Reference file `{file_name}` row {row_number} for `{brand_name}` has "
                        f"`{metric_column}` populated without `{scope_column}`. Mark it as "
                        "`direct_comparable`, `scope_mismatch`, or `not_available`."
                    )

            provenance_grade = _clean_optional_text(row.get("provenance_grade"))
            if (
                provenance_grade is not None
                and provenance_grade.upper() not in ALLOWED_PROVENANCE_GRADES
            ):
                warnings.append(
                    f"Reference file `{file_name}` row {row_number} for `{brand_name}` uses "
                    f"`{provenance_grade}` in `provenance_grade`. Prefer one of: "
                    + ", ".join(sorted(ALLOWED_PROVENANCE_GRADES))
                    + "."
                )

        if not any(_clean_optional_text(row.get(column)) is not None for column in metric_columns):
            warnings.append(
                f"Reference file `{file_name}` row {row_number} for `{brand_name}` has no "
                "metric values populated. The row will contribute provenance only."
            )

    return warnings


def build_reference_coverage(
    core_frame: pd.DataFrame,
    reference_frame: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str]]:
    """Build brand, metric, and source-type coverage rows for manual references."""

    warnings: list[str] = []
    rows: list[dict[str, Any]] = []

    core_rows = _normalize_core_rows_for_coverage(core_frame)
    total_core_brands = len(core_rows.index)
    core_brand_names = {
        str(value)
        for value in core_rows["canonical_brand_name"].fillna("").tolist()
        if str(value).strip()
    }
    brand_rows: list[dict[str, Any]] = []

    for core_row in core_rows.to_dict(orient="records"):
        canonical_brand_name = str(core_row.get("canonical_brand_name") or core_row["brand_name"])
        matched_refs = reference_frame[
            reference_frame["canonical_brand_name"].fillna("") == canonical_brand_name
        ].copy()
        covered_metrics = [
            metric_name
            for metric_name, column_name in REFERENCE_METRIC_COLUMNS.items()
            if not matched_refs.empty and matched_refs[column_name].notna().any()
        ]
        missing_metrics = [
            metric_name
            for metric_name in REFERENCE_METRIC_COLUMNS
            if metric_name not in covered_metrics
        ]
        provenance_score, completeness_summary, confidence_summary = summarize_provenance_quality(
            matched_refs
        )

        warning = None
        if matched_refs.empty:
            warning = (
                f"No populated manual reference rows matched `{canonical_brand_name}`. "
                "Missing coverage is missing evidence, not confirmation."
            )
        elif missing_metrics:
            warning = (
                f"Reference coverage for `{canonical_brand_name}` is partial. Missing metrics: "
                + ", ".join(missing_metrics)
                + "."
            )
        if warning is not None:
            warnings.append(warning)

        brand_row = {
            "coverage_kind": "brand",
            "coverage_key": canonical_brand_name,
            "brand_name": core_row["brand_name"],
            "canonical_brand_name": canonical_brand_name,
            "metric_name": None,
            "source_type": None,
            "is_covered": not matched_refs.empty,
            "reference_row_count": int(len(matched_refs.index)),
            "reference_source_count": int(matched_refs["source_name"].dropna().nunique())
            if not matched_refs.empty
            else 0,
            "covered_metrics_count": len(covered_metrics),
            "covered_brand_count": None,
            "missing_brand_count": None,
            "coverage_rate": len(covered_metrics) / len(REFERENCE_METRIC_COLUMNS),
            "missing_metrics": missing_metrics,
            "missing_brands": None,
            "source_type_names": sorted(
                str(value) for value in matched_refs["source_type"].dropna().unique()
            ),
            "provenance_completeness_score": provenance_score,
            "provenance_completeness_summary": completeness_summary,
            "provenance_confidence_summary": confidence_summary,
            "warning": warning,
            "details": {
                "source_names": sorted(
                    str(value) for value in matched_refs["source_name"].dropna().unique()
                ),
                "source_types": sorted(
                    str(value) for value in matched_refs["source_type"].dropna().unique()
                ),
            },
        }
        rows.append(brand_row)
        brand_rows.append(brand_row)

    for metric_name in REFERENCE_METRIC_COLUMNS:
        covered_brands = [
            str(row["canonical_brand_name"])
            for row in brand_rows
            if metric_name not in (row.get("missing_metrics") or [])
        ]
        missing_brands = [
            str(row["canonical_brand_name"])
            for row in brand_rows
            if metric_name in (row.get("missing_metrics") or [])
        ]
        warning = None
        if missing_brands:
            warning = (
                f"Metric `{metric_name}` has manual reference coverage for {len(covered_brands)} "
                f"of {total_core_brands} core brands."
            )
            warnings.append(warning)

        rows.append(
            {
                "coverage_kind": "metric",
                "coverage_key": metric_name,
                "brand_name": None,
                "canonical_brand_name": None,
                "metric_name": metric_name,
                "source_type": None,
                "is_covered": bool(covered_brands),
                "reference_row_count": int(
                    reference_frame[REFERENCE_METRIC_COLUMNS[metric_name]].notna().sum()
                ),
                "reference_source_count": int(
                    reference_frame.loc[
                        reference_frame[REFERENCE_METRIC_COLUMNS[metric_name]].notna(),
                        "source_name",
                    ]
                    .dropna()
                    .nunique()
                ),
                "covered_metrics_count": None,
                "covered_brand_count": len(covered_brands),
                "missing_brand_count": len(missing_brands),
                "coverage_rate": (len(covered_brands) / total_core_brands)
                if total_core_brands
                else 0.0,
                "missing_metrics": None,
                "missing_brands": missing_brands,
                "source_type_names": sorted(
                    str(value)
                    for value in reference_frame.loc[
                        reference_frame[REFERENCE_METRIC_COLUMNS[metric_name]].notna(),
                        "source_type",
                    ]
                    .dropna()
                    .unique()
                ),
                "provenance_completeness_score": None,
                "provenance_completeness_summary": None,
                "provenance_confidence_summary": None,
                "warning": warning,
                "details": {
                    "covered_brands": covered_brands,
                    "missing_brands": missing_brands,
                },
            }
        )

    for file_name in REFERENCE_TEMPLATE_FILES:
        source_type = REFERENCE_FILE_SOURCE_TYPES[file_name]
        subset = reference_frame[reference_frame["source_file_name"] == file_name].copy()
        audited_subset = subset[
            subset["canonical_brand_name"].fillna("").astype(str).isin(core_brand_names)
        ].copy()
        provenance_score, completeness_summary, confidence_summary = summarize_provenance_quality(
            audited_subset
        )
        covered_metric_names = [
            metric_name
            for metric_name, column_name in REFERENCE_METRIC_COLUMNS.items()
            if not audited_subset.empty and audited_subset[column_name].notna().any()
        ]
        missing_metric_names = [
            metric_name
            for metric_name in REFERENCE_METRIC_COLUMNS
            if metric_name not in covered_metric_names
        ]
        covered_brand_count = (
            int(audited_subset["canonical_brand_name"].dropna().nunique())
            if not audited_subset.empty
            else 0
        )
        coverage_rate = (
            min((covered_brand_count / total_core_brands), 1.0) if total_core_brands else 0.0
        )
        extra_reference_brands = sorted(
            str(value)
            for value in subset["canonical_brand_name"].dropna().unique()
            if str(value).strip() and str(value) not in core_brand_names
        )
        warning = None
        if subset.empty:
            warning = (
                f"No populated `{file_name}` rows were found. `{source_type}` coverage is explicit zero, "
                "not confirmation."
            )
            warnings.append(warning)

        rows.append(
            {
                "coverage_kind": "source_type",
                "coverage_key": source_type,
                "brand_name": None,
                "canonical_brand_name": None,
                "metric_name": None,
                "source_type": source_type,
                "is_covered": not audited_subset.empty,
                "reference_row_count": int(len(subset.index)),
                "reference_source_count": int(audited_subset["source_name"].dropna().nunique())
                if not audited_subset.empty
                else 0,
                "covered_metrics_count": len(covered_metric_names),
                "covered_brand_count": covered_brand_count,
                "missing_brand_count": max(total_core_brands - covered_brand_count, 0),
                "coverage_rate": coverage_rate,
                "missing_metrics": missing_metric_names,
                "missing_brands": None,
                "source_type_names": [source_type],
                "provenance_completeness_score": provenance_score,
                "provenance_completeness_summary": completeness_summary,
                "provenance_confidence_summary": confidence_summary,
                "warning": warning,
                "details": {
                    "source_file_name": file_name,
                    "covered_metric_names": covered_metric_names,
                    "extra_reference_brands": extra_reference_brands,
                },
            }
        )

    coverage_frame = pd.DataFrame(rows)
    return coverage_frame, warnings


def write_reference_coverage_outputs(
    coverage_frame: pd.DataFrame,
    *,
    warnings: tuple[str, ...],
    core_path: Path,
    reference_dir: Path,
    gold_dir: Path,
    report_dir: Path,
) -> ReferenceCoverageArtifacts:
    """Write the reference coverage parquet and markdown outputs."""

    gold_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    coverage_path = gold_dir / "reference_coverage.parquet"
    report_path = report_dir / "reference_coverage.md"

    parquet_frame = coverage_frame.copy()
    for column in ("missing_metrics", "missing_brands", "source_type_names", "details"):
        if column in parquet_frame.columns:
            parquet_frame[column] = parquet_frame[column].map(_json_dump_or_none)
    parquet_frame.to_parquet(coverage_path, index=False)
    report_path.write_text(
        render_reference_coverage_report(
            coverage_frame,
            warnings=warnings,
            core_path=core_path,
            reference_dir=reference_dir,
        ),
        encoding="utf-8",
    )
    return ReferenceCoverageArtifacts(
        reference_coverage_path=coverage_path,
        reference_coverage_report_path=report_path,
    )


def render_reference_coverage_report(
    coverage_frame: pd.DataFrame,
    *,
    warnings: tuple[str, ...],
    core_path: Path,
    reference_dir: Path,
) -> str:
    """Render a human-readable markdown coverage audit."""

    brand_rows = coverage_frame[coverage_frame["coverage_kind"] == "brand"].copy()
    metric_rows = coverage_frame[coverage_frame["coverage_kind"] == "metric"].copy()
    source_rows = coverage_frame[coverage_frame["coverage_kind"] == "source_type"].copy()

    lines = [
        "# Reference Coverage Audit",
        "",
        f"- Core input: `{core_path}`",
        f"- Reference directory: `{reference_dir}`",
        f"- Brand rows audited: `{len(brand_rows)}`",
        "",
        "## Brand-Level Coverage",
        "",
        "| Brand | Sources | Covered metrics | Missing metrics | Provenance completeness | Confidence summary |",
        "| --- | ---: | ---: | --- | --- | --- |",
    ]

    if brand_rows.empty:
        lines.append(
            "| _(none)_ | 0 | 0 | all | No reference coverage. | No confidence scores were reported. |"
        )
    else:
        for row in brand_rows.to_dict(orient="records"):
            missing_metrics = ", ".join(row.get("missing_metrics") or []) or "-"
            lines.append(
                "| "
                + f"{row['canonical_brand_name']} | "
                + f"{int(row['reference_source_count'])} | "
                + f"{int(row['covered_metrics_count'])} | "
                + f"{missing_metrics} | "
                + f"{row['provenance_completeness_summary']} | "
                + f"{row['provenance_confidence_summary']} |"
            )

    lines.extend(["", "## Metric-Level Coverage", ""])
    if metric_rows.empty:
        lines.append("- No metric coverage rows were generated.")
    else:
        for row in metric_rows.to_dict(orient="records"):
            missing_brands = ", ".join(row.get("missing_brands") or []) or "none"
            lines.append(
                f"- `{row['metric_name']}`: {int(row['covered_brand_count'])}/"
                f"{int(row['covered_brand_count']) + int(row['missing_brand_count'])} core brands covered; "
                f"missing brands: {missing_brands}."
            )

    lines.extend(["", "## Source-Type Coverage", ""])
    if source_rows.empty:
        lines.append("- No source-type coverage rows were generated.")
    else:
        for row in source_rows.to_dict(orient="records"):
            missing_metrics = ", ".join(row.get("missing_metrics") or []) or "none"
            lines.append(
                f"- `{row['source_type']}`: {int(row['reference_row_count'])} row(s), "
                f"{int(row['covered_brand_count'])} covered brand(s), "
                f"{int(row['covered_metrics_count'])} covered metric type(s), "
                f"missing metric types: {missing_metrics}."
            )

    lines.extend(["", "## Warnings", ""])
    if warnings:
        for warning in warnings:
            lines.append(f"- {warning}")
    else:
        lines.append("- No reference coverage warnings were emitted.")

    return "\n".join(lines) + "\n"


def summarize_provenance_quality(reference_rows: pd.DataFrame) -> tuple[float, str, str]:
    """Summarize completeness and confidence across matched reference rows."""

    if reference_rows.empty:
        return (
            0.0,
            "No reference provenance fields were populated because no matched reference rows exist.",
            "No confidence scores were reported.",
        )

    filled_count = 0
    possible_count = len(reference_rows.index) * len(PROVENANCE_COMPLETENESS_COLUMNS)
    for column_name in PROVENANCE_COMPLETENESS_COLUMNS:
        if column_name == "confidence_score":
            filled_count += int(
                pd.to_numeric(reference_rows[column_name], errors="coerce").notna().sum()
            )
        else:
            filled_count += int(
                reference_rows[column_name].fillna("").astype(str).str.strip().ne("").sum()
            )

    completeness_score = filled_count / possible_count if possible_count else 0.0
    completeness_summary = (
        f"{filled_count}/{possible_count} provenance fields populated across "
        f"{len(reference_rows.index)} matched reference row(s)."
    )

    confidence_scores = pd.to_numeric(reference_rows["confidence_score"], errors="coerce").dropna()
    if confidence_scores.empty:
        confidence_summary = "No confidence scores were reported in matched reference rows."
    else:
        confidence_summary = (
            f"Average confidence {confidence_scores.mean():.2f} across "
            f"{len(confidence_scores.index)} row(s); range "
            f"{confidence_scores.min():.2f}-{confidence_scores.max():.2f}."
        )

    return completeness_score, completeness_summary, confidence_summary


def coverage_frame_to_json_records(coverage_frame: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert a coverage frame to JSON-safe dictionaries."""

    records = coverage_frame.to_dict(orient="records")
    json.loads(json.dumps(records, ensure_ascii=False, default=str))
    return records


def _json_dump_or_none(value: object) -> str | None:
    if value is None or value == "":
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


def _normalize_core_rows_for_coverage(core_frame: pd.DataFrame) -> pd.DataFrame:
    normalized = core_frame.copy()
    if "canonical_brand_name" not in normalized.columns:
        normalized["canonical_brand_name"] = None

    known_brands = canonical_brand_dictionary().keys()
    canonical_names: list[str] = []
    for row in normalized.to_dict(orient="records"):
        resolution_input = _clean_optional_text(row.get("brand_name")) or _clean_optional_text(
            row.get("canonical_brand_name")
        )
        resolution = resolve_brand_name(resolution_input, candidate_brands=known_brands)
        fallback_name = (
            _clean_optional_text(row.get("canonical_brand_name"))
            or _clean_optional_text(row.get("brand_name"))
            or ""
        )
        canonical_names.append(resolution.canonical_brand_name or fallback_name)

    normalized["canonical_brand_name"] = canonical_names
    return normalized[["brand_name", "canonical_brand_name"]].copy()


def _template_metric_columns(file_name: str) -> tuple[str, ...]:
    metric_columns = {
        "qsr50_reference.csv": (
            "qsr50_rank",
            "us_store_count_2024",
            "systemwide_revenue_usd_billions_2024",
            "average_unit_volume_usd_thousands",
        ),
        "technomic_reference.csv": (
            "technomic_rank",
            "us_store_count_estimate",
            "systemwide_revenue_usd_billions_estimate",
            "average_unit_volume_usd_thousands_estimate",
            "margin_estimate_pct",
        ),
        "sec_filings_reference.csv": (
            "us_store_count",
            "systemwide_revenue_usd_billions",
            "average_unit_volume_usd_thousands",
        ),
        "franchise_disclosure_reference.csv": (
            "fdd_year",
            "franchise_units_us",
            "royalty_rate_pct",
            "advertising_fund_rate_pct",
            "average_unit_volume_usd_thousands",
        ),
    }
    return metric_columns[file_name]


def _row_is_blank(row: dict[str, Any]) -> bool:
    return all(_clean_optional_text(value) is None for value in row.values())


def _clean_optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def _to_float(value: object) -> float | None:
    text = _clean_optional_text(value)
    if text is None:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


__all__ = [
    "ALLOWED_PRIMARY_SOURCE_SCOPE_VALUES",
    "ALLOWED_PROVENANCE_GRADES",
    "ALLOWED_METHOD_VALUES",
    "PRIMARY_SOURCE_DIRECT_SCOPE",
    "PRIMARY_SOURCE_SCOPE_COLUMNS",
    "PRIMARY_SOURCE_SOURCE_TYPES",
    "PROVENANCE_COMPLETENESS_COLUMNS",
    "REFERENCE_FILE_SOURCE_TYPES",
    "REFERENCE_METRIC_COLUMNS",
    "REFERENCE_NUMERIC_COLUMNS",
    "REFERENCE_TEMPLATE_COLUMNS",
    "REFERENCE_TEMPLATE_FILES",
    "ReferenceCoverageArtifacts",
    "build_reference_coverage",
    "coverage_frame_to_json_records",
    "render_reference_coverage_report",
    "summarize_provenance_quality",
    "validate_reference_file",
    "write_reference_coverage_outputs",
]
