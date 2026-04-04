"""Analyst-facing report rendering for validation, reconciliation, and syntheticness outputs."""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from jinja2 import Environment, FileSystemLoader, Template, select_autoescape

DEFAULT_REPORT_ROOT = Path("reports/scorecards")
DEFAULT_DATA_ROOT = Path(".")
DEFAULT_TEMPLATE_NAME = "report.html.j2"

VALIDATION_RESULTS_PATH = Path("reports/validation/validation_results.json")
VALIDATION_FLAGS_PATH = Path("data/gold/validation_flags.parquet")
RECONCILED_CORE_PATH = Path("data/gold/reconciled_core_metrics.parquet")
PROVENANCE_REGISTRY_PATH = Path("data/gold/provenance_registry.parquet")
SYNTHETICNESS_PATH = Path("data/gold/syntheticness_signals.parquet")


@dataclass(frozen=True)
class ReportArtifacts:
    """Written report file paths."""

    global_markdown: Path
    global_html: Path
    global_json: Path
    brand_markdown_paths: dict[str, Path] = field(default_factory=dict)
    brand_html_paths: dict[str, Path] = field(default_factory=dict)
    brand_json_paths: dict[str, Path] = field(default_factory=dict)


@dataclass(frozen=True)
class ValidationSummary:
    """Compact validation summary extracted from prior outputs."""

    passed: bool
    counts: dict[str, int]
    warning_counts_by_category: dict[str, int]
    error_counts_by_category: dict[str, int]
    findings: list[dict[str, Any]]


@dataclass(frozen=True)
class GlobalScorecard:
    """Executive summary for the whole workbook program."""

    total_brands: int
    passed_brands: int
    failed_brands: int
    warning_brands: int
    validation: ValidationSummary
    warning_counts_by_category: dict[str, int]
    fields_with_weakest_provenance: list[dict[str, Any]]
    biggest_reconciliation_errors: list[dict[str, Any]]
    syntheticness_overview: dict[str, Any]
    open_issues: list[str]


@dataclass(frozen=True)
class BrandScorecard:
    """Brand-level scorecard for analyst review."""

    brand_name: str
    canonical_brand_name: str
    status: str
    normalized_metrics: dict[str, Any]
    invariant_results: list[dict[str, Any]]
    provenance_grades: dict[str, Any]
    reconciliation_error_summary: list[dict[str, Any]]
    syntheticness_signals: list[dict[str, Any]]
    open_issues: list[str]


@dataclass(frozen=True)
class AnalystReportBundle:
    """Complete reporting bundle."""

    generated_at: datetime
    global_scorecard: GlobalScorecard
    brand_scorecards: tuple[BrandScorecard, ...]
    source_paths: dict[str, Path] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return _json_safe(
            {
                "generated_at": self.generated_at.isoformat(),
                "source_paths": {key: str(value) for key, value in self.source_paths.items()},
                "global_scorecard": self.global_scorecard,
                "brand_scorecards": list(self.brand_scorecards),
            }
        )


def build_analyst_report_bundle(
    *,
    data_root: Path = DEFAULT_DATA_ROOT,
    reports_root: Path = DEFAULT_REPORT_ROOT.parent,
) -> AnalystReportBundle:
    """Load local artifacts and build an analyst-facing report bundle."""

    resolved_data_root = data_root.expanduser().resolve()
    resolved_reports_root = reports_root.expanduser().resolve()
    source_paths = {
        "validation_results": _resolve_optional_path(
            resolved_reports_root / "validation" / "validation_results.json",
            resolved_reports_root / "validation_results.json",
        ),
        "validation_flags": resolved_data_root / VALIDATION_FLAGS_PATH,
        "reconciled_core_metrics": resolved_data_root / RECONCILED_CORE_PATH,
        "provenance_registry": resolved_data_root / PROVENANCE_REGISTRY_PATH,
        "syntheticness": resolved_data_root / SYNTHETICNESS_PATH,
    }

    reconciled = _read_parquet_if_exists(source_paths["reconciled_core_metrics"])
    if reconciled is None:
        reconciled = _read_parquet_if_exists(resolved_data_root / "data/silver/core_brand_metrics.parquet")
    provenance = _read_parquet_if_exists(source_paths["provenance_registry"])
    validation_results = _read_json_if_exists(source_paths["validation_results"])
    validation_flags = _read_parquet_if_exists(source_paths["validation_flags"])
    syntheticness = _read_parquet_if_exists(source_paths["syntheticness"])

    if reconciled is None:
        reconciled = pd.DataFrame()

    validation_summary = _build_validation_summary(validation_results, validation_flags)
    brand_scorecards = build_brand_scorecards(
        reconciled,
        validation_flags=validation_flags,
        syntheticness=syntheticness,
    )
    global_scorecard = build_global_scorecard(
        reconciled=reconciled,
        provenance=provenance,
        validation_summary=validation_summary,
        syntheticness=syntheticness,
        brand_scorecards=brand_scorecards,
    )

    return AnalystReportBundle(
        generated_at=datetime.now(tz=UTC),
        global_scorecard=global_scorecard,
        brand_scorecards=tuple(brand_scorecards),
        source_paths=source_paths,
    )


def build_global_scorecard(
    *,
    reconciled: pd.DataFrame,
    provenance: pd.DataFrame | None,
    validation_summary: ValidationSummary,
    syntheticness: pd.DataFrame | None,
    brand_scorecards: list[BrandScorecard],
) -> GlobalScorecard:
    """Build the global credibility scorecard."""

    total_brands = len(reconciled)
    passed_brands = sum(1 for scorecard in brand_scorecards if scorecard.status == "pass")
    warning_brands = sum(1 for scorecard in brand_scorecards if scorecard.status == "warn")
    failed_brands = sum(1 for scorecard in brand_scorecards if scorecard.status == "fail")

    warning_counts_by_category = dict(validation_summary.warning_counts_by_category)
    for issue in _collect_reconciliation_warning_categories(reconciled):
        warning_counts_by_category[issue[0]] = warning_counts_by_category.get(issue[0], 0) + issue[1]

    fields_with_weakest_provenance = _weakest_provenance_fields(reconciled, provenance)
    biggest_reconciliation_errors = _biggest_reconciliation_errors(reconciled)
    syntheticness_overview = _syntheticness_overview(syntheticness)
    open_issues = _global_open_issues(validation_summary, reconciled, syntheticness)

    return GlobalScorecard(
        total_brands=total_brands,
        passed_brands=passed_brands,
        failed_brands=failed_brands,
        warning_brands=warning_brands,
        validation=validation_summary,
        warning_counts_by_category=warning_counts_by_category,
        fields_with_weakest_provenance=fields_with_weakest_provenance,
        biggest_reconciliation_errors=biggest_reconciliation_errors,
        syntheticness_overview=syntheticness_overview,
        open_issues=open_issues,
    )


def build_brand_scorecards(
    reconciled: pd.DataFrame,
    *,
    validation_flags: pd.DataFrame | None,
    syntheticness: pd.DataFrame | None,
) -> list[BrandScorecard]:
    """Build a brand-level scorecard for each chain."""

    if reconciled.empty:
        return []

    validation_flags = validation_flags.copy() if validation_flags is not None else pd.DataFrame()
    syntheticness = syntheticness.copy() if syntheticness is not None else pd.DataFrame()
    brand_scorecards: list[BrandScorecard] = []

    for row in reconciled.to_dict(orient="records"):
        brand_name = str(row.get("brand_name") or row.get("canonical_brand_name") or "Unknown")
        canonical_brand_name = str(row.get("canonical_brand_name") or brand_name)
        validation_rows = _brand_validation_rows(validation_flags, brand_name)
        synthetic_rows = _brand_syntheticness_rows(syntheticness, brand_name)

        provenance_grades = {
            "brand_match": {
                "confidence": _maybe_float(row.get("brand_match_confidence")),
                "method": row.get("brand_match_method"),
            },
            "rank": _field_grade(row, "rank"),
            "store_count": _field_grade(row, "store_count"),
            "system_sales": _field_grade(row, "system_sales"),
            "auv": _field_grade(row, "auv"),
            "overall": row.get("overall_credibility_grade"),
        }
        reconciliation_error_summary = _reconciliation_error_summary(row)
        open_issues = _brand_open_issues(row, validation_rows, synthetic_rows)
        status = _brand_status(row, validation_rows, synthetic_rows)

        brand_scorecards.append(
            BrandScorecard(
                brand_name=brand_name,
                canonical_brand_name=canonical_brand_name,
                status=status,
                normalized_metrics=_normalized_metrics(row),
                invariant_results=validation_rows,
                provenance_grades=provenance_grades,
                reconciliation_error_summary=reconciliation_error_summary,
                syntheticness_signals=synthetic_rows,
                open_issues=open_issues,
            )
        )

    return brand_scorecards


def write_analyst_reports(
    *,
    output_root: Path = DEFAULT_REPORT_ROOT,
    data_root: Path = DEFAULT_DATA_ROOT,
) -> ReportArtifacts:
    """Write Markdown, HTML, and JSON reports for the full scorecard suite."""

    bundle = build_analyst_report_bundle(data_root=data_root)
    report_root = output_root.expanduser().resolve()
    brand_root = report_root / "brands"
    report_root.mkdir(parents=True, exist_ok=True)
    brand_root.mkdir(parents=True, exist_ok=True)

    global_markdown = report_root / "index.md"
    global_html = report_root / "index.html"
    global_json = report_root / "index.json"

    global_markdown.write_text(render_global_markdown(bundle), encoding="utf-8")
    global_html.write_text(render_html(bundle), encoding="utf-8")
    global_json.write_text(json.dumps(bundle.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")

    brand_markdown_paths: dict[str, Path] = {}
    brand_html_paths: dict[str, Path] = {}
    brand_json_paths: dict[str, Path] = {}
    for scorecard in bundle.brand_scorecards:
        slug = slugify(scorecard.canonical_brand_name or scorecard.brand_name)
        brand_bundle = {
            "generated_at": bundle.generated_at.isoformat(),
            "global_scorecard": bundle.global_scorecard,
            "brand_scorecard": scorecard,
        }
        markdown_path = brand_root / f"{slug}.md"
        html_path = brand_root / f"{slug}.html"
        json_path = brand_root / f"{slug}.json"
        markdown_path.write_text(render_brand_markdown(scorecard, bundle), encoding="utf-8")
        html_path.write_text(render_html(brand_bundle, mode="brand"), encoding="utf-8")
        json_path.write_text(
            json.dumps(
                {
                    "generated_at": bundle.generated_at.isoformat(),
                    "brand_scorecard": _json_safe(scorecard),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        brand_markdown_paths[slug] = markdown_path
        brand_html_paths[slug] = html_path
        brand_json_paths[slug] = json_path

    return ReportArtifacts(
        global_markdown=global_markdown,
        global_html=global_html,
        global_json=global_json,
        brand_markdown_paths=brand_markdown_paths,
        brand_html_paths=brand_html_paths,
        brand_json_paths=brand_json_paths,
    )


def render_global_markdown(bundle: AnalystReportBundle) -> str:
    """Render the global scorecard as Markdown."""

    scorecard = bundle.global_scorecard
    lines = [
        "# Global Credibility Scorecard",
        "",
        f"- Generated at: `{bundle.generated_at.isoformat()}`",
        f"- Total brands: `{scorecard.total_brands}`",
        f"- Passed brands: `{scorecard.passed_brands}`",
        f"- Warning brands: `{scorecard.warning_brands}`",
        f"- Failed brands: `{scorecard.failed_brands}`",
        "",
        "## Validation Snapshot",
        "",
        f"- Findings: `{scorecard.validation.counts.get('info', 0)}` info / `{scorecard.validation.counts.get('warning', 0)}` warning / `{scorecard.validation.counts.get('error', 0)}` error",
        f"- Validation result: `{'PASS' if scorecard.validation.passed else 'FAIL'}`",
        "",
        "## Warning Counts by Category",
        "",
    ]
    if scorecard.warning_counts_by_category:
        for category, count in sorted(scorecard.warning_counts_by_category.items()):
            lines.append(f"- `{category}`: {int(count)}")
    else:
        lines.append("- No warning categories were recorded.")

    lines.extend(["", "## Weakest Provenance", ""])
    if scorecard.fields_with_weakest_provenance:
        for item in scorecard.fields_with_weakest_provenance:
            lines.append(
                f"- `{item['field']}`: grade `{item['grade']}`"
                f", average confidence `{item['average_confidence']}`"
                f", missing `{item['missing_count']}/{item['brand_count']}`"
            )
    else:
        lines.append("- No provenance summary available.")

    lines.extend(["", "## Biggest Reconciliation Errors", ""])
    if scorecard.biggest_reconciliation_errors:
        for item in scorecard.biggest_reconciliation_errors:
            lines.append(
                f"- `{item['brand_name']}` / `{item['field']}`: abs `{item['absolute_error']}`"
                f", rel `{item['relative_error']}`"
                f", grade `{item['credibility_grade']}`"
            )
    else:
        lines.append("- No comparable reconciliation errors were available.")

    lines.extend(["", "## Syntheticness", ""])
    synth = scorecard.syntheticness_overview
    lines.append(
        f"- Signals: `{synth.get('total_signals', 0)}`"
        f" | weak `{synth.get('weak', 0)}`"
        f" | moderate `{synth.get('moderate', 0)}`"
        f" | strong `{synth.get('strong', 0)}`"
        f" | unknown `{synth.get('unknown', 0)}`"
    )
    for item in synth.get("top_signals", []):
        lines.append(f"- `{item['signal_type']}` on `{item.get('field_name') or '-'}`: {item['plain_english']}")

    lines.extend(["", "## Open Issues", ""])
    if scorecard.open_issues:
        for issue in scorecard.open_issues:
            lines.append(f"- {issue}")
    else:
        lines.append("- No open issues were recorded.")

    return "\n".join(lines) + "\n"


def render_brand_markdown(scorecard: BrandScorecard, bundle: AnalystReportBundle) -> str:
    """Render a single brand scorecard as Markdown."""

    lines = [
        f"# Brand Scorecard: {scorecard.canonical_brand_name}",
        "",
        f"- Generated at: `{bundle.generated_at.isoformat()}`",
        f"- Status: `{scorecard.status}`",
        "",
        "## Normalized Metrics",
        "",
        _dict_to_markdown_table(scorecard.normalized_metrics),
        "",
        "## Invariant Results",
        "",
    ]
    if scorecard.invariant_results:
        lines.extend(_render_dict_list_as_bullets(scorecard.invariant_results))
    else:
        lines.append("- No brand-specific invariant findings were recorded.")

    lines.extend(["", "## Provenance Grades", ""])
    lines.extend(_render_dict_as_bullets(scorecard.provenance_grades))

    lines.extend(["", "## Reconciliation Errors", ""])
    if scorecard.reconciliation_error_summary:
        lines.extend(_render_dict_list_as_bullets(scorecard.reconciliation_error_summary))
    else:
        lines.append("- No reconciliation comparisons were available.")

    lines.extend(["", "## Syntheticness Signals", ""])
    if scorecard.syntheticness_signals:
        lines.extend(_render_dict_list_as_bullets(scorecard.syntheticness_signals))
    else:
        lines.append("- No brand-specific syntheticness signals were recorded.")

    lines.extend(["", "## Open Issues", ""])
    if scorecard.open_issues:
        for issue in scorecard.open_issues:
            lines.append(f"- {issue}")
    else:
        lines.append("- No open issues were recorded.")

    return "\n".join(lines) + "\n"


def render_html(
    bundle_or_mapping: AnalystReportBundle | dict[str, Any],
    *,
    mode: str = "global",
    template_path: Path | None = None,
) -> str:
    """Render the bundle as HTML using the local Jinja template."""

    template = _load_template(template_path)
    if isinstance(bundle_or_mapping, AnalystReportBundle):
        bundle = bundle_or_mapping
        context = {
            "mode": mode,
            "generated_at": bundle.generated_at.isoformat(),
            "global_scorecard": _json_safe(bundle.global_scorecard),
            "brand_scorecards": [_json_safe(scorecard) for scorecard in bundle.brand_scorecards],
            "title": "Global Credibility Scorecard" if mode == "global" else "Brand Scorecard",
            "open_issues": bundle.global_scorecard.open_issues,
        }
    else:
        context = {
            "mode": mode,
            "generated_at": bundle_or_mapping.get("generated_at"),
            "global_scorecard": _json_safe(bundle_or_mapping.get("global_scorecard")),
            "brand_scorecard": _json_safe(bundle_or_mapping.get("brand_scorecard")),
            "title": "Brand Scorecard",
        }
    return template.render(**context)


def render_json(bundle: AnalystReportBundle) -> str:
    """Render the full bundle as JSON."""

    return json.dumps(bundle.to_dict(), ensure_ascii=False, indent=2)


def _build_validation_summary(
    validation_results: dict[str, Any] | None,
    validation_flags: pd.DataFrame | None,
) -> ValidationSummary:
    if validation_results is None:
        validation_results = {}
    counts = {
        "error": int(validation_results.get("counts", {}).get("error", 0)),
        "warning": int(validation_results.get("counts", {}).get("warning", 0)),
        "info": int(validation_results.get("counts", {}).get("info", 0)),
    }

    if validation_flags is not None and not validation_flags.empty:
        warning_counts = (
            validation_flags.loc[validation_flags["severity"] == "warning", "category"]
            .fillna("unknown")
            .value_counts()
            .to_dict()
        )
        error_counts = (
            validation_flags.loc[validation_flags["severity"] == "error", "category"]
            .fillna("unknown")
            .value_counts()
            .to_dict()
        )
        findings = validation_flags.head(100).to_dict(orient="records")
    else:
        warning_counts = {}
        error_counts = {}
        findings = validation_results.get("findings", []) if isinstance(validation_results, dict) else []

    passed = int(counts["error"]) == 0
    return ValidationSummary(
        passed=passed,
        counts=counts,
        warning_counts_by_category={str(key): int(value) for key, value in warning_counts.items()},
        error_counts_by_category={str(key): int(value) for key, value in error_counts.items()},
        findings=[_json_safe(item) for item in findings],
    )


def _weakest_provenance_fields(
    reconciled: pd.DataFrame,
    provenance: pd.DataFrame | None,
) -> list[dict[str, Any]]:
    if reconciled.empty:
        return []

    fields = [
        ("rank", "rank_reference_confidence_score", "rank_credibility_grade"),
        ("store_count", "store_count_reference_confidence_score", "store_count_credibility_grade"),
        ("system_sales", "system_sales_reference_confidence_score", "system_sales_credibility_grade"),
        ("auv", "auv_reference_confidence_score", "auv_credibility_grade"),
    ]
    rows: list[dict[str, Any]] = []
    for field, confidence_column, grade_column in fields:
        if confidence_column in reconciled.columns:
            confidence_series = pd.to_numeric(reconciled[confidence_column], errors="coerce")
            missing_count = int(confidence_series.isna().sum())
            valid_confidence = confidence_series.dropna()
            average_confidence = round(float(valid_confidence.mean()), 3) if not valid_confidence.empty else None
        else:
            confidence_series = pd.Series(dtype="float64")
            missing_count = len(reconciled)
            average_confidence = None
        grade = (
            str(reconciled[grade_column].mode(dropna=True).iloc[0])
            if grade_column in reconciled and not reconciled[grade_column].dropna().empty
            else "MISSING"
        )
        rows.append(
            {
                "field": field,
                "grade": grade,
                "average_confidence": average_confidence if average_confidence is not None else "MISSING",
                "missing_count": missing_count,
                "brand_count": len(reconciled),
            }
        )
    rows.sort(
        key=lambda item: (
            -int(item["missing_count"]),
            item["average_confidence"] if isinstance(item["average_confidence"], (int, float)) else float("-inf"),
            item["field"],
        )
    )
    return rows


def _biggest_reconciliation_errors(reconciled: pd.DataFrame) -> list[dict[str, Any]]:
    if reconciled.empty:
        return []

    candidates: list[dict[str, Any]] = []
    for row in reconciled.to_dict(orient="records"):
        for field in ["rank", "store_count", "system_sales", "auv"]:
            abs_key = f"{field}_absolute_error"
            rel_key = f"{field}_relative_error"
            grade_key = f"{field}_credibility_grade"
            abs_error = row.get(abs_key)
            rel_error = row.get(rel_key)
            if abs_error is None and rel_error is None:
                continue
            candidates.append(
                {
                    "brand_name": row.get("canonical_brand_name") or row.get("brand_name"),
                    "field": field,
                    "absolute_error": abs_error,
                    "relative_error": rel_error,
                    "credibility_grade": row.get(grade_key),
                }
            )
    candidates.sort(
        key=lambda item: (
            _maybe_float(item["relative_error"]) if _maybe_float(item["relative_error"]) is not None else -1.0,
            _maybe_float(item["absolute_error"]) if _maybe_float(item["absolute_error"]) is not None else -1.0,
        ),
        reverse=True,
    )
    return candidates[:8]


def _syntheticness_overview(syntheticness: pd.DataFrame | None) -> dict[str, Any]:
    if syntheticness is None or syntheticness.empty:
        return {"total_signals": 0, "weak": 0, "moderate": 0, "strong": 0, "unknown": 0, "top_signals": []}

    counts = Counter(str(value) for value in syntheticness["strength"].fillna("unknown"))
    strength_order = {"strong": 0, "moderate": 1, "weak": 2, "info": 3, "unknown": 4}

    def _signal_sort_key(item: dict[str, Any]) -> tuple[int, float, str]:
        strength = str(item.get("strength") or "unknown").casefold()
        score = _maybe_float(item.get("score"))
        return (
            strength_order.get(strength, 4),
            -(score if score is not None else -1.0),
            str(item.get("signal_type") or ""),
        )

    top_signals = sorted(syntheticness.to_dict(orient="records"), key=_signal_sort_key)[:5]
    return {
        "total_signals": int(len(syntheticness)),
        "weak": int(counts.get("weak", 0)),
        "moderate": int(counts.get("moderate", 0)),
        "strong": int(counts.get("strong", 0)),
        "unknown": int(counts.get("unknown", 0)),
        "top_signals": [_json_safe(item) for item in top_signals],
    }


def _global_open_issues(
    validation_summary: ValidationSummary,
    reconciled: pd.DataFrame,
    syntheticness: pd.DataFrame | None,
) -> list[str]:
    issues: list[str] = []
    if not validation_summary.passed:
        issues.append(
            f"Validation produced {validation_summary.counts.get('error', 0)} error(s) and {validation_summary.counts.get('warning', 0)} warning(s)."
        )
    if reconciled.empty:
        issues.append("No reconciled core metrics were available.")
    if reconciled is not None and not reconciled.empty:
        missing = int((reconciled["reference_source_count"] == 0).sum())
        if missing:
            issues.append(f"{missing} brands have no reference coverage yet.")
    if syntheticness is not None and not syntheticness.empty:
        issues.append("Syntheticness signals are present; use them as triage inputs, not proof of fabrication.")
    return issues


def _brand_validation_rows(validation_flags: pd.DataFrame, brand_name: str) -> list[dict[str, Any]]:
    if validation_flags.empty or "brand_name" not in validation_flags:
        return []
    matches = validation_flags[validation_flags["brand_name"].fillna("").astype(str) == brand_name]
    return [_json_safe(item) for item in matches.to_dict(orient="records")]


def _brand_syntheticness_rows(syntheticness: pd.DataFrame, brand_name: str) -> list[dict[str, Any]]:
    if syntheticness.empty:
        return []
    rows: list[dict[str, Any]] = []
    brand_key = brand_name.casefold()
    for item in syntheticness.to_dict(orient="records"):
        text = " ".join(str(item.get(key) or "") for key in ["title", "plain_english", "observed", "expected"])
        if brand_key in text.casefold():
            rows.append(_json_safe(item))
    return rows


def _brand_status(
    row: dict[str, Any],
    validation_rows: list[dict[str, Any]],
    synthetic_rows: list[dict[str, Any]],
) -> str:
    if row.get("overall_credibility_grade") in {"D", "F", "MISSING"}:
        return "fail"
    if row.get("reference_source_count", 0) == 0:
        return "fail"
    if validation_rows or synthetic_rows or row.get("reconciliation_warning"):
        return "warn"
    return "pass"


def _brand_open_issues(
    row: dict[str, Any],
    validation_rows: list[dict[str, Any]],
    synthetic_rows: list[dict[str, Any]],
) -> list[str]:
    issues: list[str] = []
    warning = row.get("reconciliation_warning")
    if warning:
        issues.extend([segment.strip() for segment in str(warning).split("|") if segment.strip()])
    for finding in validation_rows:
        severity = finding.get("severity")
        message = finding.get("message")
        if severity in {"error", "warning"} and message:
            issues.append(str(message))
    for signal in synthetic_rows:
        if signal.get("strength") in {"weak", "moderate", "strong"}:
            issues.append(str(signal.get("plain_english")))
    if not issues:
        issues.append("No open issues recorded.")
    # Deduplicate while preserving order.
    seen: set[str] = set()
    deduped: list[str] = []
    for issue in issues:
        if issue not in seen:
            seen.add(issue)
            deduped.append(issue)
    return deduped


def _normalized_metrics(row: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "brand_name",
        "canonical_brand_name",
        "rank",
        "us_store_count_2024",
        "systemwide_revenue_usd_billions_2024",
        "average_unit_volume_usd_thousands",
        "fte_mid",
        "margin_mid_pct",
        "overall_credibility_grade",
        "brand_match_confidence",
        "reference_source_count",
    ]
    return {key: _json_safe(row.get(key)) for key in keys if key in row}


def _reconciliation_error_summary(row: dict[str, Any]) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for field in ["rank", "store_count", "system_sales", "auv"]:
        abs_error = row.get(f"{field}_absolute_error")
        rel_error = row.get(f"{field}_relative_error")
        reference_value = row.get(f"{field}_reference_value")
        if abs_error is None and rel_error is None and reference_value is None:
            continue
        summary.append(
            {
                "field": field,
                "reference_value": reference_value,
                "absolute_error": abs_error,
                "relative_error": rel_error,
                "credibility_grade": row.get(f"{field}_credibility_grade"),
                "reference_source_name": row.get(f"{field}_reference_source_name"),
            }
        )
    return summary


def _field_grade(row: dict[str, Any], field: str) -> dict[str, Any]:
    return {
        "grade": row.get(f"{field}_credibility_grade"),
        "reference_value": row.get(f"{field}_reference_value"),
        "absolute_error": row.get(f"{field}_absolute_error"),
        "relative_error": row.get(f"{field}_relative_error"),
        "reference_source_name": row.get(f"{field}_reference_source_name"),
        "reference_source_type": row.get(f"{field}_reference_source_type"),
        "reference_confidence_score": row.get(f"{field}_reference_confidence_score"),
    }


def _collect_reconciliation_warning_categories(reconciled: pd.DataFrame) -> list[tuple[str, int]]:
    if reconciled.empty or "reconciliation_warning" not in reconciled:
        return []
    categories: Counter[str] = Counter()
    for warning in reconciled["reconciliation_warning"].dropna().astype(str):
        for segment in (piece.strip() for piece in warning.split("|") if piece.strip()):
            lowered = segment.casefold()
            if "no reference coverage found" in lowered:
                categories["coverage"] += 1
            elif "no" in lowered and "reference value" in lowered:
                categories["missing_reference_value"] += 1
            else:
                categories["reconciliation_warning"] += 1
    return list(categories.items())


def _resolve_optional_path(*paths: Path) -> Path:
    for path in paths:
        if path.exists():
            return path
    return paths[0]


def _read_parquet_if_exists(path: Path) -> pd.DataFrame | None:
    if path.exists():
        return pd.read_parquet(path)
    return None


def _read_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _load_template(template_path: Path | None) -> Template:
    search_path = template_path.parent if template_path else Path(__file__).with_name("templates")
    env = Environment(
        loader=FileSystemLoader(str(search_path)),
        autoescape=select_autoescape(["html", "xml", "j2"]),
    )
    try:
        return env.get_template(template_path.name if template_path else DEFAULT_TEMPLATE_NAME)
    except Exception:
        return env.from_string(
            "<html><body><pre>{{ global_scorecard | tojson(indent=2) }}</pre></body></html>"
        )


def _dict_to_markdown_table(data: dict[str, Any]) -> str:
    if not data:
        return "- No metrics available."
    rows = ["| Field | Value |", "|---|---|"]
    for key, value in data.items():
        rows.append(f"| {key} | {_escape_markdown(str(value))} |")
    return "\n".join(rows)


def _render_dict_as_bullets(data: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for key, value in data.items():
        lines.append(f"- `{key}`: `{_json_safe(value)}`")
    return lines or ["- No data available."]


def _render_dict_list_as_bullets(items: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for item in items:
        title = item.get("field") or item.get("check_name") or item.get("signal_type") or "item"
        message = item.get("message") or item.get("plain_english") or item.get("status") or ""
        lines.append(f"- `{title}`: {message}")
    return lines or ["- No records available."]


def _escape_markdown(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", "<br>")


def _maybe_float(value: Any) -> float | None:
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number


def _json_safe(value: Any) -> Any:
    if is_dataclass(value):
        return _json_safe(asdict(value))
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.DataFrame):
        return [_json_safe(item) for item in value.to_dict(orient="records")]
    if isinstance(value, pd.Series):
        return _json_safe(value.to_dict())
    if hasattr(value, "item") and callable(value.item):
        try:
            return value.item()
        except (TypeError, ValueError):
            return str(value)
    return value


def slugify(value: str) -> str:
    """Create a stable file name slug."""

    text = value.casefold()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "brand"


__all__ = [
    "AnalystReportBundle",
    "BrandScorecard",
    "DEFAULT_DATA_ROOT",
    "DEFAULT_REPORT_ROOT",
    "DEFAULT_TEMPLATE_NAME",
    "GlobalScorecard",
    "ReportArtifacts",
    "ValidationSummary",
    "build_analyst_report_bundle",
    "build_brand_scorecards",
    "build_global_scorecard",
    "render_brand_markdown",
    "render_global_markdown",
    "render_html",
    "render_json",
    "slugify",
    "write_analyst_reports",
]
