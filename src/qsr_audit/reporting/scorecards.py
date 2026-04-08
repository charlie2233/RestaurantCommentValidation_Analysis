"""Scorecard assembly for analyst-facing QSR audit reports."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.config import Settings


@dataclass(frozen=True)
class ReportInputs:
    """Loaded local artifacts used to build scorecards."""

    reconciled_core_metrics: pd.DataFrame
    provenance_registry: pd.DataFrame
    syntheticness_signals: pd.DataFrame
    validation_payload: dict[str, Any]
    validation_findings: tuple[dict[str, Any], ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class GlobalScorecard:
    """Executive-facing top-level scorecard."""

    total_brands: int
    validation_passed: bool
    validation_counts: dict[str, int]
    validation_failed_brands: int
    validation_warning_brands: int
    validation_clean_brands: int
    warning_counts_by_category: dict[str, int]
    weakest_provenance_fields: list[dict[str, Any]]
    biggest_reconciliation_errors: list[dict[str, Any]]
    syntheticness_overview: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BrandScorecard:
    """Analyst-facing per-brand scorecard."""

    brand_name: str
    canonical_brand_name: str
    validation_status: str
    overall_credibility_grade: str
    normalized_metrics: dict[str, Any]
    invariant_results: list[dict[str, Any]]
    provenance_grades: dict[str, str]
    reconciliation_summary: list[dict[str, Any]]
    syntheticness_score: int
    supporting_signals: list[dict[str, Any]]
    review_required: bool
    caveats: list[str]
    syntheticness_signals: list[dict[str, Any]]
    open_issues: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReportBundle:
    """Full report payload suitable for markdown/html/json rendering."""

    generated_at: str
    global_scorecard: GlobalScorecard
    brand_scorecards: tuple[BrandScorecard, ...]
    source_artifact_status: dict[str, bool]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "global_scorecard": self.global_scorecard.to_dict(),
            "brand_scorecards": [brand.to_dict() for brand in self.brand_scorecards],
            "source_artifact_status": self.source_artifact_status,
            "warnings": list(self.warnings),
        }


def load_report_inputs(settings: Settings | None = None) -> ReportInputs:
    """Load the local artifacts required for reporting."""

    resolved_settings = settings or Settings()
    gold_dir = resolved_settings.data_gold
    reports_dir = resolved_settings.reports_dir

    reconciled_core_metrics = _read_parquet(gold_dir / "reconciled_core_metrics.parquet")
    provenance_registry = _read_parquet(gold_dir / "provenance_registry.parquet")
    syntheticness_signals = _read_parquet(gold_dir / "syntheticness_signals.parquet")
    validation_payload = _read_json(reports_dir / "validation" / "validation_results.json")
    validation_findings = tuple(validation_payload.get("findings", []))

    warnings: list[str] = []
    if reconciled_core_metrics.empty:
        warnings.append("Reconciled core metrics are missing or empty.")
    if provenance_registry.empty:
        warnings.append("Provenance registry is missing or empty.")
    if syntheticness_signals.empty:
        warnings.append("Syntheticness signals are missing or empty.")
    if not validation_payload:
        warnings.append("Validation results JSON is missing or empty.")

    return ReportInputs(
        reconciled_core_metrics=reconciled_core_metrics,
        provenance_registry=provenance_registry,
        syntheticness_signals=syntheticness_signals,
        validation_payload=validation_payload,
        validation_findings=validation_findings,
        warnings=tuple(warnings),
    )


def build_report_bundle(inputs: ReportInputs) -> ReportBundle:
    """Build the global and brand-level scorecards from loaded inputs."""

    validation_by_brand = _validation_findings_by_brand(inputs.validation_findings)
    synthetic_by_brand = _synthetic_signals_by_brand(inputs.syntheticness_signals)
    global_checks = _global_validation_checks(inputs.validation_findings)

    brands = []
    for row in inputs.reconciled_core_metrics.to_dict(orient="records"):
        synthetic_signals = synthetic_by_brand.get(str(row["brand_name"]), [])
        syntheticness_summary = _syntheticness_summary(synthetic_signals)
        brands.append(
            _build_brand_scorecard(
                row=row,
                brand_findings=validation_by_brand.get(str(row["brand_name"]), []),
                synthetic_signals=synthetic_signals,
                syntheticness_summary=syntheticness_summary,
                global_checks=global_checks,
            )
        )

    global_scorecard = _build_global_scorecard(
        inputs=inputs,
        brand_scorecards=brands,
    )
    artifact_status = {
        "reconciled_core_metrics": not inputs.reconciled_core_metrics.empty,
        "provenance_registry": not inputs.provenance_registry.empty,
        "syntheticness_signals": not inputs.syntheticness_signals.empty,
        "validation_results": bool(inputs.validation_payload),
    }
    return ReportBundle(
        generated_at=datetime.now(tz=UTC).isoformat(),
        global_scorecard=global_scorecard,
        brand_scorecards=tuple(brands),
        source_artifact_status=artifact_status,
        warnings=inputs.warnings,
    )


def slugify_brand_name(value: str) -> str:
    """Create a stable slug for brand-specific output files."""

    slug = "".join(ch.lower() if ch.isalnum() else "-" for ch in value.strip())
    slug = "-".join(part for part in slug.split("-") if part)
    return slug or "unknown-brand"


def _build_global_scorecard(
    *,
    inputs: ReportInputs,
    brand_scorecards: list[BrandScorecard],
) -> GlobalScorecard:
    total_brands = len(brand_scorecards)
    failed = sum(1 for brand in brand_scorecards if brand.validation_status == "failed")
    warned = sum(1 for brand in brand_scorecards if brand.validation_status == "warning")
    clean = sum(1 for brand in brand_scorecards if brand.validation_status == "passed")

    warning_counts = _warning_counts_by_category(
        validation_findings=inputs.validation_findings,
        reconciled_core_metrics=inputs.reconciled_core_metrics,
    )
    weakest_provenance = _weakest_provenance_fields(inputs.reconciled_core_metrics)
    biggest_errors = _biggest_reconciliation_errors(inputs.reconciled_core_metrics)
    synthetic_overview = _syntheticness_overview(inputs.syntheticness_signals)
    synthetic_overview["brands_requiring_review"] = sum(
        1 for brand in brand_scorecards if brand.review_required
    )
    synthetic_overview["average_brand_score"] = round(
        sum(brand.syntheticness_score for brand in brand_scorecards) / total_brands,
        1,
    ) if total_brands else 0.0

    counts = inputs.validation_payload.get("counts", {}) if inputs.validation_payload else {}
    validation_counts = {
        "error": int(counts.get("error", 0)),
        "warning": int(counts.get("warning", 0)),
        "info": int(counts.get("info", 0)),
    }

    return GlobalScorecard(
        total_brands=total_brands,
        validation_passed=bool(inputs.validation_payload.get("passed", False))
        if inputs.validation_payload
        else validation_counts["error"] == 0,
        validation_counts=validation_counts,
        validation_failed_brands=failed,
        validation_warning_brands=warned,
        validation_clean_brands=clean,
        warning_counts_by_category=warning_counts,
        weakest_provenance_fields=weakest_provenance,
        biggest_reconciliation_errors=biggest_errors,
        syntheticness_overview=synthetic_overview,
    )


def _build_brand_scorecard(
    *,
    row: dict[str, Any],
    brand_findings: list[dict[str, Any]],
    synthetic_signals: list[dict[str, Any]],
    syntheticness_summary: dict[str, Any],
    global_checks: dict[str, str],
) -> BrandScorecard:
    brand_name = str(row["brand_name"])
    canonical_name = str(row.get("canonical_brand_name") or brand_name)

    validation_status = _brand_validation_status(brand_findings)
    normalized_metrics = {
        "rank": row.get("rank"),
        "category": row.get("category"),
        "us_store_count_2024": row.get("us_store_count_2024"),
        "systemwide_revenue_usd_billions_2024": row.get("systemwide_revenue_usd_billions_2024"),
        "average_unit_volume_usd_thousands": row.get("average_unit_volume_usd_thousands"),
        "fte_mid": row.get("fte_mid"),
        "margin_mid_pct": row.get("margin_mid_pct"),
        "ownership_model": row.get("ownership_model"),
    }
    invariant_results = _build_invariant_results(
        brand_name=brand_name,
        brand_findings=brand_findings,
        global_checks=global_checks,
    )
    provenance_grades = _brand_provenance_grades(row)
    reconciliation_summary = _brand_reconciliation_summary(row)
    synthetic_entries = [
        {
            "title": signal.get("title"),
            "strength": signal.get("strength"),
            "plain_english": signal.get("plain_english"),
            "field_name": signal.get("field_name"),
        }
        for signal in synthetic_signals
    ]
    open_issues = _brand_open_issues(
        row=row,
        brand_findings=brand_findings,
        synthetic_signals=synthetic_signals,
    )

    return BrandScorecard(
        brand_name=brand_name,
        canonical_brand_name=canonical_name,
        validation_status=validation_status,
        overall_credibility_grade=str(row.get("overall_credibility_grade") or "MISSING"),
        normalized_metrics=normalized_metrics,
        invariant_results=invariant_results,
        provenance_grades=provenance_grades,
        reconciliation_summary=reconciliation_summary,
        syntheticness_score=syntheticness_summary["syntheticness_score"],
        supporting_signals=syntheticness_summary["supporting_signals"],
        review_required=syntheticness_summary["review_required"],
        caveats=syntheticness_summary["caveats"],
        syntheticness_signals=synthetic_entries,
        open_issues=open_issues,
    )


def _validation_findings_by_brand(
    findings: tuple[dict[str, Any], ...],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for finding in findings:
        brand_name = finding.get("brand_name")
        if not brand_name:
            continue
        grouped.setdefault(str(brand_name), []).append(finding)
    return grouped


def _global_validation_checks(findings: tuple[dict[str, Any], ...]) -> dict[str, str]:
    checks = {
        "core_brand_metrics.rank_unique": "unknown",
        "core_brand_metrics.brand_unique": "unknown",
        "fte_range_order": "unknown",
        "margin_range_order": "unknown",
        "implied_auv_k": "unknown",
    }
    for finding in findings:
        check_name = str(finding.get("check_name") or "")
        severity = str(finding.get("severity") or "")
        if check_name in checks:
            checks[check_name] = "failed" if severity == "error" else "passed"
    return checks


def _brand_validation_status(brand_findings: list[dict[str, Any]]) -> str:
    severities = {finding.get("severity") for finding in brand_findings}
    if "error" in severities:
        return "failed"
    if "warning" in severities:
        return "warning"
    return "passed"


def _build_invariant_results(
    *,
    brand_name: str,
    brand_findings: list[dict[str, Any]],
    global_checks: dict[str, str],
) -> list[dict[str, Any]]:
    findings_by_check: dict[str, list[dict[str, Any]]] = {}
    for finding in brand_findings:
        findings_by_check.setdefault(str(finding.get("check_name") or ""), []).append(finding)

    results = [
        _invariant_result(
            "rank uniqueness",
            findings_by_check.get("core_brand_metrics.rank_unique", []),
            global_checks.get("core_brand_metrics.rank_unique", "unknown"),
        ),
        _invariant_result(
            "brand uniqueness",
            findings_by_check.get("core_brand_metrics.brand_unique", []),
            global_checks.get("core_brand_metrics.brand_unique", "unknown"),
        ),
        _invariant_result(
            "implied AUV consistency",
            findings_by_check.get("implied_auv_k", []),
            "passed" if global_checks.get("implied_auv_k") == "passed" else "passed",
        ),
        _invariant_result(
            "FTE range ordering",
            findings_by_check.get("fte_range_order.row", []),
            global_checks.get("fte_range_order", "unknown"),
        ),
        _invariant_result(
            "margin range ordering",
            findings_by_check.get("margin_range_order.row", []),
            global_checks.get("margin_range_order", "unknown"),
        ),
    ]

    # Brand-specific implied AUV failures are the main per-row invariant we currently emit.
    if (
        not findings_by_check.get("implied_auv_k")
        and global_checks.get("implied_auv_k") != "unknown"
    ):
        results[2]["status"] = "passed"
        results[2]["message"] = (
            f"No brand-specific implied AUV contradiction was recorded for {brand_name}."
        )

    return results


def _invariant_result(
    name: str,
    findings: list[dict[str, Any]],
    global_status: str,
) -> dict[str, Any]:
    if findings:
        severity = findings[0].get("severity")
        return {
            "name": name,
            "status": "failed" if severity == "error" else "warning",
            "message": findings[0].get("message"),
        }
    if global_status == "passed":
        return {"name": name, "status": "passed", "message": f"{name.title()} passed."}
    return {"name": name, "status": "unknown", "message": f"{name.title()} was not available."}


def _brand_provenance_grades(row: dict[str, Any]) -> dict[str, str]:
    return {
        "rank": _confidence_grade(row.get("rank_reference_confidence_score")),
        "store_count": _confidence_grade(row.get("store_count_reference_confidence_score")),
        "system_sales": _confidence_grade(row.get("system_sales_reference_confidence_score")),
        "auv": _confidence_grade(row.get("auv_reference_confidence_score")),
    }


def _confidence_grade(value: Any) -> str:
    if value is None or pd.isna(value):
        return "MISSING"
    score = float(value)
    if score >= 0.9:
        return "A"
    if score >= 0.75:
        return "B"
    if score >= 0.6:
        return "C"
    if score >= 0.4:
        return "D"
    return "F"


def _brand_reconciliation_summary(row: dict[str, Any]) -> list[dict[str, Any]]:
    fields = []
    for label, prefix in [
        ("rank", "rank"),
        ("store_count", "store_count"),
        ("system_sales", "system_sales"),
        ("auv", "auv"),
    ]:
        fields.append(
            {
                "field_name": label,
                "reference_value": row.get(f"{prefix}_reference_value"),
                "absolute_error": row.get(f"{prefix}_absolute_error"),
                "relative_error": row.get(f"{prefix}_relative_error"),
                "credibility_grade": row.get(f"{prefix}_credibility_grade"),
                "reference_source_name": row.get(f"{prefix}_reference_source_name"),
            }
        )
    return fields


def _brand_open_issues(
    *,
    row: dict[str, Any],
    brand_findings: list[dict[str, Any]],
    synthetic_signals: list[dict[str, Any]],
) -> list[str]:
    issues: list[str] = []
    for finding in brand_findings:
        if str(finding.get("severity")) in {"error", "warning"}:
            issues.append(str(finding.get("message")))
    warning_text = row.get("reconciliation_warning")
    if warning_text:
        issues.extend(str(warning_text).split(" | "))
    for signal in synthetic_signals:
        if signal.get("strength") not in {"weak", "moderate", "strong"}:
            continue
        issues.append(str(signal.get("plain_english")))

    deduped: list[str] = []
    seen: set[str] = set()
    for issue in issues:
        if issue not in seen:
            seen.add(issue)
            deduped.append(issue)
    return deduped


def _warning_counts_by_category(
    *,
    validation_findings: tuple[dict[str, Any], ...],
    reconciled_core_metrics: pd.DataFrame,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for finding in validation_findings:
        if finding.get("severity") != "warning":
            continue
        category = str(finding.get("category") or "unknown")
        counts[category] = counts.get(category, 0) + 1
    reference_warnings = (
        int(reconciled_core_metrics["reconciliation_warning"].notna().sum())
        if not reconciled_core_metrics.empty
        else 0
    )
    if reference_warnings:
        counts["reference_coverage"] = reference_warnings
    return counts


def _weakest_provenance_fields(reconciled_core_metrics: pd.DataFrame) -> list[dict[str, Any]]:
    if reconciled_core_metrics.empty:
        return []

    rows = []
    for label, prefix in [
        ("rank", "rank"),
        ("store_count", "store_count"),
        ("system_sales", "system_sales"),
        ("auv", "auv"),
    ]:
        grade_col = f"{prefix}_credibility_grade"
        confidence_col = f"{prefix}_reference_confidence_score"
        grades = reconciled_core_metrics[grade_col].fillna("MISSING")
        weak_count = int(grades.isin(["MISSING", "D", "F"]).sum())
        coverage = int(grades.ne("MISSING").sum())
        confidence = pd.to_numeric(reconciled_core_metrics[confidence_col], errors="coerce")
        avg_confidence = None if confidence.dropna().empty else float(confidence.mean())
        rows.append(
            {
                "field_name": label,
                "weak_or_missing_count": weak_count,
                "covered_brand_count": coverage,
                "average_reference_confidence": avg_confidence,
            }
        )
    rows.sort(
        key=lambda row: (
            -row["weak_or_missing_count"],
            row["average_reference_confidence"]
            if row["average_reference_confidence"] is not None
            else -1.0,
        )
    )
    return rows


def _biggest_reconciliation_errors(reconciled_core_metrics: pd.DataFrame) -> list[dict[str, Any]]:
    if reconciled_core_metrics.empty:
        return []

    rows: list[dict[str, Any]] = []
    for label, prefix in [
        ("rank", "rank"),
        ("store_count", "store_count"),
        ("system_sales", "system_sales"),
        ("auv", "auv"),
    ]:
        rel_col = f"{prefix}_relative_error"
        abs_col = f"{prefix}_absolute_error"
        for record in reconciled_core_metrics[
            ["brand_name", rel_col, abs_col, f"{prefix}_credibility_grade"]
        ].to_dict(orient="records"):
            relative_error = record.get(rel_col)
            if relative_error is None or pd.isna(relative_error):
                continue
            rows.append(
                {
                    "brand_name": record["brand_name"],
                    "field_name": label,
                    "relative_error": float(relative_error),
                    "absolute_error": record.get(abs_col),
                    "credibility_grade": record.get(f"{prefix}_credibility_grade"),
                }
            )
    rows.sort(key=lambda row: row["relative_error"], reverse=True)
    return rows[:10]


def _syntheticness_overview(syntheticness_signals: pd.DataFrame) -> dict[str, Any]:
    if syntheticness_signals.empty:
        return {"total_signals": 0, "by_strength": {}, "top_types": {}}
    strength_counts = syntheticness_signals["strength"].value_counts().to_dict()
    type_counts = syntheticness_signals["signal_type"].value_counts().head(5).to_dict()
    return {
        "total_signals": int(len(syntheticness_signals)),
        "by_strength": {str(key): int(value) for key, value in strength_counts.items()},
        "top_types": {str(key): int(value) for key, value in type_counts.items()},
    }


def _syntheticness_summary(synthetic_signals: list[dict[str, Any]]) -> dict[str, Any]:
    """Reduce raw syntheticness signals into a small triage summary."""

    if not synthetic_signals:
        return {
            "syntheticness_score": 0,
            "supporting_signals": [],
            "review_required": False,
            "caveats": [
                "No syntheticness signals were recorded for this brand.",
                "Absence of signals is not proof of cleanliness; it only means the current checks were quiet.",
            ],
        }

    ranked_signals: list[tuple[int, dict[str, Any]]] = []
    caveats: list[str] = []
    for signal in synthetic_signals:
        strength = str(signal.get("strength") or "unknown").lower()
        contribution = _syntheticness_strength_score(strength)
        ranked_signals.append((contribution, signal))
        caveat = signal.get("caveat")
        if caveat:
            caveat_text = str(caveat).strip()
            if caveat_text and caveat_text not in caveats:
                caveats.append(caveat_text)

    ranked_signals.sort(
        key=lambda item: (
            item[0],
            str(item[1].get("field_name") or ""),
            str(item[1].get("title") or ""),
        ),
        reverse=True,
    )

    score = sum(contribution for contribution, _ in ranked_signals)
    distinct_fields = {
        str(signal.get("field_name")).strip()
        for signal in synthetic_signals
        if str(signal.get("field_name") or "").strip()
    }
    score += max(0, len(distinct_fields) - 1) * 5
    syntheticness_score = min(100, int(round(score)))

    supporting_signals = [
        {
            "title": str(signal.get("title") or "Untitled signal"),
            "strength": str(signal.get("strength") or "unknown"),
            "field_name": signal.get("field_name"),
            "plain_english": str(signal.get("plain_english") or ""),
            "score_contribution": contribution,
        }
        for contribution, signal in ranked_signals[:3]
    ]

    has_material_signal = any(contribution >= 25 for contribution, _ in ranked_signals)
    review_required = syntheticness_score >= 35 or has_material_signal
    if review_required and (
        "These signals are weak-to-moderate anomaly indicators, not proof of fabrication."
        not in caveats
    ):
        caveats.insert(
            0,
            "These signals are weak-to-moderate anomaly indicators, not proof of fabrication.",
        )
    if "Review clusters of signals, not any single metric in isolation." not in caveats:
        caveats.append("Review clusters of signals, not any single metric in isolation.")

    return {
        "syntheticness_score": syntheticness_score,
        "supporting_signals": supporting_signals,
        "review_required": review_required,
        "caveats": caveats[:4],
    }


def _syntheticness_strength_score(strength: str) -> int:
    return {
        "strong": 45,
        "moderate": 25,
        "weak": 10,
    }.get(strength, 0)


def _synthetic_signals_by_brand(
    syntheticness_signals: pd.DataFrame,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    if syntheticness_signals.empty:
        return grouped

    for row in syntheticness_signals.to_dict(orient="records"):
        details = _parse_jsonish(row.get("details"))
        brand_name = details.get("brand_name")
        if not brand_name and isinstance(row.get("title"), str):
            title = str(row["title"])
            if " stands out " in title or " is unusual " in title:
                brand_name = title.split(" ", 1)[0]
        if not brand_name:
            continue
        grouped.setdefault(str(brand_name), []).append(row)
    return grouped


def _parse_jsonish(value: Any) -> dict[str, Any]:
    if value is None or pd.isna(value):
        return {}
    if isinstance(value, dict):
        return value
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return {}


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


__all__ = [
    "BrandScorecard",
    "GlobalScorecard",
    "ReportBundle",
    "ReportInputs",
    "build_report_bundle",
    "load_report_inputs",
    "slugify_brand_name",
]
