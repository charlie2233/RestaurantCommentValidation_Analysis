"""Gold publishing gate pipeline for export decisions and audit scorecards."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.config import Settings
from qsr_audit.gold.policy import GOLD_PUBLISH_POLICY_V1, GoldPublishPolicy, MetricGatePolicy
from qsr_audit.gold.reporting import build_gold_publish_summary, render_gold_publish_scorecard

FIELD_NAME_TO_POLICY_METRIC = {
    "rank": "rank",
    "us_store_count_2024": "store_count",
    "systemwide_revenue_usd_billions_2024": "system_sales",
    "average_unit_volume_usd_thousands": "auv",
    "implied_auv_k": "auv",
    "fte_mid": "fte_mid",
    "margin_mid_pct": "margin_mid_pct",
}

CHECK_NAME_TO_POLICY_METRICS = {
    "core_brand_metrics.rank_unique": {"rank"},
    "core_brand_metrics.brand_unique": {"rank", "store_count", "system_sales", "auv"},
    "implied_auv_k": {"auv"},
    "fte_range_order": {"fte_mid"},
    "fte_range_order.row": {"fte_mid"},
    "margin_range_order": {"margin_mid_pct"},
    "margin_range_order.row": {"margin_mid_pct"},
}


@dataclass(frozen=True)
class GoldGateInputs:
    """Gold-layer artifacts required for publishing decisions."""

    reconciled_core_metrics: pd.DataFrame
    provenance_registry: pd.DataFrame
    validation_flags: pd.DataFrame
    reference_coverage: pd.DataFrame
    syntheticness_signals: pd.DataFrame


@dataclass(frozen=True)
class GoldGateArtifacts:
    """Written outputs for Gold publishing gates."""

    decisions_path: Path
    publishable_path: Path
    blocked_path: Path
    scorecard_markdown_path: Path
    summary_json_path: Path


@dataclass(frozen=True)
class GoldGateRun:
    """Complete gate execution result."""

    policy_id: str
    policy_version: str
    decisions: pd.DataFrame
    summary: dict[str, Any]
    artifacts: GoldGateArtifacts


def gate_gold_publish(
    settings: Settings | None = None,
    *,
    policy: GoldPublishPolicy = GOLD_PUBLISH_POLICY_V1,
    gold_dir: Path | None = None,
    report_dir: Path | None = None,
) -> GoldGateRun:
    """Evaluate Gold export decisions from previously generated Gold artifacts."""

    resolved_settings = settings or Settings()
    resolved_gold_dir = (gold_dir or resolved_settings.data_gold).expanduser().resolve()
    resolved_report_dir = (
        (report_dir or (resolved_settings.reports_dir / "audit")).expanduser().resolve()
    )

    inputs = load_gold_gate_inputs(gold_dir=resolved_gold_dir)
    decisions = build_gold_publish_decisions(inputs=inputs, policy=policy)
    summary = build_gold_gate_summary(inputs=inputs, decisions=decisions, policy=policy)
    artifacts = write_gold_gate_outputs(
        decisions=decisions,
        summary=summary,
        gold_dir=resolved_gold_dir,
        report_dir=resolved_report_dir,
        policy=policy,
    )
    return GoldGateRun(
        policy_id=policy.policy_id,
        policy_version=policy.version,
        decisions=decisions,
        summary=summary,
        artifacts=artifacts,
    )


def load_gold_gate_inputs(*, gold_dir: Path) -> GoldGateInputs:
    """Load the Gold artifacts required by the publish gate."""

    required_paths = {
        "reconciled_core_metrics": gold_dir / "reconciled_core_metrics.parquet",
        "provenance_registry": gold_dir / "provenance_registry.parquet",
        "validation_flags": gold_dir / "validation_flags.parquet",
        "reference_coverage": gold_dir / "reference_coverage.parquet",
        "syntheticness_signals": gold_dir / "syntheticness_signals.parquet",
    }
    missing = [str(path) for path in required_paths.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Gold publishing gates require existing Gold artifacts. Missing: "
            + ", ".join(missing)
            + ". Run validate-workbook, run-syntheticness, and reconcile first."
        )

    reconciled_core_metrics = pd.read_parquet(required_paths["reconciled_core_metrics"])
    provenance_registry = _parse_json_columns(
        pd.read_parquet(required_paths["provenance_registry"]),
        columns=("extra",),
    )
    validation_flags = _parse_json_columns(
        pd.read_parquet(required_paths["validation_flags"]),
        columns=("details",),
    )
    reference_coverage = _parse_json_columns(
        pd.read_parquet(required_paths["reference_coverage"]),
        columns=("missing_metrics", "missing_brands", "source_type_names", "details"),
    )
    syntheticness_signals = _parse_json_columns(
        pd.read_parquet(required_paths["syntheticness_signals"]),
        columns=("details",),
    )

    return GoldGateInputs(
        reconciled_core_metrics=reconciled_core_metrics,
        provenance_registry=provenance_registry,
        validation_flags=validation_flags,
        reference_coverage=reference_coverage,
        syntheticness_signals=syntheticness_signals,
    )


def build_gold_publish_decisions(
    *,
    inputs: GoldGateInputs,
    policy: GoldPublishPolicy,
) -> pd.DataFrame:
    """Evaluate one Gold publishing decision row per brand and policy metric."""

    decision_rows: list[dict[str, Any]] = []
    policy_map = policy.metric_policy_map()
    provenance_by_brand = _provenance_by_brand(inputs.provenance_registry)
    validation_by_brand, missing_ai_brands, _extra_ai_brands, _auv_mismatch_brands = (
        _validation_context(inputs.validation_flags)
    )
    brand_coverage = _brand_reference_coverage(inputs.reference_coverage)
    synthetic_by_brand_metric = _syntheticness_by_brand_metric(
        inputs.syntheticness_signals,
        warning_strengths=policy.synthetic_warning_strengths,
    )

    for row in inputs.reconciled_core_metrics.to_dict(orient="records"):
        brand_name = str(row.get("brand_name") or "unknown")
        canonical_brand_name = str(row.get("canonical_brand_name") or brand_name)
        brand_findings = _brand_findings_for_keys(
            validation_by_brand,
            keys=(brand_name, canonical_brand_name),
        )
        coverage_row = brand_coverage.get(canonical_brand_name, {})

        for metric_policy in policy_map.values():
            decision_rows.append(
                _evaluate_metric_decision(
                    row=row,
                    coverage_row=coverage_row,
                    brand_findings=brand_findings,
                    synthetic_warnings=synthetic_by_brand_metric.get(
                        (canonical_brand_name, metric_policy.metric_name),
                        [],
                    ),
                    provenance_rows=provenance_by_brand.get(canonical_brand_name, []),
                    metric_policy=metric_policy,
                    publish_policy=policy,
                    missing_ai_brands=missing_ai_brands,
                )
            )

    frame = pd.DataFrame(decision_rows)
    if frame.empty:
        frame = pd.DataFrame(
            columns=[
                "brand_name",
                "canonical_brand_name",
                "metric_name",
                "metric_value",
                "publish_status",
                "blocking_reasons",
                "warning_reasons",
                "source_type",
                "source_name",
                "source_url_or_doc_id",
                "as_of_date",
                "method_reported_or_estimated",
                "confidence_score",
                "validation_references",
                "reconciliation_grade",
                "reconciliation_relative_error",
                "reconciliation_absolute_error",
                "reference_evidence_present",
                "reference_source_count",
                "policy_id",
                "policy_version",
            ]
        )
    return frame.sort_values(
        by=["canonical_brand_name", "metric_name"],
        ascending=[True, True],
        kind="stable",
        ignore_index=True,
    )


def build_gold_gate_summary(
    *,
    inputs: GoldGateInputs,
    decisions: pd.DataFrame,
    policy: GoldPublishPolicy,
) -> dict[str, Any]:
    """Build the Gold publishing scorecard summary."""

    _validation_by_brand, _missing_ai_brands, extra_ai_brands, auv_mismatch_brands = (
        _validation_context(inputs.validation_flags)
    )
    advisory_only_metrics = [
        metric_policy.metric_name
        for metric_policy in policy.metric_policies
        if metric_policy.advisory_only
    ]
    return build_gold_publish_summary(
        decisions,
        policy_id=policy.policy_id,
        policy_version=policy.version,
        orphan_ai_brands=extra_ai_brands,
        auv_mismatch_brands=auv_mismatch_brands,
        advisory_only_metrics=advisory_only_metrics,
    )


def write_gold_gate_outputs(
    *,
    decisions: pd.DataFrame,
    summary: dict[str, Any],
    gold_dir: Path,
    report_dir: Path,
    policy: GoldPublishPolicy,
) -> GoldGateArtifacts:
    """Write Gold publish decisions, scorecard markdown, and summary JSON."""

    gold_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    decisions_path = gold_dir / "gold_publish_decisions.parquet"
    publishable_path = gold_dir / "publishable_kpis.parquet"
    blocked_path = gold_dir / "blocked_kpis.parquet"
    scorecard_markdown_path = report_dir / "gold_publish_scorecard.md"
    summary_json_path = report_dir / "gold_publish_scorecard.json"

    parquet_frame = decisions.copy()
    for column in ("blocking_reasons", "warning_reasons", "validation_references"):
        parquet_frame[column] = parquet_frame[column].map(
            lambda value: json.dumps(value, ensure_ascii=False, default=str)
        )

    parquet_frame.to_parquet(decisions_path, index=False)
    parquet_frame.loc[parquet_frame["publish_status"] == "publishable"].to_parquet(
        publishable_path,
        index=False,
    )
    parquet_frame.loc[parquet_frame["publish_status"] == "blocked"].to_parquet(
        blocked_path,
        index=False,
    )

    scorecard_markdown_path.write_text(
        render_gold_publish_scorecard(summary, decisions),
        encoding="utf-8",
    )
    summary_json_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    return GoldGateArtifacts(
        decisions_path=decisions_path,
        publishable_path=publishable_path,
        blocked_path=blocked_path,
        scorecard_markdown_path=scorecard_markdown_path,
        summary_json_path=summary_json_path,
    )


def _evaluate_metric_decision(
    *,
    row: dict[str, Any],
    coverage_row: dict[str, Any],
    brand_findings: list[dict[str, Any]],
    synthetic_warnings: list[str],
    provenance_rows: list[dict[str, Any]],
    metric_policy: MetricGatePolicy,
    publish_policy: GoldPublishPolicy,
    missing_ai_brands: set[str],
) -> dict[str, Any]:
    brand_name = str(row.get("brand_name") or "unknown")
    canonical_brand_name = str(row.get("canonical_brand_name") or brand_name)
    metric_value = row.get(metric_policy.value_column)
    blocking_reasons: list[str] = []
    warning_reasons: list[str] = []
    validation_references: list[dict[str, Any]] = []

    if metric_value is None or pd.isna(metric_value):
        blocking_reasons.append("Metric value is missing in the reconciled Gold row.")

    provenance_row = _select_metric_provenance_row(
        canonical_brand_name=canonical_brand_name,
        provenance_rows=provenance_rows,
        metric_policy=metric_policy,
        reconciled_row=row,
    )
    provenance_payload = _provenance_payload(provenance_row)

    reference_evidence_present = _reference_evidence_present(
        metric_policy=metric_policy, reconciled_row=row
    )
    if metric_policy.require_reference_evidence and not reference_evidence_present:
        blocking_reasons.append("No external reference evidence was available for this metric.")

    missing_provenance_fields = _missing_required_provenance_fields(
        provenance_payload,
        required_fields=publish_policy.required_provenance_fields,
    )
    if metric_policy.require_complete_provenance and missing_provenance_fields:
        blocking_reasons.append(
            "Required provenance fields are missing: " + ", ".join(missing_provenance_fields) + "."
        )

    confidence_score = _to_float(provenance_payload.get("confidence_score"))
    if (
        metric_policy.minimum_confidence_score is not None
        and reference_evidence_present
        and (confidence_score is None or confidence_score < metric_policy.minimum_confidence_score)
    ):
        blocking_reasons.append(
            f"Reference confidence {confidence_score if confidence_score is not None else 'missing'} "
            f"is below policy threshold {metric_policy.minimum_confidence_score:.2f}."
        )

    method_value = _clean_optional_text(provenance_payload.get("method_reported_or_estimated"))
    if (
        metric_policy.publishable_methods
        and reference_evidence_present
        and method_value is not None
        and method_value not in metric_policy.publishable_methods
    ):
        warning_reasons.append(
            f"Metric uses `{method_value}` evidence, so policy keeps it out of publishable status."
        )

    reconciliation_grade = None
    reconciliation_relative_error = None
    reconciliation_absolute_error = None
    if metric_policy.reference_prefix is not None:
        prefix = metric_policy.reference_prefix
        reconciliation_grade = _clean_optional_text(row.get(f"{prefix}_credibility_grade"))
        reconciliation_relative_error = _to_float(row.get(f"{prefix}_relative_error"))
        reconciliation_absolute_error = _to_float(row.get(f"{prefix}_absolute_error"))
        if (
            reference_evidence_present
            and reconciliation_grade is not None
            and metric_policy.allowed_reconciliation_grades
            and reconciliation_grade not in metric_policy.allowed_reconciliation_grades
        ):
            blocking_reasons.append(
                f"Reconciliation grade `{reconciliation_grade}` is below the policy requirement."
            )
        if (
            reference_evidence_present
            and reconciliation_relative_error is not None
            and reconciliation_relative_error
            > publish_policy.reconciliation_relative_error_hard_fail
        ):
            blocking_reasons.append(
                f"Reconciliation relative error {reconciliation_relative_error:.2%} exceeds the "
                f"{publish_policy.reconciliation_relative_error_hard_fail:.0%} policy threshold."
            )

    coverage_missing_metrics = set(coverage_row.get("missing_metrics") or [])
    if (
        metric_policy.reference_prefix is not None
        and metric_policy.metric_name in coverage_missing_metrics
        and metric_policy.require_reference_evidence
    ):
        blocking_reasons.append(
            "Reference coverage audit marked this metric as missing external coverage."
        )

    for finding in brand_findings:
        if not _validation_finding_applies_to_metric(finding, metric_policy.metric_name):
            continue
        validation_references.append(
            {
                "check_name": finding.get("check_name"),
                "severity": finding.get("severity"),
                "message": finding.get("message"),
            }
        )
        if (
            finding.get("severity") == "error"
            and str(finding.get("check_name")) in metric_policy.hard_fail_validation_checks
        ):
            blocking_reasons.append(str(finding.get("message")))
        elif (
            finding.get("severity") in {"warning", "info"}
            and str(finding.get("check_name")) in metric_policy.advisory_validation_checks
        ):
            warning_reasons.append(str(finding.get("message")))

    if canonical_brand_name in missing_ai_brands:
        warning_reasons.append(
            "AI strategy sheet is missing a corresponding row for this core brand."
        )

    warning_reasons.extend(synthetic_warnings)

    if metric_policy.advisory_only:
        warning_reasons.append(metric_policy.description)

    publish_status = "publishable"
    if blocking_reasons:
        publish_status = "blocked"
    elif metric_policy.advisory_only or warning_reasons:
        publish_status = "advisory"

    return {
        "brand_name": brand_name,
        "canonical_brand_name": canonical_brand_name,
        "metric_name": metric_policy.metric_name,
        "metric_value": metric_value,
        "publish_status": publish_status,
        "blocking_reasons": _dedupe_preserve_order(blocking_reasons),
        "warning_reasons": _dedupe_preserve_order(warning_reasons),
        "source_type": provenance_payload.get("source_type"),
        "source_name": provenance_payload.get("source_name"),
        "source_url_or_doc_id": provenance_payload.get("source_url_or_doc_id"),
        "as_of_date": provenance_payload.get("as_of_date"),
        "method_reported_or_estimated": provenance_payload.get("method_reported_or_estimated"),
        "confidence_score": confidence_score,
        "validation_references": validation_references,
        "reconciliation_grade": reconciliation_grade,
        "reconciliation_relative_error": reconciliation_relative_error,
        "reconciliation_absolute_error": reconciliation_absolute_error,
        "reference_evidence_present": reference_evidence_present,
        "reference_source_count": int(row.get("reference_source_count") or 0),
        "policy_id": publish_policy.policy_id,
        "policy_version": publish_policy.version,
    }


def _provenance_by_brand(provenance_frame: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if provenance_frame.empty:
        return grouped
    for row in provenance_frame.to_dict(orient="records"):
        extra = row.get("extra") or {}
        canonical_brand_name = _clean_optional_text(extra.get("canonical_brand_name"))
        if canonical_brand_name is None:
            continue
        grouped[canonical_brand_name].append(row)
    return grouped


def _validation_context(
    validation_flags: pd.DataFrame,
) -> tuple[dict[str, list[dict[str, Any]]], set[str], list[str], list[str]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    missing_ai_brands: set[str] = set()
    extra_ai_brands: set[str] = set()
    auv_mismatch_brands: set[str] = set()

    if validation_flags.empty:
        return grouped, missing_ai_brands, sorted(extra_ai_brands), sorted(auv_mismatch_brands)

    for row in validation_flags.to_dict(orient="records"):
        brand_name = _clean_optional_text(row.get("brand_name"))
        if brand_name is not None:
            grouped[brand_name].append(row)

        check_name = str(row.get("check_name") or "")
        details = row.get("details") or {}
        if check_name == "brand_alignment.missing_ai_brands":
            missing_ai_brands.update(str(value) for value in details.get("missing_ai_brands", []))
        if check_name == "brand_alignment.extra_ai_brands":
            extra_ai_brands.update(str(value) for value in details.get("extra_ai_brands", []))
        if (
            check_name == "implied_auv_k"
            and row.get("severity") == "error"
            and brand_name is not None
        ):
            auv_mismatch_brands.add(brand_name)

    return grouped, missing_ai_brands, sorted(extra_ai_brands), sorted(auv_mismatch_brands)


def _brand_reference_coverage(reference_coverage: pd.DataFrame) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    if reference_coverage.empty:
        return rows
    brand_rows = reference_coverage.loc[reference_coverage["coverage_kind"] == "brand"]
    for row in brand_rows.to_dict(orient="records"):
        canonical_name = _clean_optional_text(row.get("canonical_brand_name"))
        if canonical_name is None:
            continue
        rows[canonical_name] = row
    return rows


def _syntheticness_by_brand_metric(
    syntheticness_signals: pd.DataFrame,
    *,
    warning_strengths: tuple[str, ...],
) -> dict[tuple[str, str], list[str]]:
    grouped: dict[tuple[str, str], list[str]] = defaultdict(list)
    if syntheticness_signals.empty:
        return grouped

    for row in syntheticness_signals.to_dict(orient="records"):
        if str(row.get("strength") or "") not in warning_strengths:
            continue
        details = row.get("details") or {}
        brand_name = _clean_optional_text(details.get("brand_name"))
        metric_name = FIELD_NAME_TO_POLICY_METRIC.get(str(row.get("field_name") or ""))
        if brand_name is None or metric_name is None:
            continue
        grouped[(brand_name, metric_name)].append(
            str(row.get("plain_english") or row.get("title") or "Syntheticness signal present.")
        )
    return grouped


def _select_metric_provenance_row(
    *,
    canonical_brand_name: str,
    provenance_rows: list[dict[str, Any]],
    metric_policy: MetricGatePolicy,
    reconciled_row: dict[str, Any],
) -> dict[str, Any]:
    if metric_policy.reference_prefix is None:
        for candidate in _sorted_provenance_rows(provenance_rows):
            if _clean_optional_text(candidate.get("source_type")) == "workbook":
                return candidate
        if provenance_rows:
            return _sorted_provenance_rows(provenance_rows)[0]
        return {
            "source_type": None,
            "source_name": None,
            "source_url_or_doc_id": None,
            "as_of_date": None,
            "method_reported_or_estimated": None,
            "confidence_score": None,
            "notes": None,
            "extra": {"canonical_brand_name": canonical_brand_name},
        }

    reference_source_name = None
    reference_source_type = None
    reference_source_name = _clean_optional_text(
        reconciled_row.get(f"{metric_policy.reference_prefix}_reference_source_name")
    )
    reference_source_type = _clean_optional_text(
        reconciled_row.get(f"{metric_policy.reference_prefix}_reference_source_type")
    )

    if provenance_rows:
        for candidate in _sorted_provenance_rows(provenance_rows):
            if (
                reference_source_name
                and _clean_optional_text(candidate.get("source_name")) != reference_source_name
            ):
                continue
            if (
                reference_source_type
                and _clean_optional_text(candidate.get("source_type")) != reference_source_type
            ):
                continue
            return candidate

        for candidate in _sorted_provenance_rows(provenance_rows):
            if _clean_optional_text(candidate.get("source_type")) == "workbook":
                return candidate
        return _sorted_provenance_rows(provenance_rows)[0]

    return {
        "source_type": None,
        "source_name": None,
        "source_url_or_doc_id": None,
        "as_of_date": None,
        "method_reported_or_estimated": None,
        "confidence_score": None,
        "notes": None,
        "extra": {"canonical_brand_name": canonical_brand_name},
    }


def _sorted_provenance_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            _to_float(row.get("confidence_score"))
            if _to_float(row.get("confidence_score")) is not None
            else -1.0,
            str(row.get("as_of_date") or ""),
            str(row.get("source_name") or ""),
        ),
        reverse=True,
    )


def _provenance_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    if isinstance(payload.get("as_of_date"), pd.Timestamp):
        payload["as_of_date"] = payload["as_of_date"].date().isoformat()
    elif payload.get("as_of_date") is not None:
        payload["as_of_date"] = str(payload["as_of_date"])
    return payload


def _reference_evidence_present(
    *, metric_policy: MetricGatePolicy, reconciled_row: dict[str, Any]
) -> bool:
    if metric_policy.reference_prefix is None:
        return False
    prefix = metric_policy.reference_prefix
    source_name = _clean_optional_text(reconciled_row.get(f"{prefix}_reference_source_name"))
    reference_value = reconciled_row.get(f"{prefix}_reference_value")
    if reference_value is None or pd.isna(reference_value):
        return False
    return source_name is not None


def _missing_required_provenance_fields(
    provenance_payload: dict[str, Any],
    *,
    required_fields: tuple[str, ...],
) -> list[str]:
    missing: list[str] = []
    for field_name in required_fields:
        value = provenance_payload.get(field_name)
        if value is None:
            missing.append(field_name)
            continue
        if isinstance(value, float) and pd.isna(value):
            missing.append(field_name)
            continue
        if isinstance(value, str) and not value.strip():
            missing.append(field_name)
    return missing


def _brand_findings_for_keys(
    validation_by_brand: dict[str, list[dict[str, Any]]],
    *,
    keys: tuple[str, ...],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    seen_messages: set[str] = set()
    for key in keys:
        for finding in validation_by_brand.get(key, []):
            message = str(finding.get("message") or "")
            if message in seen_messages:
                continue
            seen_messages.add(message)
            findings.append(finding)
    return findings


def _validation_finding_applies_to_metric(finding: dict[str, Any], metric_name: str) -> bool:
    check_name = str(finding.get("check_name") or "")
    if metric_name in CHECK_NAME_TO_POLICY_METRICS.get(check_name, set()):
        return True
    field_metric = FIELD_NAME_TO_POLICY_METRIC.get(str(finding.get("field_name") or ""))
    return field_metric == metric_name


def _parse_json_columns(frame: pd.DataFrame, *, columns: tuple[str, ...]) -> pd.DataFrame:
    if frame.empty:
        return frame
    parsed = frame.copy()
    for column_name in columns:
        if column_name not in parsed.columns:
            continue
        parsed[column_name] = parsed[column_name].map(_parse_json_value)
    return parsed


def _parse_json_value(value: object) -> object:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, dict | list):
        return value
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return value


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        deduped.append(text)
    return deduped


def _clean_optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def _to_float(value: object) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(result):
        return None
    return result


__all__ = [
    "GoldGateArtifacts",
    "GoldGateInputs",
    "GoldGateRun",
    "build_gold_gate_summary",
    "build_gold_publish_decisions",
    "gate_gold_publish",
    "load_gold_gate_inputs",
    "write_gold_gate_outputs",
]
