"""Release preflight checks for external-facing QSR audit handoff."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.config import Settings
from qsr_audit.governance import (
    format_artifact_path,
    latest_manifest_path,
    load_artifact_manifest,
)

REQUIRED_DOCS = (
    "docs/analyst-runbook.md",
    "docs/cli.md",
    "docs/security-privacy-controls.md",
    "docs/release-runbook.md",
)
REQUIRED_UPSTREAM_COMMANDS = (
    "validate-workbook",
    "run-syntheticness",
    "reconcile",
    "gate-gold",
)


@dataclass(frozen=True)
class ReleasePreflightArtifacts:
    """Written artifacts for release preflight."""

    summary_json_path: Path
    summary_markdown_path: Path


@dataclass(frozen=True)
class ReleasePreflightCheck:
    """One preflight check outcome."""

    name: str
    status: str
    message: str
    details: dict[str, Any]


@dataclass(frozen=True)
class ReleasePreflightRun:
    """Full release preflight result."""

    passed: bool
    checks: tuple[ReleasePreflightCheck, ...]
    summary: dict[str, Any]
    artifacts: ReleasePreflightArtifacts


def preflight_release(
    settings: Settings | None = None,
    *,
    output_root: Path | None = None,
) -> ReleasePreflightRun:
    """Validate whether Gold outputs are ready for external-facing handoff."""

    resolved_settings = settings or Settings()
    release_root = resolved_settings.validate_artifact_root(
        output_root or (resolved_settings.artifacts_dir / "release"),
        purpose="release preflight artifacts",
    )
    release_root.mkdir(parents=True, exist_ok=True)

    checks: list[ReleasePreflightCheck] = []
    checks.append(_check_required_docs())

    gold_paths = {
        "gold_publish_decisions": resolved_settings.data_gold / "gold_publish_decisions.parquet",
        "publishable_kpis": resolved_settings.data_gold / "publishable_kpis.parquet",
        "blocked_kpis": resolved_settings.data_gold / "blocked_kpis.parquet",
    }
    checks.append(_check_required_gold_artifacts(gold_paths))

    decisions = publishable = blocked = None
    if all(path.exists() for path in gold_paths.values()):
        decisions = pd.read_parquet(gold_paths["gold_publish_decisions"])
        publishable = pd.read_parquet(gold_paths["publishable_kpis"])
        blocked = pd.read_parquet(gold_paths["blocked_kpis"])
        checks.append(_check_gold_consistency(decisions, publishable, blocked))

    manifest_check, manifests = _check_upstream_manifests(resolved_settings)
    checks.append(manifest_check)
    checks.append(_check_gate_manifest_references(manifests))

    checks.append(_check_experimental_separation(resolved_settings))

    summary = _build_summary(checks, decisions)
    artifacts = _write_preflight_outputs(summary, release_root=release_root)
    return ReleasePreflightRun(
        passed=summary["passed"],
        checks=tuple(checks),
        summary=summary,
        artifacts=artifacts,
    )


def render_preflight_summary(summary: dict[str, Any]) -> str:
    """Render a concise Markdown summary for release preflight."""

    lines = [
        "# Release Preflight Summary",
        "",
        f"- Status: `{'PASS' if summary['passed'] else 'FAIL'}`",
        f"- Failed checks: `{summary['failed_check_count']}`",
        f"- Warning checks: `{summary['warning_check_count']}`",
        f"- Passed checks: `{summary['passed_check_count']}`",
    ]

    decision_counts = summary.get("decision_counts")
    if decision_counts:
        lines.extend(
            [
                "",
                "## Gold Decision Counts",
                "",
                f"- Publishable: `{decision_counts.get('publishable', 0)}`",
                f"- Advisory: `{decision_counts.get('advisory', 0)}`",
                f"- Blocked: `{decision_counts.get('blocked', 0)}`",
            ]
        )

    lines.extend(["", "## Checks", ""])
    for check in summary["checks"]:
        lines.append(f"- `{check['status']}` `{check['name']}`: {check['message']}")

    experimental_notes = summary.get("experimental_artifact_notes") or []
    if experimental_notes:
        lines.extend(["", "## Experimental Artifact Notes", ""])
        for note in experimental_notes:
            lines.append(f"- {note}")

    return "\n".join(lines) + "\n"


def _check_required_docs() -> ReleasePreflightCheck:
    missing = [path for path in REQUIRED_DOCS if not Path(path).exists()]
    if missing:
        return ReleasePreflightCheck(
            name="required_docs",
            status="fail",
            message="Required runbook and control docs are missing.",
            details={"missing": missing},
        )
    return ReleasePreflightCheck(
        name="required_docs",
        status="pass",
        message="Required release and analyst docs are present.",
        details={"paths": list(REQUIRED_DOCS)},
    )


def _check_required_gold_artifacts(gold_paths: dict[str, Path]) -> ReleasePreflightCheck:
    missing = [str(path) for path in gold_paths.values() if not path.exists()]
    if missing:
        return ReleasePreflightCheck(
            name="required_gold_artifacts",
            status="fail",
            message="Required Gold publish artifacts are missing.",
            details={"missing": missing},
        )
    return ReleasePreflightCheck(
        name="required_gold_artifacts",
        status="pass",
        message="Gold publish decisions and publishable/blocked subsets are present.",
        details={name: str(path) for name, path in gold_paths.items()},
    )


def _check_gold_consistency(
    decisions: pd.DataFrame,
    publishable: pd.DataFrame,
    blocked: pd.DataFrame,
) -> ReleasePreflightCheck:
    issues: list[str] = []
    allowed_statuses = {"publishable", "advisory", "blocked"}
    decision_statuses = set(
        decisions.get("publish_status", pd.Series(dtype=str)).dropna().astype(str)
    )
    if not decision_statuses.issubset(allowed_statuses):
        issues.append("Gold publish decisions contain unexpected publish_status values.")

    publishable_statuses = set(
        publishable.get("publish_status", pd.Series(dtype=str)).dropna().astype(str)
    )
    if publishable_statuses - {"publishable"}:
        issues.append("publishable_kpis.parquet contains advisory or blocked rows.")

    blocked_statuses = set(blocked.get("publish_status", pd.Series(dtype=str)).dropna().astype(str))
    if blocked_statuses - {"blocked"}:
        issues.append("blocked_kpis.parquet contains publishable or advisory rows.")

    expected_publishable = decisions.loc[decisions["publish_status"] == "publishable"].copy()
    expected_blocked = decisions.loc[decisions["publish_status"] == "blocked"].copy()
    duplicate_publishable = _duplicate_decision_keys(publishable)
    if duplicate_publishable:
        issues.append("publishable_kpis.parquet contains duplicated KPI rows.")
    duplicate_blocked = _duplicate_decision_keys(blocked)
    if duplicate_blocked:
        issues.append("blocked_kpis.parquet contains duplicated KPI rows.")

    if _decision_key_counts(expected_publishable) != _decision_key_counts(publishable):
        issues.append("publishable_kpis.parquet does not match the publishable decision subset.")
    if _decision_key_counts(expected_blocked) != _decision_key_counts(blocked):
        issues.append("blocked_kpis.parquet does not match the blocked decision subset.")

    if issues:
        details: dict[str, Any] = {"issues": issues}
        duplicate_details: dict[str, list[dict[str, Any]]] = {}
        if duplicate_publishable:
            duplicate_details["publishable_kpis"] = duplicate_publishable
        if duplicate_blocked:
            duplicate_details["blocked_kpis"] = duplicate_blocked
        if duplicate_details:
            details["duplicate_rows"] = duplicate_details
        return ReleasePreflightCheck(
            name="gold_artifact_consistency",
            status="fail",
            message="Gold publish artifacts are internally inconsistent.",
            details=details,
        )

    message = (
        "Gold publish decisions are internally consistent and publishable rows stay separated "
        "from blocked/advisory rows."
    )
    if expected_publishable.empty:
        return ReleasePreflightCheck(
            name="gold_artifact_consistency",
            status="warning",
            message=f"{message} There are currently no publishable KPI rows.",
            details={
                "publishable_count": 0,
                "advisory_count": int((decisions["publish_status"] == "advisory").sum()),
                "blocked_count": int((decisions["publish_status"] == "blocked").sum()),
            },
        )

    return ReleasePreflightCheck(
        name="gold_artifact_consistency",
        status="pass",
        message=message,
        details={
            "publishable_count": int((decisions["publish_status"] == "publishable").sum()),
            "advisory_count": int((decisions["publish_status"] == "advisory").sum()),
            "blocked_count": int((decisions["publish_status"] == "blocked").sum()),
        },
    )


def _check_upstream_manifests(
    settings: Settings,
) -> tuple[ReleasePreflightCheck, dict[str, Any]]:
    manifests: dict[str, Any] = {}
    missing: list[str] = []
    for command_name in REQUIRED_UPSTREAM_COMMANDS:
        manifest_path = latest_manifest_path(settings, command_name)
        if not manifest_path.exists():
            missing.append(str(manifest_path))
            continue
        manifests[command_name] = {
            "path": manifest_path,
            "manifest": load_artifact_manifest(manifest_path),
        }

    if missing:
        return (
            ReleasePreflightCheck(
                name="upstream_manifests",
                status="fail",
                message="Required upstream manifests are missing.",
                details={"missing": missing},
            ),
            manifests,
        )

    return (
        ReleasePreflightCheck(
            name="upstream_manifests",
            status="pass",
            message="Required upstream manifests are present.",
            details={
                command_name: str(payload["path"]) for command_name, payload in manifests.items()
            },
        ),
        manifests,
    )


def _check_gate_manifest_references(manifests: dict[str, Any]) -> ReleasePreflightCheck:
    required_commands = {"gate-gold", "validate-workbook", "run-syntheticness", "reconcile"}
    missing_commands = sorted(required_commands - set(manifests))
    if missing_commands:
        return ReleasePreflightCheck(
            name="gate_manifest_references",
            status="warning",
            message=(
                "Gate manifest lineage references were not checked because required manifests "
                "are missing."
            ),
            details={"skipped": True, "missing_manifest_commands": missing_commands},
        )

    gate_manifest = manifests["gate-gold"]["manifest"]
    referenced = set(gate_manifest.upstream_artifact_references)
    required = {
        format_artifact_path(manifests["validate-workbook"]["path"]),
        format_artifact_path(manifests["run-syntheticness"]["path"]),
        format_artifact_path(manifests["reconcile"]["path"]),
    }
    missing = sorted(required - referenced)
    if missing:
        return ReleasePreflightCheck(
            name="gate_manifest_references",
            status="fail",
            message="Gate manifest is missing upstream lineage references.",
            details={"missing_references": missing},
        )
    return ReleasePreflightCheck(
        name="gate_manifest_references",
        status="pass",
        message="Gate manifest references the required upstream lineage manifests.",
        details={"required_references": sorted(required)},
    )


def _check_experimental_separation(settings: Settings) -> ReleasePreflightCheck:
    forbidden_roots = (
        settings.reports_dir / "forecasting",
        settings.reports_dir / "rag",
        settings.strategy_dir / "forecasting",
        settings.strategy_dir / "rag",
    )
    forbidden_paths = [path for path in forbidden_roots if path.exists()]
    notes: list[str] = []
    for artifact_dir in (settings.artifacts_dir / "forecasting", settings.artifacts_dir / "rag"):
        if artifact_dir.exists():
            notes.append(
                f"Experimental artifacts are present under `{artifact_dir}` and remain non-analyst-facing."
            )

    if forbidden_paths:
        return ReleasePreflightCheck(
            name="experimental_artifact_separation",
            status="fail",
            message="Experimental forecasting or RAG artifacts leaked into analyst-facing paths.",
            details={"forbidden_paths": [str(path) for path in forbidden_paths], "notes": notes},
        )
    return ReleasePreflightCheck(
        name="experimental_artifact_separation",
        status="pass",
        message="Experimental forecasting and RAG artifacts remain separated from audited release paths.",
        details={"notes": notes},
    )


def _build_summary(
    checks: list[ReleasePreflightCheck],
    decisions: pd.DataFrame | None,
) -> dict[str, Any]:
    failed = [check for check in checks if check.status == "fail"]
    warnings = [check for check in checks if check.status == "warning"]
    passed = [check for check in checks if check.status == "pass"]
    summary: dict[str, Any] = {
        "passed": not failed,
        "failed_check_count": len(failed),
        "warning_check_count": len(warnings),
        "passed_check_count": len(passed),
        "checks": [
            {
                "name": check.name,
                "status": check.status,
                "message": check.message,
                "details": check.details,
            }
            for check in checks
        ],
    }
    if decisions is not None and not decisions.empty:
        counts = decisions["publish_status"].value_counts(dropna=False).to_dict()
        summary["decision_counts"] = {key: int(value) for key, value in counts.items()}

    experimental_notes: list[str] = []
    for check in checks:
        if check.name == "experimental_artifact_separation":
            experimental_notes.extend(check.details.get("notes", []))
    if experimental_notes:
        summary["experimental_artifact_notes"] = experimental_notes
    return summary


def _write_preflight_outputs(
    summary: dict[str, Any],
    *,
    release_root: Path,
) -> ReleasePreflightArtifacts:
    json_path = release_root / "preflight_summary.json"
    markdown_path = release_root / "preflight_summary.md"
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_preflight_summary(summary), encoding="utf-8")
    return ReleasePreflightArtifacts(
        summary_json_path=json_path,
        summary_markdown_path=markdown_path,
    )


def _decision_key_counts(frame: pd.DataFrame) -> Counter[tuple[str, str, str]]:
    counts: Counter[tuple[str, str, str]] = Counter()
    if frame.empty:
        return counts
    for row in frame.to_dict(orient="records"):
        key = (
            str(row.get("canonical_brand_name") or row.get("brand_name") or ""),
            str(row.get("metric_name") or ""),
            str(row.get("publish_status") or ""),
        )
        counts[key] += 1
    return counts


def _duplicate_decision_keys(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "brand_name": brand_name,
            "metric_name": metric_name,
            "publish_status": publish_status,
            "count": count,
        }
        for (brand_name, metric_name, publish_status), count in sorted(
            _decision_key_counts(frame).items()
        )
        if count > 1
    ]


__all__ = [
    "REQUIRED_DOCS",
    "REQUIRED_UPSTREAM_COMMANDS",
    "ReleasePreflightArtifacts",
    "ReleasePreflightCheck",
    "ReleasePreflightRun",
    "preflight_release",
    "render_preflight_summary",
]
