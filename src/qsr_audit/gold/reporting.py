"""Markdown and JSON rendering for Gold publishing gate outputs."""

from __future__ import annotations

import json
from collections import Counter
from typing import Any

import pandas as pd


def render_gold_publish_scorecard(
    summary: dict[str, Any],
    decisions: pd.DataFrame,
) -> str:
    """Render the Gold publishing scorecard as Markdown."""

    lines = [
        "# Gold Publish Scorecard",
        "",
        f"- Policy: `{summary['policy_id']}`",
        f"- Version: `{summary['policy_version']}`",
        f"- Total KPI rows evaluated: `{summary['total_kpi_rows']}`",
        f"- Publishable: `{summary['publishable_count']}`",
        f"- Advisory: `{summary['advisory_count']}`",
        f"- Blocked: `{summary['blocked_count']}`",
        "",
        "## Block Reasons By Frequency",
        "",
    ]

    block_reasons = summary.get("block_reasons", [])
    if block_reasons:
        for row in block_reasons:
            lines.append(f"- `{row['reason']}`: {row['count']}")
    else:
        lines.append("- No blocking reasons were emitted.")

    lines.extend(["", "## Warning Reasons By Frequency", ""])
    warning_reasons = summary.get("warning_reasons", [])
    if warning_reasons:
        for row in warning_reasons:
            lines.append(f"- `{row['reason']}`: {row['count']}")
    else:
        lines.append("- No warning reasons were emitted.")

    lines.extend(
        [
            "",
            "## Brand-Level Readiness Summary",
            "",
            "| Brand | Ready | Caution | Hold | Publishable | Advisory | Blocked |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in summary.get("brand_readiness", []):
        lines.append(
            "| "
            + f"{row['canonical_brand_name']} | "
            + f"{1 if row['readiness'] == 'ready' else 0} | "
            + f"{1 if row['readiness'] == 'caution' else 0} | "
            + f"{1 if row['readiness'] == 'hold' else 0} | "
            + f"{row['publishable']} | "
            + f"{row['advisory']} | "
            + f"{row['blocked']} |"
        )

    lines.extend(
        [
            "",
            "## Metric-Level Readiness Summary",
            "",
            "| Metric | Publishable | Advisory | Blocked |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for row in summary.get("metric_readiness", []):
        lines.append(
            "| "
            + f"{row['metric_name']} | "
            + f"{row['publishable']} | "
            + f"{row['advisory']} | "
            + f"{row['blocked']} |"
        )

    lines.extend(["", "## Metrics With No External Evidence", ""])
    no_evidence = summary.get("metrics_without_external_evidence", [])
    if no_evidence:
        for row in no_evidence:
            brands = ", ".join(row["brands"]) if row["brands"] else "none"
            lines.append(f"- `{row['metric_name']}`: {brands}")
    else:
        lines.append("- Every evaluated publishable metric had at least one external evidence row.")

    lines.extend(["", "## Workbook-Specific Highlights", ""])
    highlights = summary.get("workbook_highlights", {})
    orphan_ai_brands = highlights.get("orphan_ai_brands", [])
    if orphan_ai_brands:
        lines.append(
            "- Orphan AI rows not present in the core workbook: " + ", ".join(orphan_ai_brands)
        )
    else:
        lines.append("- No orphan AI rows were present.")

    auv_mismatch_brands = highlights.get("auv_mismatch_brands", [])
    if auv_mismatch_brands:
        lines.append("- AUV mismatch brands: " + ", ".join(auv_mismatch_brands))
    else:
        lines.append("- No AUV mismatch brands were present.")

    missing_provenance_clusters = highlights.get("missing_provenance_clusters", [])
    if missing_provenance_clusters:
        for row in missing_provenance_clusters:
            lines.append(f"- Missing provenance cluster `{row['metric_name']}`: {row['count']}")
    else:
        lines.append("- No missing provenance clusters were detected.")

    advisory_only_metrics = summary.get("advisory_only_metrics", [])
    if advisory_only_metrics:
        lines.append(
            "- Advisory-only metrics under the current policy: " + ", ".join(advisory_only_metrics)
        )

    lines.extend(["", "## Sample Decision Rows", ""])
    sample_rows = decisions.head(10)
    if sample_rows.empty:
        lines.append("- No decision rows were generated.")
    else:
        lines.extend(
            [
                "| Brand | Metric | Status | Blocking reasons | Warning reasons |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for row in sample_rows.to_dict(orient="records"):
            block_text = ", ".join(row.get("blocking_reasons") or []) or "-"
            warning_text = ", ".join(row.get("warning_reasons") or []) or "-"
            lines.append(
                "| "
                + f"{row['canonical_brand_name']} | "
                + f"{row['metric_name']} | "
                + f"{row['publish_status']} | "
                + f"{_escape_markdown(block_text)} | "
                + f"{_escape_markdown(warning_text)} |"
            )

    return "\n".join(lines) + "\n"


def build_gold_publish_summary(
    decisions: pd.DataFrame,
    *,
    policy_id: str,
    policy_version: str,
    orphan_ai_brands: list[str],
    auv_mismatch_brands: list[str],
    advisory_only_metrics: list[str],
) -> dict[str, Any]:
    """Build a machine-readable summary for Gold publishing decisions."""

    status_counts = (
        decisions["publish_status"].value_counts().to_dict() if not decisions.empty else {}
    )
    block_counter = _reason_counter(decisions, "blocking_reasons")
    warning_counter = _reason_counter(decisions, "warning_reasons")

    brand_readiness = []
    if not decisions.empty:
        for brand_name, frame in decisions.groupby("canonical_brand_name", dropna=False):
            counts = frame["publish_status"].value_counts().to_dict()
            readiness = "ready"
            if counts.get("blocked", 0):
                readiness = "hold"
            elif counts.get("advisory", 0):
                readiness = "caution"
            brand_readiness.append(
                {
                    "canonical_brand_name": str(brand_name),
                    "readiness": readiness,
                    "publishable": int(counts.get("publishable", 0)),
                    "advisory": int(counts.get("advisory", 0)),
                    "blocked": int(counts.get("blocked", 0)),
                }
            )
    brand_readiness.sort(key=lambda row: row["canonical_brand_name"])

    metric_readiness = []
    if not decisions.empty:
        for metric_name, frame in decisions.groupby("metric_name", dropna=False):
            counts = frame["publish_status"].value_counts().to_dict()
            metric_readiness.append(
                {
                    "metric_name": str(metric_name),
                    "publishable": int(counts.get("publishable", 0)),
                    "advisory": int(counts.get("advisory", 0)),
                    "blocked": int(counts.get("blocked", 0)),
                }
            )
    metric_readiness.sort(key=lambda row: row["metric_name"])

    metrics_without_external_evidence = []
    if not decisions.empty:
        missing_evidence = decisions[
            decisions["blocking_reasons"].map(
                lambda reasons: (
                    "No external reference evidence was available for this metric."
                    in (reasons or [])
                )
            )
        ]
        for metric_name, frame in missing_evidence.groupby("metric_name", dropna=False):
            metrics_without_external_evidence.append(
                {
                    "metric_name": str(metric_name),
                    "brands": sorted(
                        str(value) for value in frame["canonical_brand_name"].dropna().unique()
                    ),
                }
            )
    metrics_without_external_evidence.sort(key=lambda row: row["metric_name"])

    missing_provenance_clusters = []
    if not decisions.empty:
        provenance_blocks = decisions[
            decisions["blocking_reasons"].map(
                lambda reasons: any(
                    "Required provenance fields are missing" in reason for reason in (reasons or [])
                )
            )
        ]
        for metric_name, frame in provenance_blocks.groupby("metric_name", dropna=False):
            missing_provenance_clusters.append(
                {"metric_name": str(metric_name), "count": int(len(frame.index))}
            )
    missing_provenance_clusters.sort(key=lambda row: (-row["count"], row["metric_name"]))

    return {
        "policy_id": policy_id,
        "policy_version": policy_version,
        "total_kpi_rows": int(len(decisions.index)),
        "publishable_count": int(status_counts.get("publishable", 0)),
        "advisory_count": int(status_counts.get("advisory", 0)),
        "blocked_count": int(status_counts.get("blocked", 0)),
        "block_reasons": _sorted_counter_rows(block_counter),
        "warning_reasons": _sorted_counter_rows(warning_counter),
        "brand_readiness": brand_readiness,
        "metric_readiness": metric_readiness,
        "metrics_without_external_evidence": metrics_without_external_evidence,
        "workbook_highlights": {
            "orphan_ai_brands": sorted(orphan_ai_brands),
            "auv_mismatch_brands": sorted(auv_mismatch_brands),
            "missing_provenance_clusters": missing_provenance_clusters,
        },
        "advisory_only_metrics": sorted(advisory_only_metrics),
    }


def _reason_counter(frame: pd.DataFrame, column_name: str) -> Counter[str]:
    counter: Counter[str] = Counter()
    if frame.empty or column_name not in frame.columns:
        return counter
    for reasons in frame[column_name].tolist():
        for reason in reasons or []:
            counter[str(reason)] += 1
    return counter


def _sorted_counter_rows(counter: Counter[str]) -> list[dict[str, Any]]:
    return [
        {"reason": reason, "count": count}
        for reason, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    ]


def write_gold_summary_json(summary: dict[str, Any], path: str) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


def _escape_markdown(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "build_gold_publish_summary",
    "render_gold_publish_scorecard",
    "write_gold_summary_json",
]
