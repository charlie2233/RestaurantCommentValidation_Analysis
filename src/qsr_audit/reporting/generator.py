"""Analyst-facing report generation built from local scorecard artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, Template, select_autoescape

from qsr_audit.config import Settings
from qsr_audit.reporting.scorecards import (
    BrandScorecard,
    ReportBundle,
    build_report_bundle,
    load_report_inputs,
    slugify_brand_name,
)

DEFAULT_TEMPLATE_NAME = "report.html.j2"


@dataclass(frozen=True)
class ReportArtifacts:
    """Paths written by the analyst-facing report generator."""

    global_markdown: Path
    global_html: Path
    global_json: Path
    brand_markdown_paths: dict[str, Path]
    brand_html_paths: dict[str, Path]
    brand_json_paths: dict[str, Path]


def write_reports(output_root: Path, settings: Settings | None = None) -> ReportArtifacts:
    """Write Markdown, HTML, and JSON reports from existing Gold-layer artifacts."""

    bundle = build_report_bundle(load_report_inputs(settings))
    report_root = output_root.expanduser().resolve()
    brand_root = report_root / "brands"
    report_root.mkdir(parents=True, exist_ok=True)
    brand_root.mkdir(parents=True, exist_ok=True)

    global_markdown = report_root / "index.md"
    global_html = report_root / "index.html"
    global_json = report_root / "index.json"

    global_markdown.write_text(render_global_markdown(bundle), encoding="utf-8")
    global_html.write_text(render_html(bundle), encoding="utf-8")
    global_json.write_text(render_json(bundle), encoding="utf-8")

    brand_markdown_paths: dict[str, Path] = {}
    brand_html_paths: dict[str, Path] = {}
    brand_json_paths: dict[str, Path] = {}
    for scorecard in bundle.brand_scorecards:
        slug = slugify_brand_name(scorecard.canonical_brand_name or scorecard.brand_name)
        markdown_path = brand_root / f"{slug}.md"
        html_path = brand_root / f"{slug}.html"
        json_path = brand_root / f"{slug}.json"

        markdown_path.write_text(
            render_brand_markdown(scorecard, generated_at=bundle.generated_at),
            encoding="utf-8",
        )
        html_path.write_text(
            render_brand_html(scorecard, generated_at=bundle.generated_at),
            encoding="utf-8",
        )
        json_path.write_text(
            json.dumps(
                {
                    "generated_at": bundle.generated_at,
                    "brand_scorecard": scorecard.to_dict(),
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


def render_global_markdown(bundle: ReportBundle) -> str:
    """Render the global scorecard as Markdown."""

    scorecard = bundle.global_scorecard
    lines = [
        "# Global Credibility Scorecard",
        "",
        f"- Generated at: `{bundle.generated_at}`",
        f"- Total brands: `{scorecard.total_brands}`",
        f"- Clean brands: `{scorecard.validation_clean_brands}`",
        f"- Warning brands: `{scorecard.validation_warning_brands}`",
        f"- Failed brands: `{scorecard.validation_failed_brands}`",
        "",
        "## Validation Snapshot",
        "",
        f"- Validation result: `{'PASS' if scorecard.validation_passed else 'FAIL'}`",
        f"- Errors: `{scorecard.validation_counts.get('error', 0)}`",
        f"- Warnings: `{scorecard.validation_counts.get('warning', 0)}`",
        f"- Info: `{scorecard.validation_counts.get('info', 0)}`",
        "",
        "## Warning Counts By Category",
        "",
    ]
    if scorecard.warning_counts_by_category:
        for category, count in sorted(scorecard.warning_counts_by_category.items()):
            lines.append(f"- `{category}`: {int(count)}")
    else:
        lines.append("- No warning categories were recorded.")

    lines.extend(["", "## Fields With Weakest Provenance", ""])
    if scorecard.weakest_provenance_fields:
        for row in scorecard.weakest_provenance_fields:
            lines.append(
                f"- `{row['field_name']}`: weak/missing `{row['weak_or_missing_count']}`, "
                f"covered brands `{row['covered_brand_count']}`, "
                f"average confidence `{row['average_reference_confidence']}`"
            )
    else:
        lines.append("- No provenance summary was available.")

    lines.extend(["", "## Biggest Reconciliation Errors", ""])
    if scorecard.biggest_reconciliation_errors:
        for row in scorecard.biggest_reconciliation_errors:
            lines.append(
                f"- `{row['brand_name']}` / `{row['field_name']}`: "
                f"absolute `{row['absolute_error']}`, "
                f"relative `{row['relative_error']}`, "
                f"grade `{row['credibility_grade']}`"
            )
    else:
        lines.append("- No reconciliation comparisons were available.")

    lines.extend(["", "## Syntheticness Overview", ""])
    synth = scorecard.syntheticness_overview
    lines.append(f"- Total signals: `{synth.get('total_signals', 0)}`")
    if "brands_requiring_review" in synth:
        lines.append(f"- Brands requiring review: `{synth.get('brands_requiring_review', 0)}`")
    if "average_brand_score" in synth:
        lines.append(f"- Average brand score: `{synth.get('average_brand_score', 0.0)}`")
    by_strength = synth.get("by_strength", {})
    if by_strength:
        for strength, count in sorted(by_strength.items()):
            lines.append(f"- `{strength}`: {int(count)}")
    else:
        lines.append("- No syntheticness signals were available.")
    top_types = synth.get("top_types", {})
    if top_types:
        lines.append("- Top signal types:")
        for signal_type, count in top_types.items():
            lines.append(f"  - `{signal_type}`: {int(count)}")

    lines.extend(["", "## Artifact Warnings", ""])
    if bundle.warnings:
        for warning in bundle.warnings:
            lines.append(f"- {warning}")
    else:
        lines.append("- No artifact-loading warnings were recorded.")

    lines.extend(["", "## Source Artifact Status", ""])
    for name, present in sorted(bundle.source_artifact_status.items()):
        lines.append(f"- `{name}`: `{'present' if present else 'missing'}`")

    return "\n".join(lines) + "\n"


def render_brand_markdown(scorecard: BrandScorecard, *, generated_at: str) -> str:
    """Render a single brand scorecard as Markdown."""

    lines = [
        f"# Brand Scorecard: {scorecard.canonical_brand_name}",
        "",
        f"- Generated at: `{generated_at}`",
        f"- Validation status: `{scorecard.validation_status}`",
        f"- Overall credibility grade: `{scorecard.overall_credibility_grade}`",
        "",
        "## Normalized Metrics",
        "",
        _dict_to_markdown_table(scorecard.normalized_metrics),
        "",
        "## Invariant Results",
        "",
        _rows_to_markdown_table(
            scorecard.invariant_results,
            columns=["name", "status", "message"],
        ),
        "",
        "## Provenance Grades",
        "",
        _dict_to_markdown_table(scorecard.provenance_grades),
        "",
        "## Reconciliation Summary",
        "",
        _rows_to_markdown_table(
            scorecard.reconciliation_summary,
            columns=[
                "field_name",
                "reference_value",
                "absolute_error",
                "relative_error",
                "credibility_grade",
                "reference_source_name",
            ],
        ),
        "",
        "## Syntheticness Summary",
        "",
        f"- Syntheticness score: `{scorecard.syntheticness_score}` / 100",
        f"- Review required: `{'yes' if scorecard.review_required else 'no'}`",
        "",
        "### Supporting Signals",
        "",
    ]
    if scorecard.supporting_signals:
        lines.append(
            _rows_to_markdown_table(
                scorecard.supporting_signals,
                columns=["title", "strength", "field_name", "plain_english", "score_contribution"],
            )
        )
    else:
        lines.append("_No supporting signals were available._")

    lines.extend(
        [
            "",
            "### Caveats",
            "",
        ]
    )
    if scorecard.caveats:
        for caveat in scorecard.caveats:
            lines.append(f"- {caveat}")
    else:
        lines.append("- No syntheticness caveats were recorded.")

    lines.extend(
        [
            "",
            "## Syntheticness Signals",
            "",
            _rows_to_markdown_table(
                scorecard.syntheticness_signals,
                columns=["title", "strength", "field_name", "plain_english"],
            ),
            "",
            "## Open Issues",
            "",
        ]
    )
    if scorecard.open_issues:
        for issue in scorecard.open_issues:
            lines.append(f"- {issue}")
    else:
        lines.append("- No open issues were recorded.")
    return "\n".join(lines) + "\n"


def render_html(bundle: ReportBundle) -> str:
    """Render the global report as HTML."""

    template = _load_template()
    return template.render(
        mode="global",
        title="QSR Workbook Credibility Scorecard",
        generated_at=bundle.generated_at,
        warnings=list(bundle.warnings),
        artifact_status=bundle.source_artifact_status,
        global_scorecard=bundle.global_scorecard.to_dict(),
        brand_scorecards=[scorecard.to_dict() for scorecard in bundle.brand_scorecards],
    )


def render_brand_html(scorecard: BrandScorecard, *, generated_at: str) -> str:
    """Render a single brand report as HTML."""

    template = _load_template()
    return template.render(
        mode="brand",
        title=f"Brand Scorecard: {scorecard.canonical_brand_name}",
        generated_at=generated_at,
        warnings=[],
        artifact_status={},
        brand_scorecard=scorecard.to_dict(),
    )


def render_json(bundle: ReportBundle) -> str:
    """Render the full report bundle as machine-readable JSON."""

    return json.dumps(bundle.to_dict(), ensure_ascii=False, indent=2)


def _load_template() -> Template:
    template_dir = Path(__file__).with_name("templates")
    environment = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(["html", "xml", "j2"]),
    )
    return environment.get_template(DEFAULT_TEMPLATE_NAME)


def _dict_to_markdown_table(data: dict[str, Any]) -> str:
    if not data:
        return "_No data available._"
    lines = ["| Field | Value |", "|---|---|"]
    for key, value in data.items():
        lines.append(f"| {key} | {_markdown_cell(value)} |")
    return "\n".join(lines)


def _rows_to_markdown_table(rows: list[dict[str, Any]], *, columns: list[str]) -> str:
    if not rows:
        return "_No rows available._"
    header = "| " + " | ".join(columns) + " |"
    divider = "|" + "|".join("---" for _ in columns) + "|"
    lines = [header, divider]
    for row in rows:
        values = [_markdown_cell(row.get(column)) for column in columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def _markdown_cell(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    return text.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "ReportArtifacts",
    "render_brand_html",
    "render_brand_markdown",
    "render_global_markdown",
    "render_html",
    "render_json",
    "write_reports",
]
