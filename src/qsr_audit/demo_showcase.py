"""Static demo hub rendering and packaging helpers for the happy-path demo."""

from __future__ import annotations

import json
import shutil
from collections.abc import Sequence
from dataclasses import dataclass
from html import escape
from pathlib import Path

import pandas as pd

from qsr_audit.config import Settings
from qsr_audit.reporting.scorecards import ReportBundle


@dataclass(frozen=True)
class DemoBundleArtifacts:
    """Files written by the demo packaging command."""

    bundle_root: Path
    manifest_json_path: Path
    copied_paths: tuple[Path, ...]


def write_demo_hub_html(
    *,
    output_path: Path,
    report_bundle: ReportBundle,
    demo_gold: pd.DataFrame,
    brand_deltas: pd.DataFrame,
    demo_brands: Sequence[str],
) -> Path:
    """Write the static HTML demo hub to disk."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_demo_hub_html(
            report_bundle=report_bundle,
            demo_gold=demo_gold,
            brand_deltas=brand_deltas,
            demo_brands=demo_brands,
        ),
        encoding="utf-8",
    )
    return output_path


def render_demo_hub_html(
    *,
    report_bundle: ReportBundle,
    demo_gold: pd.DataFrame,
    brand_deltas: pd.DataFrame,
    demo_brands: Sequence[str],
) -> str:
    """Render the screenshot-friendly 5-brand demo hub."""

    scorecard = report_bundle.global_scorecard
    publishability_rows = _publishability_rows(report_bundle=report_bundle, demo_gold=demo_gold)
    provenance_rows = _provenance_rows(report_bundle=report_bundle, brand_deltas=brand_deltas)
    invariant_rows = _invariant_failures(report_bundle)
    syntheticness_rows = _syntheticness_rows(report_bundle)
    publish_counts = demo_gold["publish_status"].value_counts().to_dict()
    publishable_rows = int(publish_counts.get("publishable", 0))
    advisory_rows = int(publish_counts.get("advisory", 0))
    blocked_rows = int(publish_counts.get("blocked", 0))
    review_required_brands = sum(1 for row in syntheticness_rows if row["review_required"])
    included_brand_chips = "".join(
        f'<span class="chip">{escape(brand)}</span>' for brand in demo_brands
    )
    artifact_cards = "".join(
        [
            _artifact_card(
                "Core scorecard",
                "../validation/core_scorecard.html",
                "Brand-level validation and publishability detail.",
            ),
            _artifact_card(
                "Brand deltas",
                "../reconciliation/brand_deltas.csv",
                "Metric-level reconciliation deltas for the five-brand slice.",
            ),
            _artifact_card(
                "Top risks",
                "../summary/top_risks.md",
                "Concise review of the largest invariant and reconciliation risks.",
            ),
        ]
    )

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Five-Brand Happy-Path Demo</title>
    <style>
      :root {{
        color-scheme: light;
        --bg: #f4efe7;
        --panel: rgba(255, 255, 255, 0.95);
        --line: #ded4c4;
        --text: #1e2933;
        --muted: #5f6b78;
        --accent: #0f766e;
        --accent-soft: #dff3ef;
        --warn: #9a6700;
        --warn-soft: #f8edd5;
        --fail: #b42318;
        --fail-soft: #fde4e1;
        --good: #067647;
        --good-soft: #def8e7;
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        background:
          radial-gradient(circle at top left, rgba(15, 118, 110, 0.14), transparent 24%),
          radial-gradient(circle at top right, rgba(154, 103, 0, 0.10), transparent 20%),
          linear-gradient(180deg, #f8f5ef, #f2ebe0 55%, #f7f4ee);
        color: var(--text);
      }}
      .page {{
        max-width: 1320px;
        margin: 0 auto;
        padding: 28px 18px 48px;
      }}
      .hero, .panel, .card {{
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 20px;
        box-shadow: 0 18px 40px rgba(15, 23, 42, 0.06);
      }}
      .hero {{
        padding: 26px;
        margin-bottom: 18px;
      }}
      .eyebrow {{
        margin: 0 0 8px;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.08em;
        font-size: 12px;
        font-weight: 700;
      }}
      h1, h2, h3 {{
        margin: 0;
        line-height: 1.15;
      }}
      h1 {{
        font-family: Georgia, "Times New Roman", serif;
        font-size: clamp(2.2rem, 4vw, 3.6rem);
        letter-spacing: -0.03em;
      }}
      h2 {{
        font-size: 1.22rem;
        margin-bottom: 10px;
      }}
      .subtle {{
        margin-top: 12px;
        max-width: 80ch;
        color: var(--muted);
        line-height: 1.6;
      }}
      .alert {{
        margin-top: 16px;
        padding: 14px 16px;
        border-radius: 16px;
        background: #fff8ea;
        border: 1px solid #f2d492;
        color: #7a4b00;
        line-height: 1.45;
      }}
      .grid {{
        display: grid;
        gap: 14px;
      }}
      .cards {{
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      }}
      .two-up {{
        grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
      }}
      .card, .panel {{
        padding: 16px;
      }}
      .metric-label {{
        color: var(--muted);
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }}
      .metric-value {{
        margin-top: 6px;
        font-size: 1.9rem;
        font-weight: 760;
        letter-spacing: -0.03em;
      }}
      .metric-note {{
        margin-top: 8px;
        color: var(--muted);
        font-size: 0.92rem;
      }}
      .chips {{
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-top: 12px;
      }}
      .chip {{
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 5px 11px;
        border-radius: 999px;
        background: #fff;
        border: 1px solid var(--line);
        color: var(--text);
        font-size: 0.85rem;
        font-weight: 700;
      }}
      .chip.good {{ background: var(--good-soft); color: var(--good); border-color: #b6e7c9; }}
      .chip.warn {{ background: var(--warn-soft); color: var(--warn); border-color: #f4d69d; }}
      .chip.fail {{ background: var(--fail-soft); color: var(--fail); border-color: #f3b9b3; }}
      .chip.neutral {{ background: var(--accent-soft); color: var(--accent); border-color: #b8e4de; }}
      table {{
        width: 100%;
        border-collapse: collapse;
        background: rgba(255, 255, 255, 0.96);
        border: 1px solid var(--line);
        border-radius: 18px;
        overflow: hidden;
      }}
      th, td {{
        border-bottom: 1px solid var(--line);
        padding: 10px 12px;
        vertical-align: top;
        text-align: left;
      }}
      th {{
        background: #fbf7ef;
        color: var(--muted);
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
      }}
      tr:last-child td {{ border-bottom: none; }}
      ul {{
        margin: 10px 0 0 20px;
        padding: 0;
      }}
      li + li {{ margin-top: 6px; }}
      .empty {{
        color: var(--muted);
        font-style: italic;
      }}
      .section {{ margin-top: 20px; }}
      .artifacts {{
        display: grid;
        gap: 12px;
        grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      }}
      .artifact a {{
        color: var(--accent);
        font-weight: 700;
        text-decoration: none;
      }}
      .artifact p {{
        margin: 8px 0 0;
        color: var(--muted);
        line-height: 1.5;
      }}
      code {{
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      }}
    </style>
  </head>
  <body>
    <main class="page">
      <section class="hero">
        <p class="eyebrow">Five-brand happy-path demo</p>
        <h1>Demo hub for reviewer-friendly packaging</h1>
        <p class="subtle">
          This slice shows the exact five-brand demo path. It is intentionally narrow:
          publishability, reconciliation provenance, top invariant failures, and syntheticness
          review signals are shown as-is, without implying full-workbook readiness.
        </p>
        <div class="chips">
          <span class="chip neutral">Generated at {escape(report_bundle.generated_at)}</span>
          <span class="chip neutral">{len(demo_brands)} brands</span>
          <span class="chip good">{publishable_rows} publishable rows</span>
          <span class="chip warn">{advisory_rows} advisory rows</span>
          <span class="chip fail">{blocked_rows} blocked rows</span>
          <span class="chip warn">{review_required_brands} syntheticness reviews</span>
        </div>
        <div class="chips">{included_brand_chips}</div>
        <div class="alert">
          Demo semantics stay honest: publishable, advisory, and blocked rows are shown separately;
          this package does not prove full-workbook validation or workbook-wide readiness.
        </div>
      </section>

      <section class="grid cards">
        <div class="card">
          <div class="metric-label">Brands included</div>
          <div class="metric-value">{len(demo_brands)}</div>
          <div class="metric-note">Starbucks, Taco Bell, Raising Cane&apos;s, Dutch Bros, Shake Shack.</div>
        </div>
        <div class="card">
          <div class="metric-label">Validation status</div>
          <div class="metric-value">{escape("PASS" if scorecard.validation_passed else "FAIL")}</div>
          <div class="metric-note">{scorecard.validation_counts.get("error", 0)} errors, {scorecard.validation_counts.get("warning", 0)} warnings.</div>
        </div>
        <div class="card">
          <div class="metric-label">Blocked KPI rows</div>
          <div class="metric-value">{blocked_rows}</div>
          <div class="metric-note">Blocked stays blocked. It is never folded into publishable counts.</div>
        </div>
        <div class="card">
          <div class="metric-label">Top invariant failures</div>
          <div class="metric-value">{len(invariant_rows)}</div>
          <div class="metric-note">Only failed or warning invariants are surfaced here.</div>
        </div>
      </section>

      <section class="section">
        <h2>Publishability Summary</h2>
        <p class="subtle">
          The table below keeps the row-level publishability split intact. It does not collapse
          advisory and blocked rows into a generic pass/fail view.
        </p>
        { _render_table(
            [
                "Brand",
                "Recommendation",
                "Publishable",
                "Advisory",
                "Blocked",
                "Validation",
            ],
            [
                [
                    row["brand_name"],
                    row["recommendation"],
                    row["publishable"],
                    row["advisory"],
                    row["blocked"],
                    row["validation_status"],
                ]
                for row in publishability_rows
            ],
        ) if publishability_rows else '<p class="empty">No publishability rows were available.</p>'}
      </section>

      <section class="section grid two-up">
        <div class="panel">
          <h2>Reconciliation Provenance Summary</h2>
          <p class="subtle">
            Brand-level provenance grades and the weakest field-level provenance coverage are
            shown together so reviewers can trace how the reconciliation story was assembled.
          </p>
          { _render_table(
              [
                  "Brand",
                  "Overall grade",
                  "Rank",
                  "Store count",
                  "System sales",
                  "AUV",
                  "Worst delta",
              ],
              [
                  [
                      row["brand_name"],
                      row["overall_grade"],
                      row["rank_grade"],
                      row["store_grade"],
                      row["sales_grade"],
                      row["auv_grade"],
                      row["worst_delta"],
                  ]
                  for row in provenance_rows
              ],
          ) if provenance_rows else '<p class="empty">No provenance rows were available.</p>'}
        </div>
        <div class="panel">
          <h2>Weakest Provenance Fields</h2>
          { _render_table(
              [
                  "Field",
                  "Weak or missing",
                  "Covered brands",
                  "Average confidence",
              ],
              [
                  [
                      row["field_name"],
                      row["weak_or_missing_count"],
                      row["covered_brand_count"],
                      row["average_reference_confidence"],
                  ]
                  for row in scorecard.weakest_provenance_fields
              ],
          ) if scorecard.weakest_provenance_fields else '<p class="empty">No provenance summary was available.</p>'}
        </div>
      </section>

      <section class="section">
        <h2>Top Invariant Failures</h2>
        <p class="subtle">
          These are the highest-signal invariant warnings and failures across the five-brand slice.
        </p>
        {_render_invariant_list(invariant_rows)}
      </section>

      <section class="section grid two-up">
        <div class="panel">
          <h2>Syntheticness Review Summary</h2>
          <div class="chips">
            <span class="chip warn">{review_required_brands} brands require review</span>
            <span class="chip neutral">Average score {round(_average_brand_score(syntheticness_rows), 1)}</span>
          </div>
          <p class="subtle">
            Syntheticness scores are triage signals, not proof of fabrication. Review required
            marks where the signal cluster deserves a human look.
          </p>
          { _render_table(
              ["Brand", "Score", "Review required", "Open issues"],
              [
                  [
                      row["brand_name"],
                      row["score"],
                      "yes" if row["review_required"] else "no",
                      row["open_issues"],
                  ]
                  for row in syntheticness_rows
              ],
          ) if syntheticness_rows else '<p class="empty">No syntheticness rows were available.</p>'}
        </div>
        <div class="panel">
          <h2>Artifact Links</h2>
          <div class="artifacts">
            {artifact_cards}
          </div>
        </div>
      </section>
    </main>
  </body>
</html>
"""


def package_demo_bundle(
    *,
    settings: Settings,
    bundle_root: Path | None,
    source_paths: Sequence[Path],
) -> DemoBundleArtifacts:
    """Copy the demo outputs into a shareable bundle directory."""

    resolved_root = settings.validate_artifact_root(
        bundle_root or settings.artifacts_dir / "demo_bundle",
        purpose="demo bundle",
    )
    if resolved_root.exists():
        shutil.rmtree(resolved_root)
    resolved_root.mkdir(parents=True, exist_ok=True)

    copied_paths: list[Path] = []
    bundle_manifest = {
        "bundle_root": str(resolved_root),
        "included_paths": [],
    }
    for source in source_paths:
        resolved_source = Path(source).expanduser().resolve()
        relative_path = _bundle_relative_path(settings, resolved_source)
        destination = resolved_root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(resolved_source, destination)
        copied_paths.append(destination)
        bundle_manifest["included_paths"].append(str(relative_path))

    manifest_path = resolved_root / "demo_bundle_manifest.json"
    manifest_path.write_text(
        json.dumps(bundle_manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    copied_paths.append(manifest_path)
    return DemoBundleArtifacts(
        bundle_root=resolved_root,
        manifest_json_path=manifest_path,
        copied_paths=tuple(copied_paths),
    )


def _artifact_card(title: str, href: str, description: str) -> str:
    return f"""<article class="card artifact">
      <a href="{escape(href)}">{escape(title)}</a>
      <p>{escape(description)}</p>
    </article>"""


def _publishability_rows(
    *, report_bundle: ReportBundle, demo_gold: pd.DataFrame
) -> list[dict[str, object]]:
    validation_lookup = {
        scorecard.canonical_brand_name: scorecard.validation_status
        for scorecard in report_bundle.brand_scorecards
    }
    rows: list[dict[str, object]] = []
    for brand_name, frame in demo_gold.groupby("canonical_brand_name", sort=True):
        counts = frame["publish_status"].value_counts().to_dict()
        rows.append(
            {
                "brand_name": brand_name,
                "recommendation": frame["brand_publish_status_recommendation"].iloc[0],
                "publishable": int(counts.get("publishable", 0)),
                "advisory": int(counts.get("advisory", 0)),
                "blocked": int(counts.get("blocked", 0)),
                "validation_status": validation_lookup.get(str(brand_name), "unknown"),
            }
        )
    return rows


def _provenance_rows(
    *, report_bundle: ReportBundle, brand_deltas: pd.DataFrame
) -> list[dict[str, object]]:
    scorecard_lookup = {
        scorecard.canonical_brand_name: scorecard for scorecard in report_bundle.brand_scorecards
    }
    worst_delta_lookup = _worst_delta_by_brand(brand_deltas)
    rows: list[dict[str, object]] = []
    for brand_name, scorecard in scorecard_lookup.items():
        worst_delta = worst_delta_lookup.get(brand_name, "n/a")
        rows.append(
            {
                "brand_name": brand_name,
                "overall_grade": scorecard.overall_credibility_grade,
                "rank_grade": scorecard.provenance_grades.get("rank", "MISSING"),
                "store_grade": scorecard.provenance_grades.get("store_count", "MISSING"),
                "sales_grade": scorecard.provenance_grades.get("system_sales", "MISSING"),
                "auv_grade": scorecard.provenance_grades.get("auv", "MISSING"),
                "worst_delta": worst_delta,
            }
        )
    return rows


def _worst_delta_by_brand(brand_deltas: pd.DataFrame) -> dict[str, str]:
    if brand_deltas.empty:
        return {}
    frame = brand_deltas.copy()
    frame["abs_relative_error"] = pd.to_numeric(frame["relative_error"], errors="coerce").abs()
    frame = frame.sort_values(
        by=["canonical_brand_name", "abs_relative_error"],
        ascending=[True, False],
        kind="stable",
    )
    rows: dict[str, str] = {}
    for canonical_brand_name, group in frame.groupby("canonical_brand_name", sort=True):
        row = group.iloc[0]
        relative_error = row.get("relative_error")
        if relative_error is None or pd.isna(relative_error):
            rows[str(canonical_brand_name)] = "n/a"
        else:
            rows[str(canonical_brand_name)] = f"{row['metric_name']} ({float(relative_error):.1%})"
    return rows


def _invariant_failures(report_bundle: ReportBundle) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for scorecard in report_bundle.brand_scorecards:
        for invariant in scorecard.invariant_results:
            status = str(invariant.get("status") or "")
            if status not in {"failed", "warning"}:
                continue
            rows.append(
                {
                    "brand_name": scorecard.canonical_brand_name,
                    "status": status,
                    "name": str(invariant.get("name") or ""),
                    "message": str(invariant.get("message") or ""),
                }
            )
    severity_order = {"failed": 0, "warning": 1}
    rows.sort(
        key=lambda row: (
            severity_order.get(row["status"], 99),
            row["brand_name"],
            row["name"],
        )
    )
    return rows[:8]


def _syntheticness_rows(report_bundle: ReportBundle) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for scorecard in report_bundle.brand_scorecards:
        open_issues = len(scorecard.open_issues)
        rows.append(
            {
                "brand_name": scorecard.canonical_brand_name,
                "score": scorecard.syntheticness_score,
                "review_required": scorecard.review_required,
                "open_issues": open_issues,
            }
        )
    return rows


def _average_brand_score(rows: Sequence[dict[str, object]]) -> float:
    if not rows:
        return 0.0
    return float(sum(float(row["score"]) for row in rows) / len(rows))


def _render_invariant_list(rows: Sequence[dict[str, str]]) -> str:
    if not rows:
        return '<p class="empty">No failed or warning invariants were recorded.</p>'
    items = []
    for row in rows:
        chip_class = "fail" if row["status"] == "failed" else "warn"
        items.append(
            "<li>"
            f'<span class="chip {chip_class}">{escape(row["brand_name"])} / {escape(row["name"])}</span>'
            f" {escape(row['message'])}"
            "</li>"
        )
    return f"<ul>{''.join(items)}</ul>"


def _render_table(headers: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    if not rows:
        return ""
    header_html = "".join(f"<th>{escape(str(header))}</th>" for header in headers)
    row_html: list[str] = []
    for row in rows:
        cells = "".join(f"<td>{escape(str(cell))}</td>" for cell in row)
        row_html.append(f"<tr>{cells}</tr>")
    return f"""
    <table>
      <thead>
        <tr>{header_html}</tr>
      </thead>
      <tbody>
        {''.join(row_html)}
      </tbody>
    </table>
    """


def _bundle_relative_path(settings: Settings, source_path: Path) -> Path:
    if source_path.is_relative_to(settings.reports_dir):
        return Path("reports") / source_path.relative_to(settings.reports_dir)
    if source_path.is_relative_to(settings.data_gold):
        return Path("data") / "gold" / source_path.relative_to(settings.data_gold)
    raise ValueError(f"Demo bundle does not support copying unexpected source path: {source_path}")
