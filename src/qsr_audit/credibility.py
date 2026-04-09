"""Calibrated credibility rollup built on top of Gold publish decisions."""

from __future__ import annotations

import json
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.config import Settings
from qsr_audit.syntheticness_interpretation import (
    filter_applicable_signals,
    interpret_syntheticness_signals,
)
from qsr_audit.validate.syntheticness_benchmark import (
    SyntheticnessBenchmarkRun,
    run_syntheticness_benchmark,
)

SCORING_VERSION = "credibility-v1.0.0"
DECISION_JSON_COLUMNS = ("blocking_reasons", "warning_reasons", "validation_references")
SIGNAL_JSON_COLUMNS = ("details",)
REQUIRED_COLUMNS = [
    "brand_name",
    "canonical_brand_name",
    "metric_name",
    "publish_status",
    "credibility_score",
    "provenance_score",
    "reconciliation_score",
    "syntheticness_score",
    "invariant_status",
    "supporting_signals",
    "caveats",
    "review_required",
    "scoring_version",
]
FIELD_RECONCILIATION_METRICS = {"rank", "store_count", "system_sales", "auv"}
GRADE_BASE_SCORES = {
    "A": 100,
    "B": 85,
    "C": 65,
    "D": 40,
    "F": 15,
    "MISSING": 20,
    None: 20,
}
INVARIANT_PENALTIES = {
    "passed": 0,
    "warning": 15,
    "failed": 35,
    "unknown": 5,
}


@dataclass(frozen=True)
class CredibilityArtifacts:
    """Files written by the credibility engine."""

    rollup_parquet_path: Path
    scorecard_html_path: Path
    method_markdown_path: Path
    benchmark_metrics_json_path: Path
    benchmark_summary_markdown_path: Path


@dataclass(frozen=True)
class CredibilityRun:
    """Complete credibility engine result."""

    rollup: pd.DataFrame
    summary: dict[str, Any]
    benchmark: SyntheticnessBenchmarkRun
    artifacts: CredibilityArtifacts


def score_credibility(
    settings: Settings | None = None,
    *,
    gold_dir: Path | None = None,
    report_dir: Path | None = None,
    benchmark_root: Path | None = None,
) -> CredibilityRun:
    """Build a conservative credibility rollup without changing Gold publish status."""

    resolved_settings = settings or Settings()
    resolved_gold_dir = (gold_dir or resolved_settings.data_gold).expanduser().resolve()
    resolved_report_dir = (
        (report_dir or (resolved_settings.reports_dir / "summary")).expanduser().resolve()
    )
    resolved_benchmark_root = resolved_settings.validate_artifact_root(
        benchmark_root or (resolved_settings.artifacts_dir / "syntheticness"),
        purpose="syntheticness benchmark artifacts",
    )

    decisions = _read_required_parquet(
        resolved_gold_dir / "gold_publish_decisions.parquet",
        missing_hint="Run `qsr-audit gate-gold` first.",
    )
    decisions = _parse_json_columns(decisions, columns=DECISION_JSON_COLUMNS)

    syntheticness_signals = _read_required_parquet(
        resolved_gold_dir / "syntheticness_signals.parquet",
        missing_hint="Run `qsr-audit run-syntheticness` first.",
    )
    syntheticness_signals = _parse_json_columns(syntheticness_signals, columns=SIGNAL_JSON_COLUMNS)

    rollup = build_credibility_rollup(
        decisions=decisions,
        syntheticness_signals=syntheticness_signals,
    )
    summary = build_credibility_summary(rollup)
    benchmark = run_syntheticness_benchmark(
        settings=resolved_settings,
        output_root=resolved_benchmark_root,
    )
    artifacts = write_credibility_outputs(
        rollup=rollup,
        summary=summary,
        benchmark=benchmark,
        gold_dir=resolved_gold_dir,
        report_dir=resolved_report_dir,
    )
    return CredibilityRun(
        rollup=rollup,
        summary=summary,
        benchmark=benchmark,
        artifacts=artifacts,
    )


def build_credibility_rollup(
    *,
    decisions: pd.DataFrame,
    syntheticness_signals: pd.DataFrame,
) -> pd.DataFrame:
    """Score each Gold publish decision row with conservative evidence weighting."""

    if decisions.empty:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)

    rows: list[dict[str, Any]] = []
    for row in decisions.to_dict(orient="records"):
        brand_name = str(row.get("brand_name") or row.get("canonical_brand_name") or "")
        canonical_brand_name = str(row.get("canonical_brand_name") or brand_name)
        metric_name = str(row.get("metric_name") or "")

        applicable_signals = filter_applicable_signals(
            syntheticness_signals,
            brand_name=brand_name,
            canonical_brand_name=canonical_brand_name,
            metric_name=metric_name,
        )
        syntheticness = interpret_syntheticness_signals(applicable_signals)
        invariant_status, invariant_messages = _invariant_status(row)
        provenance_score, provenance_signal, provenance_caveats = _score_provenance(row)
        reconciliation_score, reconciliation_signal, reconciliation_caveats = _score_reconciliation(
            row=row,
            metric_name=metric_name,
        )
        credibility_score = _score_credibility_components(
            provenance_score=provenance_score,
            reconciliation_score=reconciliation_score,
            syntheticness_score=syntheticness.syntheticness_score,
            invariant_status=invariant_status,
        )
        review_required = _review_required(
            publish_status=str(row.get("publish_status") or "advisory"),
            credibility_score=credibility_score,
            provenance_score=provenance_score,
            reconciliation_score=reconciliation_score,
            syntheticness_review_required=syntheticness.review_required,
            invariant_status=invariant_status,
        )
        supporting_signals = _build_supporting_signals(
            row=row,
            provenance_signal=provenance_signal,
            reconciliation_signal=reconciliation_signal,
            invariant_status=invariant_status,
            invariant_messages=invariant_messages,
            syntheticness_supporting_signals=syntheticness.supporting_signals,
        )
        caveats = _build_caveats(
            row=row,
            reconciliation_caveats=reconciliation_caveats,
            provenance_caveats=provenance_caveats,
            syntheticness_caveats=syntheticness.caveats,
        )

        rows.append(
            {
                "brand_name": brand_name,
                "canonical_brand_name": canonical_brand_name,
                "metric_name": metric_name,
                "publish_status": str(row.get("publish_status") or "advisory"),
                "credibility_score": credibility_score,
                "provenance_score": provenance_score,
                "reconciliation_score": reconciliation_score,
                "syntheticness_score": int(syntheticness.syntheticness_score),
                "invariant_status": invariant_status,
                "supporting_signals": json.dumps(
                    supporting_signals,
                    ensure_ascii=False,
                    default=str,
                ),
                "caveats": json.dumps(caveats, ensure_ascii=False, default=str),
                "review_required": review_required,
                "scoring_version": SCORING_VERSION,
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    return frame.loc[:, REQUIRED_COLUMNS].sort_values(
        by=["canonical_brand_name", "metric_name"],
        kind="stable",
        ignore_index=True,
    )


def build_credibility_summary(rollup: pd.DataFrame) -> dict[str, Any]:
    """Build a compact summary for the HTML scorecard."""

    if rollup.empty:
        return {
            "row_count": 0,
            "review_required_count": 0,
            "average_credibility_score": 0.0,
            "publish_status_counts": {},
            "top_review_rows": [],
            "brand_summaries": [],
        }

    publish_status_counts = {
        str(key): int(value)
        for key, value in rollup["publish_status"].value_counts().to_dict().items()
    }
    top_review_rows = (
        rollup.loc[rollup["review_required"]]
        .sort_values(
            by=["credibility_score", "syntheticness_score", "canonical_brand_name", "metric_name"],
            ascending=[True, False, True, True],
            kind="stable",
        )
        .head(10)
        .to_dict(orient="records")
    )

    brand_summaries = []
    for brand_name, frame in rollup.groupby("canonical_brand_name", sort=True):
        brand_summaries.append(
            {
                "brand_name": brand_name,
                "rows": int(len(frame)),
                "average_credibility_score": round(float(frame["credibility_score"].mean()), 1),
                "review_required_rows": int(frame["review_required"].sum()),
                "blocked_rows": int(frame["publish_status"].eq("blocked").sum()),
            }
        )

    return {
        "row_count": int(len(rollup)),
        "review_required_count": int(rollup["review_required"].sum()),
        "average_credibility_score": round(float(rollup["credibility_score"].mean()), 1),
        "publish_status_counts": publish_status_counts,
        "top_review_rows": top_review_rows,
        "brand_summaries": brand_summaries,
    }


def write_credibility_outputs(
    *,
    rollup: pd.DataFrame,
    summary: dict[str, Any],
    benchmark: SyntheticnessBenchmarkRun,
    gold_dir: Path,
    report_dir: Path,
) -> CredibilityArtifacts:
    """Write the parquet rollup plus reviewer-facing report artifacts."""

    gold_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    rollup_parquet_path = gold_dir / "credibility_rollup.parquet"
    scorecard_html_path = report_dir / "credibility_scorecard.html"
    method_markdown_path = report_dir / "credibility_method.md"

    rollup.to_parquet(rollup_parquet_path, index=False)
    scorecard_html_path.write_text(
        render_credibility_scorecard_html(rollup=rollup, summary=summary),
        encoding="utf-8",
    )
    method_markdown_path.write_text(
        render_credibility_method_markdown(summary=summary, benchmark=benchmark),
        encoding="utf-8",
    )

    return CredibilityArtifacts(
        rollup_parquet_path=rollup_parquet_path,
        scorecard_html_path=scorecard_html_path,
        method_markdown_path=method_markdown_path,
        benchmark_metrics_json_path=benchmark.artifacts.metrics_json_path,
        benchmark_summary_markdown_path=benchmark.artifacts.summary_markdown_path,
    )


def render_credibility_scorecard_html(*, rollup: pd.DataFrame, summary: dict[str, Any]) -> str:
    """Render a screenshot-friendly credibility scorecard."""

    publishable = summary["publish_status_counts"].get("publishable", 0)
    advisory = summary["publish_status_counts"].get("advisory", 0)
    blocked = summary["publish_status_counts"].get("blocked", 0)

    review_rows = []
    for row in summary["top_review_rows"]:
        signals = json.loads(str(row["supporting_signals"]))
        signal_preview = "<br />".join(
            escape(str(signal.get("message") or ""))
            for signal in signals[:2]
            if isinstance(signal, dict)
        )
        review_rows.append(
            "<tr>"
            f"<td>{escape(str(row['canonical_brand_name']))}</td>"
            f"<td>{escape(str(row['metric_name']))}</td>"
            f"<td>{escape(str(row['publish_status']))}</td>"
            f"<td>{int(row['credibility_score'])}</td>"
            f"<td>{int(row['provenance_score'])}</td>"
            f"<td>{int(row['reconciliation_score'])}</td>"
            f"<td>{int(row['syntheticness_score'])}</td>"
            f"<td>{escape(str(row['invariant_status']))}</td>"
            f"<td>{'yes' if bool(row['review_required']) else 'no'}</td>"
            f"<td>{signal_preview or '&mdash;'}</td>"
            "</tr>"
        )

    brand_rows = []
    for row in summary["brand_summaries"]:
        brand_rows.append(
            "<tr>"
            f"<td>{escape(str(row['brand_name']))}</td>"
            f"<td>{int(row['rows'])}</td>"
            f"<td>{row['average_credibility_score']}</td>"
            f"<td>{int(row['review_required_rows'])}</td>"
            f"<td>{int(row['blocked_rows'])}</td>"
            "</tr>"
        )

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Credibility Scorecard</title>
    <style>
      :root {{
        color-scheme: light;
        --bg: #f6f2ea;
        --panel: rgba(255, 255, 255, 0.96);
        --line: #dfd5c6;
        --text: #1f2933;
        --muted: #5b6672;
        --accent: #14532d;
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        background: linear-gradient(180deg, #faf7f1, #f2ece1 60%, #f7f3eb);
        color: var(--text);
        font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      }}
      main {{
        max-width: 1240px;
        margin: 0 auto;
        padding: 28px 18px 44px;
      }}
      .hero, .panel, .card {{
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 18px;
        box-shadow: 0 16px 34px rgba(15, 23, 42, 0.06);
      }}
      .hero, .panel {{
        padding: 18px;
      }}
      .eyebrow {{
        margin: 0 0 10px;
        color: var(--muted);
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }}
      h1, h2 {{
        margin: 0;
        line-height: 1.12;
      }}
      h1 {{
        font-family: Georgia, "Times New Roman", serif;
        font-size: clamp(2rem, 3.5vw, 3.1rem);
        letter-spacing: -0.03em;
      }}
      h2 {{
        font-size: 1.1rem;
        margin-bottom: 12px;
      }}
      p {{
        color: var(--muted);
        line-height: 1.6;
      }}
      .cards, .grid {{
        display: grid;
        gap: 14px;
      }}
      .cards {{
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        margin-top: 18px;
      }}
      .grid {{
        grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
        margin-top: 18px;
      }}
      .card {{
        padding: 16px;
      }}
      .metric-label {{
        color: var(--muted);
        font-size: 0.8rem;
        letter-spacing: 0.06em;
        text-transform: uppercase;
      }}
      .metric-value {{
        margin-top: 6px;
        font-size: 1.9rem;
        font-weight: 760;
      }}
      table {{
        width: 100%;
        border-collapse: collapse;
      }}
      th, td {{
        border-bottom: 1px solid var(--line);
        padding: 10px 12px;
        text-align: left;
        vertical-align: top;
      }}
      th {{
        background: #fbf8f2;
        color: var(--muted);
        font-size: 0.78rem;
        letter-spacing: 0.05em;
        text-transform: uppercase;
      }}
      tr:last-child td {{ border-bottom: none; }}
      code {{
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      }}
    </style>
  </head>
  <body>
    <main>
      <section class="hero">
        <p class="eyebrow">Calibrated credibility engine</p>
        <h1>Evidence quality stays separate from publishability.</h1>
        <p>
          This scorecard combines invariant status, reconciliation quality, provenance quality,
          and syntheticness review pressure into one conservative row-level rollup. It inherits
          <code>publish_status</code> unchanged from the Gold gate.
        </p>
      </section>

      <section class="cards">
        <div class="card">
          <div class="metric-label">Rows scored</div>
          <div class="metric-value">{summary['row_count']}</div>
        </div>
        <div class="card">
          <div class="metric-label">Average credibility</div>
          <div class="metric-value">{summary['average_credibility_score']}</div>
        </div>
        <div class="card">
          <div class="metric-label">Review required</div>
          <div class="metric-value">{summary['review_required_count']}</div>
        </div>
        <div class="card">
          <div class="metric-label">Publishable / advisory / blocked</div>
          <div class="metric-value">{publishable} / {advisory} / {blocked}</div>
        </div>
      </section>

      <section class="grid">
        <div class="panel">
          <h2>Top Review Rows</h2>
          <table>
            <thead>
              <tr>
                <th>Brand</th>
                <th>Metric</th>
                <th>Status</th>
                <th>Credibility</th>
                <th>Provenance</th>
                <th>Reconciliation</th>
                <th>Syntheticness</th>
                <th>Invariant</th>
                <th>Review</th>
                <th>Signals</th>
              </tr>
            </thead>
            <tbody>
              {''.join(review_rows) if review_rows else '<tr><td colspan="10">No review rows triggered.</td></tr>'}
            </tbody>
          </table>
        </div>

        <div class="panel">
          <h2>Brand Summary</h2>
          <table>
            <thead>
              <tr>
                <th>Brand</th>
                <th>Rows</th>
                <th>Average credibility</th>
                <th>Review rows</th>
                <th>Blocked rows</th>
              </tr>
            </thead>
            <tbody>
              {''.join(brand_rows) if brand_rows else '<tr><td colspan="5">No brand rows available.</td></tr>'}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  </body>
</html>
"""


def render_credibility_method_markdown(
    *,
    summary: dict[str, Any],
    benchmark: SyntheticnessBenchmarkRun,
) -> str:
    """Render the method note that explains the credibility score."""

    lines = [
        "# Credibility Method",
        "",
        f"- Scoring version: `{SCORING_VERSION}`",
        "- `publish_status` is inherited from the Gold gate. The credibility engine does not relabel rows.",
        "- Syntheticness is interpreted as review pressure only. It cannot silently auto-block a row by itself.",
        f"- Rows scored in the latest run: `{summary['row_count']}`",
        f"- Benchmark cases in the latest syntheticness harness: `{benchmark.metrics['case_count']}`",
        "",
        "## Component Weights",
        "",
        "- Provenance score weight: `40%`",
        "- Reconciliation score weight: `35%`",
        "- Syntheticness damping weight: `25%` applied as `100 - syntheticness_score`",
        "- Invariant penalty: `passed=0`, `warning=15`, `failed=35`, `unknown=5`",
        "",
        "## Score Construction",
        "",
        "- `provenance_score` uses source confidence, provenance completeness, and external-source support.",
        "- `reconciliation_score` uses Gold reconciliation grade plus a relative-error penalty when metric-level reconciliation exists.",
        "- `syntheticness_score` comes from the offline joined interpretation layer over row-applicable syntheticness signals.",
        "- `invariant_status` comes from Gold validation references and remains traceable at the row level.",
        "",
        "## Review Triggers",
        "",
        "- `publish_status != publishable`",
        "- `invariant_status != passed`",
        "- syntheticness interpretation marks the row for review",
        "- `provenance_score < 70`",
        "- `reconciliation_score < 70`",
        "- `credibility_score < 65`",
        "",
        "## What The Score Means",
        "",
        "- Higher scores mean the row has stronger, better-aligned evidence across provenance and reconciliation with fewer review pressures.",
        "- Lower scores mean the row is evidence-fragile, policy-constrained, or otherwise deserves reviewer attention before reuse.",
        "",
        "## What The Score Does Not Mean",
        "",
        "- It is not a proof-of-truth score.",
        "- It is not full-workbook validation readiness.",
        "- It does not promote advisory or blocked rows into publishable output.",
        "- Syntheticness remains a review cue, not a standalone fabrication detector.",
        "",
    ]
    return "\n".join(lines)


def _read_required_parquet(path: Path, *, missing_hint: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact `{path}`. {missing_hint}")
    return pd.read_parquet(path)


def _parse_json_columns(frame: pd.DataFrame, *, columns: tuple[str, ...]) -> pd.DataFrame:
    if frame.empty:
        return frame
    parsed = frame.copy()
    for column in columns:
        if column not in parsed.columns:
            continue
        parsed[column] = parsed[column].map(_parse_jsonish)
    return parsed


def _parse_jsonish(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, list | dict):
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


def _invariant_status(row: dict[str, Any]) -> tuple[str, tuple[str, ...]]:
    references = row.get("validation_references")
    if references is None:
        return "unknown", ()
    if not isinstance(references, list):
        return "unknown", ()

    messages = tuple(
        str(reference.get("message"))
        for reference in references
        if isinstance(reference, dict) and reference.get("message")
    )
    severities = {
        str(reference.get("severity") or "").lower()
        for reference in references
        if isinstance(reference, dict)
    }
    if "error" in severities:
        return "failed", messages
    if severities & {"warning", "info"}:
        return "warning", messages
    return "passed", messages


def _score_provenance(row: dict[str, Any]) -> tuple[int, dict[str, Any], list[str]]:
    required_fields = (
        "source_type",
        "source_name",
        "source_url_or_doc_id",
        "as_of_date",
        "method_reported_or_estimated",
        "confidence_score",
    )
    present_count = sum(1 for field in required_fields if _has_value(row.get(field)))
    completeness_ratio = present_count / len(required_fields)
    confidence_score = max(0.0, min(1.0, _to_float(row.get("confidence_score")) or 0.0))
    source_type = str(row.get("source_type") or "unknown")
    external_source_count = min(3, max(0, _to_int(row.get("reference_source_count"))))
    external_bonus = 0.0
    caveats: list[str] = []
    if source_type != "workbook" and _truthy_flag(row.get("reference_evidence_present")):
        external_bonus = (external_source_count / 3) * 20
    else:
        caveats.append(
            "External-source bonus was withheld because the row is workbook-only or missing reference evidence."
        )
    if present_count < len(required_fields):
        caveats.append("One or more provenance fields are missing on the selected source row.")

    score = round((confidence_score * 45) + (completeness_ratio * 35) + external_bonus)
    signal = {
        "family": "provenance",
        "score": int(score),
        "message": (
            f"Primary provenance source `{source_type}` with confidence "
            f"`{confidence_score:.2f}` and `{external_source_count}` external reference source(s)."
        ),
    }
    return _clamp_score(score), signal, caveats


def _score_reconciliation(
    *,
    row: dict[str, Any],
    metric_name: str,
) -> tuple[int, dict[str, Any], list[str]]:
    if metric_name not in FIELD_RECONCILIATION_METRICS:
        score = 55
        return (
            score,
            {
                "family": "reconciliation",
                "score": score,
                "message": (
                    f"No field-level external reconciliation is configured for `{metric_name}`; "
                    "a neutral fallback score was applied."
                ),
            },
            [
                "Field-level external reconciliation is not configured for this metric, so reconciliation uses a neutral fallback."
            ],
        )

    if not _truthy_flag(row.get("reference_evidence_present")):
        score = 20
        return (
            score,
            {
                "family": "reconciliation",
                "score": score,
                "message": "Reference evidence is absent for this row, so reconciliation confidence stays low.",
            },
            ["Reference evidence is missing for this metric row."],
        )

    grade = _clean_optional_text(row.get("reconciliation_grade"))
    base_score = GRADE_BASE_SCORES.get(grade, 20)
    relative_error = _to_float(row.get("reconciliation_relative_error"))
    penalty = 0 if relative_error is None else min(25, int(round(relative_error * 200)))
    score = _clamp_score(base_score - penalty)
    message = (
        f"Reconciliation grade `{grade or 'MISSING'}` with relative error "
        f"`{_format_percent(relative_error)}`."
    )
    caveats = []
    if relative_error is None:
        caveats.append("Relative error is missing, so reconciliation used grade-only scoring.")
    return (
        score,
        {
            "family": "reconciliation",
            "score": score,
            "message": message,
        },
        caveats,
    )


def _score_credibility_components(
    *,
    provenance_score: int,
    reconciliation_score: int,
    syntheticness_score: int,
    invariant_status: str,
) -> int:
    base_score = (
        (provenance_score * 0.40)
        + (reconciliation_score * 0.35)
        + ((100 - syntheticness_score) * 0.25)
    )
    penalty = INVARIANT_PENALTIES.get(invariant_status, INVARIANT_PENALTIES["unknown"])
    return _clamp_score(round(base_score - penalty))


def _review_required(
    *,
    publish_status: str,
    credibility_score: int,
    provenance_score: int,
    reconciliation_score: int,
    syntheticness_review_required: bool,
    invariant_status: str,
) -> bool:
    return any(
        [
            publish_status != "publishable",
            publish_status == "blocked",
            invariant_status != "passed",
            syntheticness_review_required,
            provenance_score < 70,
            reconciliation_score < 70,
            credibility_score < 65,
        ]
    )


def _build_supporting_signals(
    *,
    row: dict[str, Any],
    provenance_signal: dict[str, Any],
    reconciliation_signal: dict[str, Any],
    invariant_status: str,
    invariant_messages: tuple[str, ...],
    syntheticness_supporting_signals: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    signals = [
        provenance_signal,
        reconciliation_signal,
        {
            "family": "invariants",
            "status": invariant_status,
            "message": invariant_messages[0]
            if invariant_messages
            else "No applicable blocking validation findings were recorded for this row.",
        },
        {
            "family": "policy",
            "status": str(row.get("publish_status") or "advisory"),
            "message": (
                f"Publish status stays `{row.get('publish_status') or 'advisory'}` under the Gold gate; "
                "credibility scoring does not relabel rows."
            ),
        },
    ]
    for signal in syntheticness_supporting_signals[:3]:
        signals.append(
            {
                "family": "syntheticness",
                "strength": signal.get("strength"),
                "score_contribution": signal.get("score_contribution"),
                "message": str(signal.get("plain_english") or signal.get("title") or ""),
            }
        )
    return signals


def _build_caveats(
    *,
    row: dict[str, Any],
    reconciliation_caveats: list[str],
    provenance_caveats: list[str],
    syntheticness_caveats: list[str],
) -> list[str]:
    caveats: list[str] = []
    publish_status = str(row.get("publish_status") or "advisory")
    if publish_status != "publishable":
        caveats.append(
            f"The Gold gate already keeps this row `{publish_status}`; the credibility score does not promote it."
        )
    for reason in _as_string_list(row.get("blocking_reasons"))[:2]:
        caveats.append(reason)
    for reason in _as_string_list(row.get("warning_reasons"))[:2]:
        caveats.append(reason)
    caveats.extend(provenance_caveats)
    caveats.extend(reconciliation_caveats)
    caveats.extend(syntheticness_caveats)
    return _dedupe_preserve_order(caveats)[:6]


def _as_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return []


def _truthy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _has_value(value: Any) -> bool:
    return _clean_optional_text(value) is not None


def _clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _format_percent(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.1%}"


def _clamp_score(value: int | float) -> int:
    return max(0, min(100, int(round(value))))


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


run_credibility_scoring = score_credibility


__all__ = [
    "CredibilityArtifacts",
    "CredibilityRun",
    "REQUIRED_COLUMNS",
    "SCORING_VERSION",
    "build_credibility_rollup",
    "build_credibility_summary",
    "render_credibility_method_markdown",
    "render_credibility_scorecard_html",
    "run_credibility_scoring",
    "score_credibility",
    "write_credibility_outputs",
]
