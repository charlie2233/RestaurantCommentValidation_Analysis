"""Minimal read-only dashboard for local validation and reconciliation artifacts."""

from __future__ import annotations

import html
import json
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

try:
    import pandas as pd
except Exception:  # pragma: no cover - optional dependency for local dashboard preview
    pd = None  # type: ignore[assignment]

DEFAULT_REPORTS_DIR = Path("reports")
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8501


@dataclass(frozen=True)
class DashboardArtifacts:
    """Local artifacts used to render the dashboard."""

    generated_report_json: dict[str, Any] | None
    generated_report_html: str | None
    validation_results: dict[str, Any] | None
    validation_summary_markdown: str | None
    reconciliation_summary_markdown: str | None
    syntheticness_report_markdown: str | None
    reconciled_core_metrics: Any | None
    provenance_registry: Any | None


def load_dashboard_artifacts(reports_dir: Path = DEFAULT_REPORTS_DIR) -> DashboardArtifacts:
    """Load local report artifacts and Gold parquet outputs."""

    generated_report_json = _read_json(reports_dir / "index.json")
    generated_report_html = _read_text(reports_dir / "index.html")
    validation_results = _read_json(reports_dir / "validation" / "validation_results.json")
    validation_summary = _read_text(reports_dir / "validation" / "validation_summary.md")
    reconciliation_summary = _read_text(
        reports_dir / "reconciliation" / "reconciliation_summary.md"
    )
    syntheticness_report = _read_text(reports_dir / "validation" / "syntheticness_report.md")
    reconciled_core_metrics = _read_parquet(Path("data/gold/reconciled_core_metrics.parquet"))
    provenance_registry = _read_parquet(Path("data/gold/provenance_registry.parquet"))

    return DashboardArtifacts(
        generated_report_json=generated_report_json,
        generated_report_html=generated_report_html,
        validation_results=validation_results,
        validation_summary_markdown=validation_summary,
        reconciliation_summary_markdown=reconciliation_summary,
        syntheticness_report_markdown=syntheticness_report,
        reconciled_core_metrics=reconciled_core_metrics,
        provenance_registry=provenance_registry,
    )


def render_dashboard_html(artifacts: DashboardArtifacts) -> str:
    """Render a single read-only HTML page from local artifacts."""

    if artifacts.generated_report_html:
        return artifacts.generated_report_html

    validation = artifacts.validation_results or {}
    counts = validation.get("counts", {})
    findings = validation.get("findings", [])
    validation_status = "PASS" if validation.get("passed") else "FAIL"
    passed_validation_count = max(len(findings) - int(counts.get("error", 0)), 0)
    failed_validation_count = int(counts.get("error", 0))
    total_brands = (
        int(artifacts.reconciled_core_metrics.shape[0])
        if _has_frame(artifacts.reconciled_core_metrics)
        else 0
    )
    provenance_fields = _weak_provenance_fields(artifacts.provenance_registry)
    reconciliation_errors = _largest_reconciliation_errors(artifacts.reconciled_core_metrics)
    syntheticness_overview = _syntheticness_overview()

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>QSR Audit Dashboard</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f6f4ef;
      --panel: #ffffff;
      --text: #1f2937;
      --muted: #6b7280;
      --accent: #0f766e;
      --accent-soft: #d9f0ec;
      --warning: #b45309;
      --border: #e5e7eb;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(15, 118, 110, 0.12), transparent 28%),
        radial-gradient(circle at top right, rgba(180, 83, 9, 0.08), transparent 20%),
        var(--bg);
      color: var(--text);
    }}
    .wrap {{ max-width: 1240px; margin: 0 auto; padding: 28px 20px 48px; }}
    header {{
      display: flex; flex-wrap: wrap; gap: 16px; align-items: end; justify-content: space-between;
      margin-bottom: 24px;
    }}
    h1 {{ margin: 0; font-size: clamp(2rem, 5vw, 3.1rem); letter-spacing: -0.03em; }}
    .subtle {{ color: var(--muted); max-width: 70ch; line-height: 1.5; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 14px;
      margin: 22px 0 28px;
    }}
    .card {{
      background: rgba(255,255,255,0.92);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 16px;
      box-shadow: 0 14px 34px rgba(15, 23, 42, 0.06);
    }}
    .card h2, .card h3 {{ margin: 0 0 10px; font-size: 1rem; }}
    .kpi {{ font-size: 2rem; font-weight: 700; letter-spacing: -0.03em; }}
    .label {{ color: var(--muted); font-size: 0.92rem; }}
    .pill {{
      display: inline-flex; align-items: center; gap: 6px;
      border-radius: 999px; padding: 4px 10px; background: var(--accent-soft); color: var(--accent);
      font-size: 0.82rem; font-weight: 600;
    }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.93rem; }}
    th, td {{ text-align: left; padding: 9px 8px; border-bottom: 1px solid var(--border); vertical-align: top; }}
    th {{ color: var(--muted); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.06em; }}
    code, pre {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
    }}
    pre {{
      white-space: pre-wrap;
      word-break: break-word;
      background: #0b1220;
      color: #e5eef8;
      padding: 16px;
      border-radius: 16px;
      overflow: auto;
    }}
    details {{
      background: rgba(255,255,255,0.9);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 14px 16px;
      margin: 14px 0;
    }}
    summary {{ cursor: pointer; font-weight: 650; }}
    .stack {{ display: grid; gap: 12px; }}
    .note {{ color: var(--muted); font-size: 0.9rem; }}
    .warn {{ color: var(--warning); font-weight: 600; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }}
    .section-title {{
      display: flex; align-items: baseline; justify-content: space-between; gap: 12px;
      margin: 28px 0 12px;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <header>
      <div>
        <div class="pill">Read-only local dashboard</div>
        <h1>QSR Audit Dashboard</h1>
        <div class="subtle">
          Executive-facing summary from local validation, reconciliation, syntheticness, and Gold-layer files only.
          Missing artifacts are surfaced explicitly rather than hidden.
        </div>
      </div>
      <div class="card" style="min-width: 280px;">
        <h2>Validation</h2>
        <div class="kpi">{validation_status}</div>
        <div class="label">Errors: {counts.get("error", 0)} | Warnings: {counts.get("warning", 0)} | Info: {counts.get("info", 0)}</div>
      </div>
    </header>

    <section class="grid">
      <div class="card">
        <h2>Total Brands</h2>
        <div class="kpi">{total_brands}</div>
        <div class="label">Brands in reconciled Gold core metrics</div>
      </div>
      <div class="card">
        <h2>Warning Counts</h2>
        <div class="kpi">{sum(_warning_counts_by_category(findings).values())}</div>
        <div class="label">Across validation findings</div>
      </div>
      <div class="card">
        <h2>Weakest Provenance</h2>
        <div class="kpi">{len(provenance_fields)}</div>
        <div class="label">Fields needing the most manual support</div>
      </div>
      <div class="card">
        <h2>Biggest Errors</h2>
        <div class="kpi">{len(reconciliation_errors)}</div>
        <div class="label">Field comparisons with the largest gaps</div>
      </div>
    </section>

    <div class="section-title">
      <h2>Global Credibility Scorecard</h2>
      <div class="note">Useful for executive review and analyst triage.</div>
    </div>
    <div class="card">
      <table>
        <thead>
          <tr>
            <th>Metric</th>
            <th>Value</th>
            <th>Notes</th>
          </tr>
        </thead>
        <tbody>
          <tr><td>Total brands</td><td>{total_brands}</td><td>From reconciled Gold core metrics.</td></tr>
          <tr><td>Passed validations</td><td>{passed_validation_count}</td><td>All validation findings except hard errors.</td></tr>
          <tr><td>Failed validations</td><td>{failed_validation_count}</td><td>Hard errors from schema and invariant checks.</td></tr>
          <tr><td>Warning counts</td><td>{counts.get("warning", 0)}</td><td>Includes cross-sheet and missing-coverage warnings.</td></tr>
          <tr><td>Syntheticness overview</td><td>{syntheticness_overview}</td><td>Weak-to-moderate anomaly signals only.</td></tr>
        </tbody>
      </table>
    </div>

    <div class="section-title">
      <h2>Warning Counts By Category</h2>
    </div>
    <div class="card">
      {_render_warning_counts(_warning_counts_by_category(findings))}
    </div>

    <div class="section-title">
      <h2>Fields With Weakest Provenance</h2>
    </div>
    <div class="card">
      {_render_simple_list(provenance_fields, empty_message="No provenance registry was found locally.")}
    </div>

    <div class="section-title">
      <h2>Biggest Reconciliation Errors</h2>
    </div>
    <div class="card">
      {_render_simple_list(reconciliation_errors, empty_message="No reconciled rows with numeric comparisons were found.")}
    </div>

    <div class="section-title">
      <h2>Brand-Level Scorecards</h2>
    </div>
    <div class="stack">
      {_render_brand_scorecards(artifacts)}
    </div>

    <div class="section-title">
      <h2>Raw Artifacts</h2>
    </div>
    <details open>
      <summary>Validation summary</summary>
      <pre>{_escape_pre(artifacts.validation_summary_markdown or "Missing local file.")}</pre>
    </details>
    <details>
      <summary>Reconciliation summary</summary>
      <pre>{_escape_pre(artifacts.reconciliation_summary_markdown or "Missing local file.")}</pre>
    </details>
    <details>
      <summary>Syntheticness report</summary>
      <pre>{_escape_pre(artifacts.syntheticness_report_markdown or "Missing local file.")}</pre>
    </details>
    <details>
      <summary>Reconciled core preview</summary>
      <pre>{_escape_pre(_preview_dataframe(artifacts.reconciled_core_metrics))}</pre>
    </details>
  </div>
</body>
</html>
"""


def build_dashboard_json(artifacts: DashboardArtifacts) -> dict[str, Any]:
    """Return a machine-readable snapshot for the dashboard."""

    if artifacts.generated_report_json:
        return artifacts.generated_report_json

    validation = artifacts.validation_results or {}
    findings = validation.get("findings", [])
    return {
        "validation": validation,
        "summary": {
            "total_brands": int(artifacts.reconciled_core_metrics.shape[0])
            if _has_frame(artifacts.reconciled_core_metrics)
            else 0,
            "warning_counts_by_category": _warning_counts_by_category(findings),
            "weakest_provenance_fields": _weak_provenance_fields(artifacts.provenance_registry),
            "biggest_reconciliation_errors": _largest_reconciliation_errors(
                artifacts.reconciled_core_metrics
            ),
            "syntheticness_overview": _syntheticness_overview(),
        },
        "brand_scorecards": _brand_scorecards(artifacts),
    }


def serve_dashboard(
    *,
    reports_dir: Path = DEFAULT_REPORTS_DIR,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
) -> None:
    """Serve the local dashboard over HTTP."""

    artifacts = load_dashboard_artifacts(reports_dir)
    snapshot = build_dashboard_json(artifacts)
    html_page = render_dashboard_html(artifacts)

    class DashboardHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/api.json":
                payload = json.dumps(snapshot, ensure_ascii=False, indent=2).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if self.path not in {"/", "/index.html"}:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"Not found")
                return

            payload = html_page.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"Serving QSR Audit dashboard at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main() -> None:
    """Entry point for the local dashboard stub."""

    serve_dashboard()


def _brand_scorecards(artifacts: DashboardArtifacts) -> list[dict[str, Any]]:
    frame = artifacts.reconciled_core_metrics
    if not _has_frame(frame):
        return []

    validation_findings = (
        artifacts.validation_results.get("findings", []) if artifacts.validation_results else []
    )
    scorecards = []
    for row in frame.to_dict(orient="records"):
        brand_name = row.get("brand_name")
        invariant_results = [
            {
                "severity": finding.get("severity"),
                "category": finding.get("category"),
                "check_name": finding.get("check_name"),
                "message": finding.get("message"),
            }
            for finding in validation_findings
            if finding.get("brand_name") is not None
            and str(finding.get("brand_name")) == str(brand_name)
        ]
        open_issues: list[str] = []
        if row.get("reconciliation_warning"):
            open_issues.append(str(row.get("reconciliation_warning")))
        open_issues.extend(
            finding.get("message")
            for finding in validation_findings
            if finding.get("severity") in {"error", "warning"}
            and finding.get("brand_name") is not None
            and str(finding.get("brand_name")) == str(brand_name)
        )
        deduped_issues: list[str] = []
        for issue in open_issues:
            if issue and issue not in deduped_issues:
                deduped_issues.append(issue)
        scorecards.append(
            {
                "brand_name": brand_name,
                "canonical_brand_name": row.get("canonical_brand_name"),
                "normalized_metrics": {
                    "rank": row.get("rank"),
                    "us_store_count_2024": row.get("us_store_count_2024"),
                    "systemwide_revenue_usd_billions_2024": row.get(
                        "systemwide_revenue_usd_billions_2024"
                    ),
                    "average_unit_volume_usd_thousands": row.get(
                        "average_unit_volume_usd_thousands"
                    ),
                    "fte_mid": row.get("fte_mid"),
                    "margin_mid_pct": row.get("margin_mid_pct"),
                },
                "provenance_grades": {
                    "brand_match": row.get("brand_match_confidence"),
                    "rank": row.get("rank_credibility_grade"),
                    "store_count": row.get("store_count_credibility_grade"),
                    "system_sales": row.get("system_sales_credibility_grade"),
                    "auv": row.get("auv_credibility_grade"),
                    "overall": row.get("overall_credibility_grade"),
                },
                "reconciliation_error_summary": {
                    "rank_absolute_error": row.get("rank_absolute_error"),
                    "store_count_relative_error": row.get("store_count_relative_error"),
                    "system_sales_relative_error": row.get("system_sales_relative_error"),
                    "auv_relative_error": row.get("auv_relative_error"),
                },
                "invariant_results": invariant_results,
                "open_issues": deduped_issues,
            }
        )
    return scorecards


def _render_brand_scorecards(artifacts: DashboardArtifacts) -> str:
    scorecards = _brand_scorecards(artifacts)
    if not scorecards:
        return '<div class="card">No reconciled brands were found locally.</div>'

    blocks = []
    for scorecard in scorecards[:30]:
        open_issues_text = "\n".join(scorecard["open_issues"]) or "None"
        blocks.append(
            f"""
<details>
  <summary>{html.escape(str(scorecard["brand_name"]))} <span class="note">({html.escape(str(scorecard["canonical_brand_name"] or "unresolved"))})</span></summary>
  <div class="grid" style="margin-top: 12px;">
    <div class="card">
      <h3>Normalized Metrics</h3>
      <pre>{_escape_pre(json.dumps(scorecard["normalized_metrics"], ensure_ascii=False, indent=2, default=str))}</pre>
    </div>
    <div class="card">
      <h3>Provenance Grades</h3>
      <pre>{_escape_pre(json.dumps(scorecard["provenance_grades"], ensure_ascii=False, indent=2, default=str))}</pre>
    </div>
    <div class="card">
      <h3>Reconciliation Error Summary</h3>
      <pre>{_escape_pre(json.dumps(scorecard["reconciliation_error_summary"], ensure_ascii=False, indent=2, default=str))}</pre>
    </div>
    <div class="card">
      <h3>Invariant Results</h3>
      <pre>{_escape_pre(json.dumps(scorecard["invariant_results"], ensure_ascii=False, indent=2, default=str) or "[]")}</pre>
    </div>
    <div class="card">
      <h3>Open Issues</h3>
      <pre>{_escape_pre(open_issues_text)}</pre>
    </div>
  </div>
</details>
"""
        )
    return "\n".join(blocks)


def _warning_counts_by_category(findings: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for finding in findings:
        if finding.get("severity") != "warning":
            continue
        category = str(finding.get("category") or "unknown")
        counts[category] = counts.get(category, 0) + 1
    return dict(sorted(counts.items()))


def _weak_provenance_fields(provenance_registry: pd.DataFrame | None) -> list[str]:
    if not _has_frame(provenance_registry):
        return []
    rows = _frame_to_records(provenance_registry)
    ranked = sorted(
        rows,
        key=lambda row: (
            _safe_float(row.get("confidence_score"), default=0.0),
            str(row.get("source_type") or ""),
            str(row.get("source_name") or ""),
        ),
    )
    top = ranked[:5]
    return [
        f"{row.get('source_type')} / {row.get('source_name')} ({_safe_float(row.get('confidence_score'), default=0.0):.2f})"
        for row in top
    ]


def _largest_reconciliation_errors(
    reconciled_core_metrics: Any | None,
) -> list[str]:
    if not _has_frame(reconciled_core_metrics):
        return []
    candidate_columns = [
        "rank_absolute_error",
        "store_count_absolute_error",
        "system_sales_absolute_error",
        "auv_absolute_error",
    ]
    records = []
    for row in reconciled_core_metrics.to_dict(orient="records"):
        for column in candidate_columns:
            value = row.get(column)
            if _is_missing(value):
                continue
            records.append(f"{row.get('brand_name')} - {column}: {value}")
    return records[:10]


def _syntheticness_overview() -> str:
    parquet_path = Path("data/gold/syntheticness_signals.parquet")
    if parquet_path.exists() and pd is not None:
        try:
            frame = pd.read_parquet(parquet_path)
        except Exception:
            frame = None
        if frame is not None and not frame.empty and "strength" in frame.columns:
            counts = frame["strength"].fillna("unknown").value_counts().to_dict()
            ordered = ", ".join(f"{key}: {int(value)}" for key, value in sorted(counts.items()))
            return f"{len(frame)} signals ({ordered})."
        return "Syntheticness signals are present in Gold parquet."
    report = Path("reports/validation/syntheticness_report.md")
    if report.exists():
        return "Syntheticness report available in Markdown."
    return "No syntheticness outputs found."


def _render_warning_counts(counts: dict[str, int]) -> str:
    if not counts:
        return '<div class="note">No warnings were emitted.</div>'
    rows = [
        "<table><thead><tr><th>Category</th><th>Count</th></tr></thead><tbody>",
    ]
    for category, count in counts.items():
        rows.append(f"<tr><td>{html.escape(category)}</td><td>{count}</td></tr>")
    rows.append("</tbody></table>")
    return "".join(rows)


def _render_simple_list(items: list[str], *, empty_message: str) -> str:
    if not items:
        return f'<div class="note">{html.escape(empty_message)}</div>'
    return "<ul>" + "".join(f"<li>{html.escape(item)}</li>" for item in items) + "</ul>"


def _preview_dataframe(frame: pd.DataFrame | None, max_rows: int = 8) -> str:
    if not _has_frame(frame):
        if pd is None:
            return (
                "Parquet preview unavailable in this environment because pandas is not installed."
            )
        return "No local Gold parquet found."
    preview = frame.head(max_rows).copy()
    return preview.to_string(index=False)


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or path.suffix.lower() != ".json":
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _read_text(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return None


def _read_parquet(path: Path) -> pd.DataFrame | None:
    if pd is None or not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


def _has_frame(frame: Any | None) -> bool:
    if frame is None:
        return False
    empty = getattr(frame, "empty", None)
    return bool(empty is False)


def _frame_to_records(frame: Any) -> list[dict[str, Any]]:
    if hasattr(frame, "to_dict"):
        try:
            return list(frame.to_dict(orient="records"))
        except Exception:
            return []
    return []


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if pd is not None:
        try:
            return bool(pd.isna(value))
        except Exception:
            return False
    try:
        return value != value  # NaN check
    except Exception:
        return False


def _safe_float(value: Any, *, default: float = 0.0) -> float:
    try:
        number = float(value)
    except Exception:
        return default
    return number if number == number else default


def _escape_pre(text: str) -> str:
    return html.escape(text)


if __name__ == "__main__":
    main()
