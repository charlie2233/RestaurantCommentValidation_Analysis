"""Five-brand end-to-end happy-path demo orchestration."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.config import Settings
from qsr_audit.gold import gate_gold_publish
from qsr_audit.ingest import ingest_workbook
from qsr_audit.reconcile import load_reference_catalog, reconcile_core_metrics
from qsr_audit.reconcile.reconciliation import select_best_reference_row
from qsr_audit.reporting import build_report_bundle, load_report_inputs
from qsr_audit.validate import run_syntheticness, validate_workbook

DEMO_BRANDS: tuple[str, ...] = (
    "Starbucks",
    "Taco Bell",
    "Raising Cane's",
    "Dutch Bros",
    "Shake Shack",
)
DEMO_COMMAND_NAME = "demo-happy-path"
DEMO_WORKSPACE_DIRNAME = "demo_happy_path"
DEMO_REFERENCE_FILENAME = "qsr50_reference.csv"
DELTA_FIELD_SPECS: tuple[tuple[str, str, str], ...] = (
    ("rank", "reference_rank", "rank"),
    ("us_store_count_2024", "reference_us_store_count_2024", "store_count"),
    (
        "systemwide_revenue_usd_billions_2024",
        "reference_systemwide_revenue_usd_billions_2024",
        "system_sales",
    ),
    ("average_unit_volume_usd_thousands", "reference_average_unit_volume_usd_thousands", "auv"),
)


@dataclass(frozen=True)
class DemoArtifacts:
    """Final demo artifact locations."""

    core_scorecard_html_path: Path
    brand_deltas_csv_path: Path
    top_risks_markdown_path: Path
    demo_gold_parquet_path: Path
    demo_syntheticness_parquet_path: Path


@dataclass(frozen=True)
class DemoHappyPathRun:
    """Complete demo run result."""

    artifacts: DemoArtifacts
    demo_gold: pd.DataFrame
    demo_syntheticness: pd.DataFrame
    brand_deltas: pd.DataFrame
    warnings: tuple[str, ...]


def run_demo_happy_path(
    *,
    settings: Settings | None = None,
    input_path: Path | None = None,
    reference_dir: Path | None = None,
) -> DemoHappyPathRun:
    """Run the 5-brand happy-path demo end to end in an isolated workspace."""

    resolved_settings = settings or Settings()
    workbook_path = (input_path or _default_workbook_path(resolved_settings)).expanduser().resolve()
    resolved_reference_dir = (
        (reference_dir or resolved_settings.data_reference).expanduser().resolve()
    )

    workspace_root = resolved_settings.validate_artifact_root(
        resolved_settings.artifacts_dir / DEMO_WORKSPACE_DIRNAME,
        purpose="happy-path demo workspace",
    )
    if workspace_root.exists():
        shutil.rmtree(workspace_root)
    demo_reference_dir = _build_demo_reference_dir(
        source_reference_dir=resolved_reference_dir,
        workspace_root=workspace_root,
    )
    demo_settings = _build_demo_settings(
        base_settings=resolved_settings,
        workspace_root=workspace_root,
        reference_dir=demo_reference_dir,
    )

    ingest_workbook(workbook_path, demo_settings)
    _filter_demo_silver(demo_settings)

    reference_frame, reference_warnings, _ = load_reference_catalog(demo_reference_dir)
    missing_brands = _missing_demo_reference_brands(reference_frame)
    if missing_brands:
        joined = ", ".join(missing_brands)
        raise ValueError(
            "The happy-path demo requires QSR50 coverage for all five demo brands. "
            f"Missing: {joined}."
        )

    validate_workbook(
        demo_settings.data_silver,
        settings=demo_settings,
        output_dir=demo_settings.reports_dir / "validation",
        gold_dir=demo_settings.data_gold,
    )
    run_syntheticness(
        demo_settings.data_silver / "core_brand_metrics.parquet",
        settings=demo_settings,
        output_dir=demo_settings.reports_dir / "validation",
        gold_dir=demo_settings.data_gold,
    )
    reconciliation_run = reconcile_core_metrics(
        core_path=demo_settings.data_silver / "core_brand_metrics.parquet",
        reference_dir=demo_reference_dir,
        settings=demo_settings,
        gold_dir=demo_settings.data_gold,
        report_dir=demo_settings.reports_dir / "reconciliation",
    )
    gold_run = gate_gold_publish(
        settings=demo_settings,
        gold_dir=demo_settings.data_gold,
        report_dir=demo_settings.reports_dir / "audit",
    )

    report_bundle = build_report_bundle(load_report_inputs(demo_settings))
    demo_syntheticness = _build_demo_syntheticness(report_bundle)
    demo_gold = _build_demo_gold(
        decisions=gold_run.decisions,
        reconciled_core_metrics=reconciliation_run.reconciled_core_metrics,
        syntheticness=demo_syntheticness,
    )
    brand_deltas = _build_brand_deltas(
        reconciled_core_metrics=reconciliation_run.reconciled_core_metrics,
        reference_frame=reference_frame,
        demo_gold=demo_gold,
    )

    artifacts = _write_demo_outputs(
        settings=resolved_settings,
        demo_gold=demo_gold,
        demo_syntheticness=demo_syntheticness,
        brand_deltas=brand_deltas,
        report_bundle=report_bundle,
    )

    return DemoHappyPathRun(
        artifacts=artifacts,
        demo_gold=demo_gold,
        demo_syntheticness=demo_syntheticness,
        brand_deltas=brand_deltas,
        warnings=tuple(reference_warnings) + reconciliation_run.warnings,
    )


def _default_workbook_path(settings: Settings) -> Path:
    workbooks = _supported_workbook_paths(settings.data_raw)
    if len(workbooks) != 1:
        workbook_list = ", ".join(path.name for path in workbooks) or "none found"
        raise FileNotFoundError(
            "demo-happy-path requires exactly one workbook under `data/raw/` when "
            f"`--input` is not provided. Found: {workbook_list}. "
            "Pass `--input <workbook>` explicitly when multiple workbook files exist."
        )
    return workbooks[0]


def _supported_workbook_paths(raw_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in raw_dir.iterdir()
        if path.is_file() and path.suffix.lower() in {".xlsx", ".xlsm", ".xls"}
    )


def _build_demo_settings(
    *,
    base_settings: Settings,
    workspace_root: Path,
    reference_dir: Path,
) -> Settings:
    return Settings(
        data_raw=workspace_root / "raw",
        data_bronze=workspace_root / "bronze",
        data_silver=workspace_root / "silver",
        data_gold=workspace_root / "gold",
        data_reference=reference_dir,
        gold_history_dir=workspace_root / "gold" / "history",
        reports_dir=workspace_root / "reports",
        strategy_dir=workspace_root / "strategy",
        artifacts_dir=workspace_root / "artifacts",
        log_level=base_settings.log_level,
    )


def _build_demo_reference_dir(*, source_reference_dir: Path, workspace_root: Path) -> Path:
    demo_reference_dir = workspace_root / "reference"
    demo_reference_dir.mkdir(parents=True, exist_ok=True)

    source_path = source_reference_dir / DEMO_REFERENCE_FILENAME
    template_path = source_reference_dir / "templates" / DEMO_REFERENCE_FILENAME
    if source_path.exists():
        shutil.copy2(source_path, demo_reference_dir / DEMO_REFERENCE_FILENAME)
        return demo_reference_dir
    if template_path.exists():
        (demo_reference_dir / "templates").mkdir(parents=True, exist_ok=True)
        shutil.copy2(template_path, demo_reference_dir / "templates" / DEMO_REFERENCE_FILENAME)
    return demo_reference_dir


def _filter_demo_silver(settings: Settings) -> None:
    brand_set = set(DEMO_BRANDS)
    core_path = settings.data_silver / "core_brand_metrics.parquet"
    ai_path = settings.data_silver / "ai_strategy_registry.parquet"

    core_frame = pd.read_parquet(core_path)
    ai_frame = pd.read_parquet(ai_path)
    filtered_core = core_frame.loc[core_frame["brand_name"].isin(brand_set)].reset_index(drop=True)
    filtered_ai = ai_frame.loc[ai_frame["brand_name"].isin(brand_set)].reset_index(drop=True)

    _require_demo_brand_coverage(filtered_core["brand_name"], "core_brand_metrics")
    _require_demo_brand_coverage(filtered_ai["brand_name"], "ai_strategy_registry")

    filtered_core.to_parquet(core_path, index=False)
    filtered_ai.to_parquet(ai_path, index=False)


def _require_demo_brand_coverage(values: pd.Series, dataset_name: str) -> None:
    found = {str(value) for value in values.dropna().astype(str)}
    missing = sorted(set(DEMO_BRANDS) - found)
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"{dataset_name} is missing required demo brands: {joined}.")


def _missing_demo_reference_brands(reference_frame: pd.DataFrame) -> list[str]:
    qsr50_rows = reference_frame.loc[
        reference_frame["source_file_name"].eq("qsr50_reference.csv")
        & reference_frame["canonical_brand_name"].isin(DEMO_BRANDS)
    ]
    covered = {str(value) for value in qsr50_rows["canonical_brand_name"].dropna().astype(str)}
    return sorted(set(DEMO_BRANDS) - covered)


def _build_demo_syntheticness(report_bundle) -> pd.DataFrame:
    rows = []
    for scorecard in report_bundle.brand_scorecards:
        rows.append(
            {
                "brand_name": scorecard.brand_name,
                "canonical_brand_name": scorecard.canonical_brand_name,
                "syntheticness_score": int(scorecard.syntheticness_score),
                "supporting_signals": json.dumps(
                    scorecard.supporting_signals, ensure_ascii=False, default=str
                ),
                "review_required": bool(scorecard.review_required),
                "caveats": json.dumps(scorecard.caveats, ensure_ascii=False, default=str),
            }
        )
    return pd.DataFrame(rows).sort_values(
        by="canonical_brand_name",
        kind="stable",
        ignore_index=True,
    )


def _build_demo_gold(
    *,
    decisions: pd.DataFrame,
    reconciled_core_metrics: pd.DataFrame,
    syntheticness: pd.DataFrame,
) -> pd.DataFrame:
    provenance_lookup = reconciled_core_metrics[
        [
            "canonical_brand_name",
            "overall_credibility_grade",
            "provenance_completeness_score",
            "provenance_completeness_summary",
            "provenance_confidence_summary",
        ]
    ].copy()
    provenance_lookup["provenance_grade"] = provenance_lookup["provenance_completeness_score"].map(
        _score_to_grade
    )

    brand_recommendations = _brand_publish_recommendations(decisions)
    frame = (
        decisions.merge(
            provenance_lookup,
            on="canonical_brand_name",
            how="left",
        )
        .merge(
            syntheticness,
            on=["brand_name", "canonical_brand_name"],
            how="left",
        )
        .merge(
            brand_recommendations,
            on="canonical_brand_name",
            how="left",
        )
    )

    for column in ("blocking_reasons", "warning_reasons", "validation_references"):
        if column in frame.columns:
            frame[column] = frame[column].map(
                lambda value: json.dumps(value, ensure_ascii=False, default=str)
            )
    return frame.sort_values(
        by=["canonical_brand_name", "metric_name"],
        kind="stable",
        ignore_index=True,
    )


def _brand_publish_recommendations(decisions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for canonical_brand_name, frame in decisions.groupby("canonical_brand_name", sort=True):
        counts = frame["publish_status"].value_counts().to_dict()
        publishable = int(counts.get("publishable", 0))
        advisory = int(counts.get("advisory", 0))
        blocked = int(counts.get("blocked", 0))
        if blocked and publishable:
            recommendation = "publishable_subset_only"
        elif blocked:
            recommendation = "blocked_for_external_use"
        elif advisory and publishable:
            recommendation = "publishable_subset_only"
        elif advisory:
            recommendation = "advisory_only"
        else:
            recommendation = "publishable"
        rows.append(
            {
                "canonical_brand_name": canonical_brand_name,
                "brand_publish_status_recommendation": recommendation,
                "publishable_metric_count": publishable,
                "advisory_metric_count": advisory,
                "blocked_metric_count": blocked,
            }
        )
    return pd.DataFrame(rows)


def _build_brand_deltas(
    *,
    reconciled_core_metrics: pd.DataFrame,
    reference_frame: pd.DataFrame,
    demo_gold: pd.DataFrame,
) -> pd.DataFrame:
    decision_lookup = demo_gold.set_index(["canonical_brand_name", "metric_name"])[
        "publish_status"
    ].to_dict()
    recommendation_lookup = (
        demo_gold.drop_duplicates("canonical_brand_name")
        .set_index("canonical_brand_name")["brand_publish_status_recommendation"]
        .to_dict()
    )

    rows: list[dict[str, Any]] = []
    for record in reconciled_core_metrics.to_dict(orient="records"):
        canonical_name = str(record["canonical_brand_name"])
        matched_refs = reference_frame.loc[
            reference_frame["canonical_brand_name"].eq(canonical_name)
        ].copy()
        for workbook_column, reference_column, metric_name in DELTA_FIELD_SPECS:
            best_reference = select_best_reference_row(matched_refs, field_name=reference_column)
            prefix = metric_name
            rows.append(
                {
                    "brand_name": record["brand_name"],
                    "canonical_brand_name": canonical_name,
                    "metric_name": metric_name,
                    "workbook_value": record.get(workbook_column),
                    "reference_value": record.get(f"{prefix}_reference_value"),
                    "absolute_error": record.get(f"{prefix}_absolute_error"),
                    "relative_error": record.get(f"{prefix}_relative_error"),
                    "credibility_grade": record.get(f"{prefix}_credibility_grade"),
                    "source_type": None
                    if best_reference is None
                    else best_reference.get("source_type"),
                    "source_name": None
                    if best_reference is None
                    else best_reference.get("source_name"),
                    "source_url_or_doc_id": None
                    if best_reference is None
                    else best_reference.get("source_url_or_doc_id"),
                    "as_of_date": None
                    if best_reference is None
                    else best_reference.get("as_of_date"),
                    "method_reported_or_estimated": None
                    if best_reference is None
                    else best_reference.get("method_reported_or_estimated"),
                    "confidence_score": None
                    if best_reference is None
                    else best_reference.get("confidence_score"),
                    "publish_status": decision_lookup.get((canonical_name, metric_name)),
                    "brand_publish_status_recommendation": recommendation_lookup.get(
                        canonical_name
                    ),
                }
            )
    return pd.DataFrame(rows).sort_values(
        by=["canonical_brand_name", "metric_name"],
        kind="stable",
        ignore_index=True,
    )


def _write_demo_outputs(
    *,
    settings: Settings,
    demo_gold: pd.DataFrame,
    demo_syntheticness: pd.DataFrame,
    brand_deltas: pd.DataFrame,
    report_bundle,
) -> DemoArtifacts:
    validation_dir = settings.reports_dir / "validation"
    reconciliation_dir = settings.reports_dir / "reconciliation"
    summary_dir = settings.reports_dir / "summary"
    validation_dir.mkdir(parents=True, exist_ok=True)
    reconciliation_dir.mkdir(parents=True, exist_ok=True)
    summary_dir.mkdir(parents=True, exist_ok=True)
    settings.data_gold.mkdir(parents=True, exist_ok=True)

    artifacts = DemoArtifacts(
        core_scorecard_html_path=validation_dir / "core_scorecard.html",
        brand_deltas_csv_path=reconciliation_dir / "brand_deltas.csv",
        top_risks_markdown_path=summary_dir / "top_risks.md",
        demo_gold_parquet_path=settings.data_gold / "demo_gold.parquet",
        demo_syntheticness_parquet_path=settings.data_gold / "demo_syntheticness.parquet",
    )

    demo_gold.to_parquet(artifacts.demo_gold_parquet_path, index=False)
    demo_syntheticness.to_parquet(artifacts.demo_syntheticness_parquet_path, index=False)
    brand_deltas.to_csv(artifacts.brand_deltas_csv_path, index=False, encoding="utf-8")
    artifacts.top_risks_markdown_path.write_text(
        _render_top_risks(
            report_bundle=report_bundle, demo_gold=demo_gold, brand_deltas=brand_deltas
        ),
        encoding="utf-8",
    )
    artifacts.core_scorecard_html_path.write_text(
        _render_core_scorecard_html(
            report_bundle=report_bundle,
            demo_gold=demo_gold,
            brand_deltas=brand_deltas,
        ),
        encoding="utf-8",
    )
    return artifacts


def _render_top_risks(*, report_bundle, demo_gold: pd.DataFrame, brand_deltas: pd.DataFrame) -> str:
    blocked = demo_gold.loc[demo_gold["publish_status"].eq("blocked")]
    advisory = demo_gold.loc[demo_gold["publish_status"].eq("advisory")]
    delta_frame = brand_deltas.copy()
    delta_frame["abs_relative_error"] = pd.to_numeric(
        delta_frame["relative_error"], errors="coerce"
    ).abs()
    delta_frame = delta_frame.sort_values(
        by="abs_relative_error",
        ascending=False,
        kind="stable",
        na_position="last",
    )

    lines = [
        "# Top Risks",
        "",
        f"- Demo brands: `{len(report_bundle.brand_scorecards)}`",
        f"- Blocked KPI rows: `{len(blocked)}`",
        f"- Advisory KPI rows: `{len(advisory)}`",
        f"- Brands requiring syntheticness review: `{sum(1 for brand in report_bundle.brand_scorecards if brand.review_required)}`",
        "",
        "## Invariant Failures",
        "",
    ]

    invariant_failures = 0
    for brand in report_bundle.brand_scorecards:
        failed = [row for row in brand.invariant_results if row["status"] in {"failed", "warning"}]
        if not failed:
            continue
        invariant_failures += len(failed)
        messages = "; ".join(str(row["message"]) for row in failed)
        lines.append(f"- {brand.brand_name}: {messages}")
    if invariant_failures == 0:
        lines.append("- None.")

    lines.extend(["", "## Largest Reconciliation Deltas", ""])
    top_deltas = delta_frame.head(5).to_dict(orient="records")
    if not top_deltas:
        lines.append("- None.")
    else:
        for row in top_deltas:
            relative_error = row.get("relative_error")
            if relative_error is None or pd.isna(relative_error):
                delta_text = "missing reference delta"
            else:
                delta_text = f"{float(relative_error):.1%} delta"
            lines.append(
                f"- {row['brand_name']} {row['metric_name']}: workbook `{row['workbook_value']}` vs "
                f"reference `{row['reference_value']}` ({delta_text}; status `{row['publish_status']}`)"
            )

    lines.extend(["", "## Publishability Risks", ""])
    if blocked.empty:
        lines.append("- No blocked KPI rows in the five-brand slice.")
    else:
        for brand_name, frame in blocked.groupby("brand_name", sort=True):
            metrics = ", ".join(frame["metric_name"].tolist())
            lines.append(f"- {brand_name}: blocked metrics `{metrics}`")

    return "\n".join(lines) + "\n"


def _render_core_scorecard_html(
    *, report_bundle, demo_gold: pd.DataFrame, brand_deltas: pd.DataFrame
) -> str:
    decision_summary = demo_gold.groupby("canonical_brand_name", sort=True).agg(
        brand_publish_status_recommendation=("brand_publish_status_recommendation", "first"),
        publishable_metric_count=("publishable_metric_count", "first"),
        advisory_metric_count=("advisory_metric_count", "first"),
        blocked_metric_count=("blocked_metric_count", "first"),
        provenance_grade=("provenance_grade", "first"),
    )
    worst_delta = (
        brand_deltas.assign(
            abs_relative_error=pd.to_numeric(brand_deltas["relative_error"], errors="coerce").abs()
        )
        .sort_values(
            by=["canonical_brand_name", "abs_relative_error"],
            ascending=[True, False],
            kind="stable",
        )
        .groupby("canonical_brand_name", sort=True)
        .head(1)
        .set_index("canonical_brand_name")
    )

    rows: list[str] = []
    for scorecard in report_bundle.brand_scorecards:
        summary = decision_summary.loc[scorecard.canonical_brand_name]
        delta_row = worst_delta.loc[scorecard.canonical_brand_name]
        invariant_failures = [
            row for row in scorecard.invariant_results if row["status"] in {"failed", "warning"}
        ]
        invariant_text = (
            "<br>".join(_html_escape(str(row["message"])) for row in invariant_failures)
            if invariant_failures
            else "None"
        )
        delta_text = (
            "n/a"
            if pd.isna(delta_row["relative_error"])
            else f"{delta_row['metric_name']} ({float(delta_row['relative_error']):.1%})"
        )
        rows.append(
            "<tr>"
            f"<td><strong>{_html_escape(scorecard.brand_name)}</strong></td>"
            f"<td>{_html_escape(str(summary['brand_publish_status_recommendation']))}</td>"
            f"<td>{int(summary['publishable_metric_count'])}/{int(summary['advisory_metric_count'])}/{int(summary['blocked_metric_count'])}</td>"
            f"<td>{_html_escape(str(summary['provenance_grade']))}</td>"
            f"<td>{_html_escape(str(scorecard.overall_credibility_grade))}</td>"
            f"<td>{scorecard.syntheticness_score}</td>"
            f"<td>{'yes' if scorecard.review_required else 'no'}</td>"
            f"<td>{_html_escape(delta_text)}</td>"
            f"<td>{invariant_text}</td>"
            "</tr>"
        )

    return (
        "<!DOCTYPE html>\n"
        '<html lang="en">\n'
        "<head>\n"
        '<meta charset="utf-8">\n'
        "<title>Five-Brand Happy-Path Demo Scorecard</title>\n"
        "<style>"
        "body{font-family:Arial,sans-serif;margin:24px;color:#16202a;background:#f7f9fb;}"
        "h1{margin-bottom:8px;}table{border-collapse:collapse;width:100%;background:#fff;}"
        "th,td{border:1px solid #d9e2ec;padding:10px;vertical-align:top;text-align:left;}"
        "th{background:#eef3f8;} .meta{margin:0 0 16px 0;color:#51606d;} code{background:#eef3f8;padding:2px 4px;}"
        "</style>\n"
        "</head>\n"
        "<body>\n"
        "<h1>Five-Brand Happy-Path Demo</h1>\n"
        '<p class="meta">Rows show publishability, reconciliation, provenance, and syntheticness for '
        f"<code>{len(report_bundle.brand_scorecards)}</code> manually referenced brands.</p>\n"
        "<table>\n"
        "<thead><tr>"
        "<th>Brand</th><th>Publish recommendation</th><th>P/A/B metrics</th><th>Provenance grade</th>"
        "<th>Reconciliation grade</th><th>Syntheticness score</th><th>Review required</th>"
        "<th>Worst delta</th><th>Invariant failures</th>"
        "</tr></thead>\n"
        "<tbody>\n" + "\n".join(rows) + "\n</tbody>\n</table>\n</body>\n</html>\n"
    )


def _score_to_grade(value: Any) -> str:
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


def _html_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    )


__all__ = [
    "DEMO_BRANDS",
    "DEMO_COMMAND_NAME",
    "DemoArtifacts",
    "DemoHappyPathRun",
    "run_demo_happy_path",
]
