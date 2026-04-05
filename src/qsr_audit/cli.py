"""CLI entrypoint for qsr-audit-pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from qsr_audit.config import get_settings
from qsr_audit.forecasting import build_forecast_panel as build_forecast_panel_pipeline
from qsr_audit.forecasting import forecast_baselines as forecast_baselines_pipeline
from qsr_audit.forecasting import snapshot_gold_history as snapshot_gold_history_pipeline
from qsr_audit.gold import gate_gold_publish as gate_gold_publish_pipeline
from qsr_audit.ingest import ingest_workbook as ingest_workbook_pipeline
from qsr_audit.reconcile import audit_reference_coverage as audit_reference_coverage_pipeline
from qsr_audit.reconcile import reconcile_core_metrics as reconcile_core_metrics_pipeline
from qsr_audit.reporting import write_reports as write_reports_pipeline
from qsr_audit.strategy import generate_strategy_outputs as generate_strategy_outputs_pipeline
from qsr_audit.validate import run_syntheticness as run_syntheticness_pipeline
from qsr_audit.validate import validate_workbook as validate_workbook_pipeline

app = typer.Typer(
    name="qsr-audit",
    help=(
        "QSR workbook audit pipeline CLI for ingesting workbook claims, validating them, "
        "reconciling Gold outputs, and producing analyst-facing reports."
    ),
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)
console = Console()

InputWorkbookOption = Annotated[
    Path,
    typer.Option(
        ...,
        "--input",
        exists=True,
        dir_okay=False,
        readable=True,
        path_type=Path,
        help="Path to the source workbook that should be copied into the raw/Bronze workflow.",
    ),
]

ValidationInputOption = Annotated[
    Path,
    typer.Option(
        ...,
        "--input",
        exists=True,
        file_okay=True,
        dir_okay=True,
        readable=True,
        path_type=Path,
        help="Path to a raw workbook, a Silver directory, or a Silver parquet file.",
    ),
]

SyntheticnessInputOption = Annotated[
    Path,
    typer.Option(
        ...,
        "--input",
        exists=True,
        file_okay=True,
        dir_okay=True,
        readable=True,
        path_type=Path,
        help="Path to a raw workbook, a Silver directory, or core_brand_metrics.parquet.",
    ),
]

CoreMetricsOption = Annotated[
    Path,
    typer.Option(
        ...,
        "--core",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        path_type=Path,
        help="Path to the normalized core_brand_metrics parquet file.",
    ),
]

ReferenceDirOption = Annotated[
    Path,
    typer.Option(
        ...,
        "--reference-dir",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        path_type=Path,
        help="Directory containing manual reference CSV files and templates.",
    ),
]

ReportOutputOption = Annotated[
    Path,
    typer.Option(
        "--output",
        file_okay=False,
        dir_okay=True,
        path_type=Path,
        help="Directory where analyst-facing reports should be written.",
    ),
]

MetricNameOption = Annotated[
    str,
    typer.Option(
        ...,
        "--metric",
        help="Metric name from Gold publish decisions, such as store_count, system_sales, or auv.",
    ),
]

SnapshotDateOption = Annotated[
    str,
    typer.Option(
        ...,
        "--as-of-date",
        help="Snapshot date in ISO format (YYYY-MM-DD).",
    ),
]

ExperimentOutputOption = Annotated[
    Path | None,
    typer.Option(
        "--output-root",
        file_okay=False,
        dir_okay=True,
        path_type=Path,
        help=(
            "Directory for non-analyst-facing experiment artifacts. Defaults to "
            "`artifacts/forecasting/<metric>`."
        ),
    ),
]


@app.command()
def ingest(
    source: str = typer.Argument(..., help="Path to raw workbook or data source."),
) -> None:
    """Legacy placeholder for generic ingest flows. Use `ingest-workbook` for Excel workbooks."""

    console.print(
        "[bold yellow]Legacy placeholder[/bold yellow] - use "
        "`qsr-audit ingest-workbook --input <workbook.xlsx>` for workbook inputs. "
        f"Received source: {source}"
    )


@app.command("ingest-workbook")
def ingest_workbook_command(
    input_path: InputWorkbookOption,
) -> None:
    """Ingest the workbook and emit Bronze and Silver artifacts."""

    result = ingest_workbook_pipeline(input_path, get_settings())
    console.print(f"[bold green]Workbook ingested[/bold green] - {input_path}")
    console.print(f"Bronze workbook copy: {result.workbook_copy_path}")
    for sheet_name, sheet_artifacts in result.bronze_sheet_artifacts.items():
        console.print(f"Bronze raw dump ({sheet_name}): {sheet_artifacts.parquet_path}")
        console.print(f"Bronze raw csv ({sheet_name}): {sheet_artifacts.csv_path}")
    console.print(f"Silver core metrics: {result.silver_artifacts.core_brand_metrics_path}")
    console.print(f"Silver AI registry: {result.silver_artifacts.ai_strategy_registry_path}")
    console.print(f"Silver data notes: {result.silver_artifacts.data_notes_path}")
    console.print(f"Silver key findings: {result.silver_artifacts.key_findings_path}")


@app.command()
def validate(
    layer: str = typer.Option("silver", help="Data layer to validate: bronze | silver | gold."),
) -> None:
    """Legacy placeholder for generic layer validation. Use `validate-workbook` for workbook-derived data."""

    console.print(
        "[bold yellow]Legacy placeholder[/bold yellow] - use "
        "`qsr-audit validate-workbook --input <raw workbook|silver path>` for workbook-derived data. "
        f"Received layer: {layer}"
    )


@app.command("validate-workbook")
def validate_workbook_command(
    input_path: ValidationInputOption,
    tolerance_auv: float = typer.Option(
        0.05,
        "--tolerance-auv",
        min=0.0,
        help="Maximum allowed relative delta between recorded and implied AUV.",
    ),
) -> None:
    """Validate normalized workbook tables from a raw workbook or Silver path."""

    run = validate_workbook_pipeline(
        input_path=input_path,
        settings=get_settings(),
        tolerance_auv=tolerance_auv,
    )
    counts = run.counts
    status_label = "passed" if run.passed else "failed"
    status_color = "green" if run.passed else "red"

    console.print(
        f"[bold {status_color}]Validation {status_label}[/bold {status_color}] - {input_path}"
    )
    console.print(f"Errors: {counts['error']}")
    console.print(f"Warnings: {counts['warning']}")
    console.print(f"Info: {counts['info']}")
    if run.artifacts is not None:
        console.print(f"Summary: {run.artifacts.summary_markdown}")
        console.print(f"Results JSON: {run.artifacts.results_json}")
        console.print(f"Validation flags: {run.artifacts.flags_parquet}")

    if not run.passed:
        raise typer.Exit(code=1)


@app.command("run-syntheticness")
def run_syntheticness_command(
    input_path: SyntheticnessInputOption,
    include_isolation_forest: bool = typer.Option(
        True,
        "--include-isolation-forest/--skip-isolation-forest",
        help="Whether to run the optional Isolation Forest multivariate model.",
    ),
) -> None:
    """Run syntheticness diagnostics on normalized core brand metrics."""

    run = run_syntheticness_pipeline(
        input_path=input_path,
        settings=get_settings(),
        include_isolation_forest=include_isolation_forest,
    )
    counts = run.counts

    console.print(f"[bold magenta]Syntheticness analysis complete[/bold magenta] - {input_path}")
    console.print(f"Strong signals: {counts['strong']}")
    console.print(f"Moderate signals: {counts['moderate']}")
    console.print(f"Weak signals: {counts['weak']}")
    console.print(f"Unknown / skipped: {counts['unknown']}")
    console.print(f"Report: {run.artifacts.report_markdown}")
    console.print(f"Signals parquet: {run.artifacts.signals_parquet}")


@app.command("reconcile")
def reconcile_command(
    core_path: CoreMetricsOption,
    reference_dir: ReferenceDirOption,
) -> None:
    """Reconcile normalized core metrics and emit reference coverage artifacts."""

    run = reconcile_core_metrics_pipeline(
        core_path=core_path,
        reference_dir=reference_dir,
        settings=get_settings(),
    )
    console.print(f"[bold cyan]Reconciliation complete[/bold cyan] - {core_path}")
    console.print(f"Warnings: {len(run.warnings)}")
    console.print(f"Reconciled core metrics: {run.artifacts.reconciled_core_metrics_path}")
    console.print(f"Provenance registry: {run.artifacts.provenance_registry_path}")
    console.print(f"Summary: {run.artifacts.reconciliation_summary_path}")
    console.print(f"Reference coverage parquet: {run.artifacts.reference_coverage_parquet_path}")
    console.print(f"Reference coverage markdown: {run.artifacts.reference_coverage_markdown_path}")


@app.command("audit-reference")
def audit_reference_command(
    core_path: CoreMetricsOption,
    reference_dir: ReferenceDirOption,
) -> None:
    """Audit manual reference coverage and emit Gold/report coverage artifacts."""

    run = audit_reference_coverage_pipeline(
        core_path=core_path,
        reference_dir=reference_dir,
        settings=get_settings(),
    )
    console.print(f"[bold cyan]Reference coverage audit complete[/bold cyan] - {core_path}")
    console.print(f"Warnings: {len(run.warnings)}")
    console.print(f"Coverage parquet: {run.artifacts.coverage_parquet_path}")
    console.print(f"Coverage markdown: {run.artifacts.coverage_markdown_path}")


@app.command("gate-gold")
def gate_gold_command() -> None:
    """Evaluate Gold publishing gates and emit KPI export decisions plus an audit scorecard."""

    run = gate_gold_publish_pipeline(settings=get_settings())
    console.print("[bold green]Gold publishing gates complete[/bold green]")
    console.print(f"Policy: {run.policy_id} {run.policy_version}")
    console.print(f"Decision rows: {len(run.decisions.index)}")
    console.print(f"Publishable KPIs: {run.summary['publishable_count']}")
    console.print(f"Advisory KPIs: {run.summary['advisory_count']}")
    console.print(f"Blocked KPIs: {run.summary['blocked_count']}")
    console.print(f"Gold decisions: {run.artifacts.decisions_path}")
    console.print(f"Publishable parquet: {run.artifacts.publishable_path}")
    console.print(f"Blocked parquet: {run.artifacts.blocked_path}")
    console.print(f"Scorecard: {run.artifacts.scorecard_markdown_path}")
    console.print(f"Summary JSON: {run.artifacts.summary_json_path}")


@app.command("snapshot-gold")
def snapshot_gold_command(
    as_of_date: SnapshotDateOption,
    include_advisory: bool = typer.Option(
        False,
        "--include-advisory",
        help="Include advisory rows in the snapshot. Blocked rows remain excluded.",
    ),
) -> None:
    """Snapshot current Gold publish outputs into dated history for future forecasting."""

    try:
        run = snapshot_gold_history_pipeline(
            as_of_date=as_of_date,
            settings=get_settings(),
            include_advisory=include_advisory,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    console.print(f"[bold blue]Gold snapshot captured[/bold blue] - {run.as_of_date}")
    console.print(f"Included statuses: {', '.join(run.included_statuses)}")
    console.print(f"Snapshot rows: {run.row_count}")
    console.print(f"Snapshot parquet: {run.artifacts.snapshot_rows_path}")
    console.print(f"Snapshot manifest: {run.artifacts.manifest_path}")
    console.print(f"History index: {run.artifacts.index_path}")


@app.command("build-forecast-panel")
def build_forecast_panel_command(
    metric_name: MetricNameOption,
    output_root: ExperimentOutputOption = None,
    include_advisory: bool = typer.Option(
        False,
        "--include-advisory",
        help="Include advisory snapshot rows in the panel. Blocked rows remain excluded.",
    ),
    allow_short_history: bool = typer.Option(
        False,
        "--allow-short-history",
        help="Testing or local scaffolding override for very short history panels.",
    ),
) -> None:
    """Assemble a forecast-ready longitudinal panel from Gold snapshot history."""

    try:
        run = build_forecast_panel_pipeline(
            metric_name=metric_name,
            settings=get_settings(),
            output_root=output_root,
            include_advisory=include_advisory,
            allow_short_history=allow_short_history,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    console.print(f"[bold blue]Forecast panel built[/bold blue] - {metric_name}")
    console.print(f"Periods: {run.summary['period_count']}")
    console.print(f"Brands: {run.summary['brand_count']}")
    console.print(f"Rows: {run.summary['row_count']}")
    console.print(f"Panel parquet: {run.artifacts.panel_parquet_path}")
    console.print(f"Panel metadata: {run.artifacts.metadata_json_path}")
    console.print(f"Panel summary: {run.artifacts.summary_markdown_path}")


@app.command("forecast-baseline")
def forecast_baseline_command(
    metric_name: MetricNameOption,
    output_root: ExperimentOutputOption = None,
    include_advisory: bool = typer.Option(
        False,
        "--include-advisory",
        help="Include advisory snapshot rows in the experiment panel. Blocked rows remain excluded.",
    ),
    allow_short_history: bool = typer.Option(
        False,
        "--allow-short-history",
        help="Testing or local scaffolding override for very short history panels.",
    ),
    holdout_periods: int = typer.Option(
        1,
        "--holdout-periods",
        min=1,
        help="Number of trailing holdout periods per brand for evaluation.",
    ),
    season_length: int | None = typer.Option(
        None,
        "--season-length",
        min=2,
        help="Optional seasonal length for seasonal naive evaluation when cadence supports it.",
    ),
) -> None:
    """Run leakage-safe offline forecast baselines, including seasonal naive when supported."""

    try:
        run = forecast_baselines_pipeline(
            metric_name=metric_name,
            settings=get_settings(),
            output_root=output_root,
            include_advisory=include_advisory,
            allow_short_history=allow_short_history,
            holdout_periods=holdout_periods,
            season_length=season_length,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    console.print(f"[bold blue]Forecast baseline evaluation complete[/bold blue] - {metric_name}")
    console.print(f"Panel parquet: {run.artifacts.panel_parquet_path}")
    console.print(f"Split metadata: {run.artifacts.split_metadata_json_path}")
    console.print(f"Metrics JSON: {run.artifacts.metrics_json_path}")
    console.print(f"Metrics CSV: {run.artifacts.metrics_csv_path}")
    console.print(f"Summary: {run.artifacts.summary_markdown_path}")


@app.command()
def report(
    output: ReportOutputOption = Path("reports"),
) -> None:
    """Generate Markdown/HTML/JSON audit reports and Gold-derived strategy outputs."""

    settings = get_settings()
    artifacts = write_reports_pipeline(output_root=output, settings=settings)
    strategy_run = generate_strategy_outputs_pipeline(
        settings=settings,
        report_dir=output / "strategy",
    )
    console.print(f"[bold yellow]Analyst reports generated[/bold yellow] - {output}")
    console.print(f"Global markdown: {artifacts.global_markdown}")
    console.print(f"Global HTML: {artifacts.global_html}")
    console.print(f"Global JSON: {artifacts.global_json}")
    console.print(f"Brand markdown files: {len(artifacts.brand_markdown_paths)}")
    console.print(
        f"Strategy recommendations parquet: {strategy_run.artifacts.recommendations_parquet_path}"
    )
    console.print(
        f"Strategy recommendations JSON: {strategy_run.artifacts.recommendations_json_path}"
    )
    console.print(f"Strategy playbook: {strategy_run.artifacts.playbook_markdown_path}")


if __name__ == "__main__":
    app()
