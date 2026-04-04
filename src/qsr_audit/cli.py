"""CLI entrypoint for qsr-audit-pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from qsr_audit.config import get_settings
from qsr_audit.ingest import ingest_workbook as ingest_workbook_pipeline
from qsr_audit.reconcile import reconcile_core_metrics as reconcile_core_metrics_pipeline
from qsr_audit.reporting import write_reports as write_reports_pipeline
from qsr_audit.validate import run_syntheticness as run_syntheticness_pipeline
from qsr_audit.validate import validate_workbook as validate_workbook_pipeline

app = typer.Typer(name="qsr-audit", help="QSR workbook audit pipeline CLI.")
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
        help="Path to the source workbook.",
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
        help="Path to a raw workbook, Silver directory, or Silver parquet file.",
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


@app.command()
def ingest(
    source: str = typer.Argument(..., help="Path to raw workbook or data source."),
) -> None:
    """Ingest raw data into the Bronze layer."""

    console.print(f"[bold green]Ingest[/bold green] - source: {source} (not yet implemented)")


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
    """Validate data in the specified layer."""

    console.print(f"[bold blue]Validate[/bold blue] - layer: {layer} (not yet implemented)")


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
    input_path: ValidationInputOption,
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
    """Reconcile normalized core metrics against manual reference data."""

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


@app.command()
def report(
    output: ReportOutputOption = Path("reports"),
) -> None:
    """Generate audit reports from Gold-layer data."""

    artifacts = write_reports_pipeline(output_root=output, settings=get_settings())
    console.print(f"[bold yellow]Analyst reports generated[/bold yellow] - {output}")
    console.print(f"Global markdown: {artifacts.global_markdown}")
    console.print(f"Global HTML: {artifacts.global_html}")
    console.print(f"Global JSON: {artifacts.global_json}")
    console.print(f"Brand markdown files: {len(artifacts.brand_markdown_paths)}")


if __name__ == "__main__":
    app()
