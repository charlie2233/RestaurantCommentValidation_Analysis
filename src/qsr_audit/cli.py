"""CLI entrypoint for qsr-audit-pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from qsr_audit.config import get_settings
from qsr_audit.ingest import ingest_workbook as ingest_workbook_pipeline

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


@app.command()
def report(
    output: str = typer.Option("reports/", help="Output directory for generated reports."),
) -> None:
    """Generate audit reports from Gold-layer data."""

    console.print(f"[bold yellow]Report[/bold yellow] - output: {output} (not yet implemented)")


if __name__ == "__main__":
    app()
