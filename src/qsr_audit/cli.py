"""CLI entrypoint for qsr-audit-pipeline."""

import typer
from rich.console import Console

app = typer.Typer(name="qsr-audit", help="QSR workbook audit pipeline CLI.")
console = Console()


@app.command()
def ingest(
    source: str = typer.Argument(..., help="Path to raw workbook or data source."),
) -> None:
    """Ingest raw data into the Bronze layer."""
    console.print(f"[bold green]Ingest[/bold green] – source: {source} (not yet implemented)")


@app.command()
def validate(
    layer: str = typer.Option("silver", help="Data layer to validate: bronze | silver | gold."),
) -> None:
    """Validate data in the specified layer."""
    console.print(f"[bold blue]Validate[/bold blue] – layer: {layer} (not yet implemented)")


@app.command()
def report(
    output: str = typer.Option("reports/", help="Output directory for generated reports."),
) -> None:
    """Generate audit reports from Gold-layer data."""
    console.print(f"[bold yellow]Report[/bold yellow] – output: {output} (not yet implemented)")


if __name__ == "__main__":
    app()
