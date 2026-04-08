"""CLI entrypoint for qsr-audit-pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from qsr_audit.config import get_settings
from qsr_audit.demo import run_demo_happy_path as run_demo_happy_path_pipeline
from qsr_audit.demo_showcase import package_demo_bundle as package_demo_bundle_pipeline
from qsr_audit.forecasting import build_forecast_panel as build_forecast_panel_pipeline
from qsr_audit.forecasting import forecast_baselines as forecast_baselines_pipeline
from qsr_audit.forecasting import snapshot_gold_history as snapshot_gold_history_pipeline
from qsr_audit.gold import gate_gold_publish as gate_gold_publish_pipeline
from qsr_audit.governance import (
    DataClassification,
    begin_command_audit,
    latest_manifest_path,
    write_artifact_manifest,
    write_command_audit_log,
)
from qsr_audit.ingest import ingest_workbook as ingest_workbook_pipeline
from qsr_audit.rag import adjudicate_rag_benchmark as adjudicate_rag_benchmark_pipeline
from qsr_audit.rag import available_reranker_names, resolve_rag_corpus_path
from qsr_audit.rag import bootstrap_rag_judgments as bootstrap_rag_judgments_pipeline
from qsr_audit.rag import build_rag_corpus as build_rag_corpus_pipeline
from qsr_audit.rag import eval_rag_retrieval as eval_rag_retrieval_pipeline
from qsr_audit.rag import init_rag_benchmark as init_rag_benchmark_pipeline
from qsr_audit.rag import inspect_rag_benchmark_query as inspect_rag_benchmark_query_pipeline
from qsr_audit.rag import mine_rag_hard_negatives as mine_rag_hard_negatives_pipeline
from qsr_audit.rag import rag_search as rag_search_pipeline
from qsr_audit.rag import seed_rag_queries as seed_rag_queries_pipeline
from qsr_audit.rag import (
    summarize_rag_benchmark_authoring as summarize_rag_benchmark_authoring_pipeline,
)
from qsr_audit.rag import summarize_rag_failures as summarize_rag_failures_pipeline
from qsr_audit.rag import validate_rag_benchmark_pack as validate_rag_benchmark_pack_pipeline
from qsr_audit.rag import validate_rag_reviewer_file as validate_rag_reviewer_file_pipeline
from qsr_audit.reconcile import audit_reference_coverage as audit_reference_coverage_pipeline
from qsr_audit.reconcile import reconcile_core_metrics as reconcile_core_metrics_pipeline
from qsr_audit.reconcile.primary_source_scaleup import (
    run_primary_source_scaleup as run_primary_source_scaleup_pipeline,
)
from qsr_audit.reconcile.qsr50_scaleup import (
    run_qsr50_scaleup as run_qsr50_scaleup_pipeline,
)
from qsr_audit.release import preflight_release as preflight_release_pipeline
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


def _record_command_success(
    *,
    settings,
    session,
    input_paths: list[Path | str],
    output_paths: list[Path | str],
    row_counts: dict[str, int] | None,
    data_classification: DataClassification,
    intended_audience: str,
    publish_status_scope: str,
    warnings_count: int = 0,
    errors_count: int = 0,
    upstream_artifact_references: list[Path | str] | None = None,
) -> tuple[Path, Path]:
    manifest_path = write_artifact_manifest(
        settings=settings,
        command_name=session.command_name,
        input_paths=input_paths,
        output_paths=output_paths,
        row_counts=row_counts,
        data_classification=data_classification,
        intended_audience=intended_audience,
        publish_status_scope=publish_status_scope,
        upstream_artifact_references=upstream_artifact_references or [],
        warnings_count=warnings_count,
        errors_count=errors_count,
        run_timestamp=session.start_timestamp,
        run_id=session.run_id,
    )
    audit_log_path = write_command_audit_log(
        settings=settings,
        session=session,
        status="success",
        input_paths=input_paths,
        output_paths=output_paths,
        warnings_count=warnings_count,
        errors_count=errors_count,
        manifest_path=manifest_path,
    )
    return manifest_path, audit_log_path


def _record_command_failure(
    *, settings, session, input_paths: list[Path | str], exc: Exception
) -> Path:
    return write_command_audit_log(
        settings=settings,
        session=session,
        status="failure",
        input_paths=input_paths,
        warnings_count=0,
        errors_count=1,
        failure_type=type(exc).__name__,
    )


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

OptionalInputWorkbookOption = Annotated[
    Path | None,
    typer.Option(
        "--input",
        exists=True,
        dir_okay=False,
        readable=True,
        path_type=Path,
        help="Optional workbook override for the five-brand happy-path demo.",
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

OptionalReferenceDirOption = Annotated[
    Path | None,
    typer.Option(
        "--reference-dir",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        path_type=Path,
        help="Optional reference directory override for the five-brand happy-path demo.",
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

CorpusPathOption = Annotated[
    Path | None,
    typer.Option(
        "--corpus-path",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        path_type=Path,
        help=(
            "Path to an existing retrieval corpus parquet. Defaults to "
            "`artifacts/rag/corpus/corpus.parquet`."
        ),
    ),
]

BenchmarkPathOption = Annotated[
    Path | None,
    typer.Option(
        "--benchmark-path",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        path_type=Path,
        help="Optional JSON benchmark fixture with analyst-style queries and relevance judgments.",
    ),
]

BenchmarkDirOption = Annotated[
    Path | None,
    typer.Option(
        "--benchmark-dir",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        path_type=Path,
        help=(
            "Directory containing analyst-authored `queries.csv`, `judgments.csv`, and "
            "optional `filters.csv` / `query_groups.csv`."
        ),
    ),
]

RequiredBenchmarkDirOption = Annotated[
    Path,
    typer.Option(
        ...,
        "--benchmark-dir",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        path_type=Path,
        help=(
            "Directory containing analyst-authored benchmark pack files under "
            "`data/rag_benchmarks/<pack>/`."
        ),
    ),
]

BenchmarkNameOption = Annotated[
    str,
    typer.Option(
        ...,
        "--name",
        help="Benchmark pack name. This becomes the local directory under `data/rag_benchmarks/`.",
    ),
]

ReviewerNameOption = Annotated[
    str,
    typer.Option(
        ...,
        "--reviewer",
        help="Reviewer name used to resolve `reviewers/<name>/judgments.csv` inside the pack.",
    ),
]

BenchmarkAuthorsOption = Annotated[
    list[str] | None,
    typer.Option(
        "--author",
        help="Analyst or reviewer name to record in the pack metadata. Repeatable.",
    ),
]

BenchmarkNotesOption = Annotated[
    str,
    typer.Option(
        "--notes",
        help="Optional pack notes stored in metadata.json.",
    ),
]

RagOutputOption = Annotated[
    Path | None,
    typer.Option(
        "--output-root",
        file_okay=False,
        dir_okay=True,
        path_type=Path,
        help=(
            "Directory for non-analyst-facing retrieval artifacts. Defaults to `artifacts/rag/...`."
        ),
    ),
]

RunDirOption = Annotated[
    Path,
    typer.Option(
        ...,
        "--run-dir",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        path_type=Path,
        help=(
            "Existing benchmark run directory under `artifacts/rag/benchmarks/` that contains "
            "per-query results and metrics."
        ),
    ),
]

RetrieverOption = typer.Option(
    None,
    "--retriever",
    help=(
        "Retriever slug to evaluate. Repeat for multiple runs, for example `--retriever bm25 "
        "--retriever dense-minilm`."
    ),
)

RerankerOption = Annotated[
    str | None,
    typer.Option(
        "--reranker",
        help=(
            "Optional reranker slug for benchmark comparison. Supported values: "
            + ", ".join(available_reranker_names())
            + "."
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

    settings = get_settings()
    session = begin_command_audit("validate-workbook")
    try:
        run = validate_workbook_pipeline(
            input_path=input_path,
            settings=settings,
            tolerance_auv=tolerance_auv,
        )
    except Exception as exc:
        _record_command_failure(
            settings=settings, session=session, input_paths=[input_path], exc=exc
        )
        raise
    counts = run.counts
    status_label = "passed" if run.passed else "failed"
    status_color = "green" if run.passed else "red"
    manifest_path, audit_log_path = _record_command_success(
        settings=settings,
        session=session,
        input_paths=[input_path],
        output_paths=[
            run.artifacts.summary_markdown,
            run.artifacts.results_json,
            run.artifacts.flags_parquet,
        ],
        row_counts={
            "validation_findings": len(run.findings),
            "validation_flags": len(run.findings),
        },
        data_classification=DataClassification.CONFIDENTIAL,
        intended_audience="analyst",
        publish_status_scope="working_layer_findings",
        warnings_count=counts["warning"],
        errors_count=counts["error"],
    )

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
    console.print(f"Manifest: {manifest_path}")
    console.print(f"Audit log: {audit_log_path}")

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

    settings = get_settings()
    session = begin_command_audit("run-syntheticness")
    try:
        run = run_syntheticness_pipeline(
            input_path=input_path,
            settings=settings,
            include_isolation_forest=include_isolation_forest,
        )
    except Exception as exc:
        _record_command_failure(
            settings=settings, session=session, input_paths=[input_path], exc=exc
        )
        raise
    counts = run.counts
    manifest_path, audit_log_path = _record_command_success(
        settings=settings,
        session=session,
        input_paths=[input_path],
        output_paths=[run.artifacts.report_markdown, run.artifacts.signals_parquet],
        row_counts={"syntheticness_signals": len(run.report.signals)},
        data_classification=DataClassification.INTERNAL,
        intended_audience="analyst",
        publish_status_scope="experimental_signals",
        warnings_count=counts["strong"] + counts["moderate"] + counts["weak"],
        errors_count=0,
        upstream_artifact_references=[latest_manifest_path(settings, "validate-workbook")],
    )

    console.print(f"[bold magenta]Syntheticness analysis complete[/bold magenta] - {input_path}")
    console.print(f"Strong signals: {counts['strong']}")
    console.print(f"Moderate signals: {counts['moderate']}")
    console.print(f"Weak signals: {counts['weak']}")
    console.print(f"Unknown / skipped: {counts['unknown']}")
    console.print(f"Report: {run.artifacts.report_markdown}")
    console.print(f"Signals parquet: {run.artifacts.signals_parquet}")
    console.print(f"Manifest: {manifest_path}")
    console.print(f"Audit log: {audit_log_path}")


@app.command("reconcile")
def reconcile_command(
    core_path: CoreMetricsOption,
    reference_dir: ReferenceDirOption,
) -> None:
    """Reconcile normalized core metrics and emit reference coverage artifacts."""

    settings = get_settings()
    session = begin_command_audit("reconcile")
    try:
        run = reconcile_core_metrics_pipeline(
            core_path=core_path,
            reference_dir=reference_dir,
            settings=settings,
        )
    except Exception as exc:
        _record_command_failure(
            settings=settings,
            session=session,
            input_paths=[core_path, reference_dir],
            exc=exc,
        )
        raise
    manifest_path, audit_log_path = _record_command_success(
        settings=settings,
        session=session,
        input_paths=[core_path, reference_dir],
        output_paths=[
            run.artifacts.reconciled_core_metrics_path,
            run.artifacts.provenance_registry_path,
            run.artifacts.reconciliation_summary_path,
            run.artifacts.reference_coverage_parquet_path,
            run.artifacts.reference_coverage_markdown_path,
        ],
        row_counts={
            "reconciled_core_metrics": len(run.reconciled_core_metrics),
            "provenance_registry": len(run.provenance_registry),
            "reference_coverage": len(run.reference_coverage),
        },
        data_classification=DataClassification.CONFIDENTIAL,
        intended_audience="analyst",
        publish_status_scope="all_gold_rows",
        warnings_count=len(run.warnings),
        errors_count=0,
        upstream_artifact_references=[latest_manifest_path(settings, "validate-workbook")],
    )
    console.print(f"[bold cyan]Reconciliation complete[/bold cyan] - {core_path}")
    console.print(f"Warnings: {len(run.warnings)}")
    console.print(f"Reconciled core metrics: {run.artifacts.reconciled_core_metrics_path}")
    console.print(f"Provenance registry: {run.artifacts.provenance_registry_path}")
    console.print(f"Summary: {run.artifacts.reconciliation_summary_path}")
    console.print(f"Reference coverage parquet: {run.artifacts.reference_coverage_parquet_path}")
    console.print(f"Reference coverage markdown: {run.artifacts.reference_coverage_markdown_path}")
    console.print(f"Manifest: {manifest_path}")
    console.print(f"Audit log: {audit_log_path}")


@app.command("reconcile-qsr50")
def reconcile_qsr50_command(
    core_path: CoreMetricsOption,
    reference_dir: ReferenceDirOption,
) -> None:
    """Run a QSR50-only broader reconciliation slice and emit analyst-readable deltas."""

    settings = get_settings()
    session = begin_command_audit("reconcile-qsr50")
    input_paths = [core_path, reference_dir]
    try:
        run = run_qsr50_scaleup_pipeline(
            core_path=core_path,
            reference_dir=reference_dir,
            settings=settings,
        )
    except Exception as exc:
        _record_command_failure(
            settings=settings,
            session=session,
            input_paths=input_paths,
            exc=exc,
        )
        raise
    manifest_path, audit_log_path = _record_command_success(
        settings=settings,
        session=session,
        input_paths=input_paths,
        output_paths=[
            run.artifacts.qsr50_coverage_markdown_path,
            run.artifacts.brand_deltas_full_csv_path,
            run.artifacts.qsr50_gold_candidates_parquet_path,
            run.artifacts.unresolved_reference_gaps_markdown_path,
        ],
        row_counts={
            "qsr50_gold_candidates": len(run.qsr50_gold_candidates),
            "brand_deltas_full": len(run.brand_deltas_full),
            "reference_coverage": len(run.reference_coverage),
        },
        data_classification=DataClassification.INTERNAL,
        intended_audience="analyst",
        publish_status_scope="qsr50_reconciliation_candidates",
        warnings_count=len(run.warnings),
        errors_count=0,
    )
    console.print("[bold cyan]QSR50 reconciliation slice complete[/bold cyan]")
    console.print(f"Warnings: {len(run.warnings)}")
    console.print(f"Coverage markdown: {run.artifacts.qsr50_coverage_markdown_path}")
    console.print(f"Brand deltas CSV: {run.artifacts.brand_deltas_full_csv_path}")
    console.print(f"Gold candidates parquet: {run.artifacts.qsr50_gold_candidates_parquet_path}")
    console.print(
        f"Unresolved gaps markdown: {run.artifacts.unresolved_reference_gaps_markdown_path}"
    )
    console.print(f"Manifest: {manifest_path}")
    console.print(f"Audit log: {audit_log_path}")


@app.command("reconcile-primary-source")
def reconcile_primary_source_command(
    core_path: CoreMetricsOption,
    reference_dir: ReferenceDirOption,
) -> None:
    """Run a primary-source-first reconciliation slice over public-chain reference rows."""

    settings = get_settings()
    session = begin_command_audit("reconcile-primary-source")
    input_paths = [core_path, reference_dir]
    try:
        run = run_primary_source_scaleup_pipeline(
            core_path=core_path,
            reference_dir=reference_dir,
            settings=settings,
        )
    except Exception as exc:
        _record_command_failure(
            settings=settings,
            session=session,
            input_paths=input_paths,
            exc=exc,
        )
        raise
    manifest_path, audit_log_path = _record_command_success(
        settings=settings,
        session=session,
        input_paths=input_paths,
        output_paths=[
            run.artifacts.primary_source_coverage_markdown_path,
            run.artifacts.primary_source_deltas_csv_path,
            run.artifacts.primary_source_gold_candidates_parquet_path,
        ],
        row_counts={
            "primary_source_gold_candidates": len(run.primary_source_gold_candidates),
            "primary_source_deltas": len(run.primary_source_deltas),
            "reference_coverage": len(run.reference_coverage),
        },
        data_classification=DataClassification.INTERNAL,
        intended_audience="analyst",
        publish_status_scope="primary_source_reconciliation_candidates",
        warnings_count=len(run.warnings),
        errors_count=0,
    )
    console.print("[bold cyan]Primary-source reconciliation slice complete[/bold cyan]")
    console.print(f"Warnings: {len(run.warnings)}")
    console.print(f"Coverage markdown: {run.artifacts.primary_source_coverage_markdown_path}")
    console.print(f"Brand deltas CSV: {run.artifacts.primary_source_deltas_csv_path}")
    console.print(
        "Gold candidates parquet: " f"{run.artifacts.primary_source_gold_candidates_parquet_path}"
    )
    console.print(f"Manifest: {manifest_path}")
    console.print(f"Audit log: {audit_log_path}")


@app.command("audit-reference")
def audit_reference_command(
    core_path: CoreMetricsOption,
    reference_dir: ReferenceDirOption,
) -> None:
    """Audit manual reference coverage and emit Gold/report coverage artifacts."""

    settings = get_settings()
    session = begin_command_audit("audit-reference")
    try:
        run = audit_reference_coverage_pipeline(
            core_path=core_path,
            reference_dir=reference_dir,
            settings=settings,
        )
    except Exception as exc:
        _record_command_failure(
            settings=settings,
            session=session,
            input_paths=[core_path, reference_dir],
            exc=exc,
        )
        raise
    manifest_path, audit_log_path = _record_command_success(
        settings=settings,
        session=session,
        input_paths=[core_path, reference_dir],
        output_paths=[run.artifacts.coverage_parquet_path, run.artifacts.coverage_markdown_path],
        row_counts={"reference_coverage": len(run.coverage_frame)},
        data_classification=DataClassification.INTERNAL,
        intended_audience="analyst",
        publish_status_scope="reference_coverage",
        warnings_count=len(run.warnings),
        errors_count=0,
        upstream_artifact_references=[latest_manifest_path(settings, "validate-workbook")],
    )
    console.print(f"[bold cyan]Reference coverage audit complete[/bold cyan] - {core_path}")
    console.print(f"Warnings: {len(run.warnings)}")
    console.print(f"Coverage parquet: {run.artifacts.coverage_parquet_path}")
    console.print(f"Coverage markdown: {run.artifacts.coverage_markdown_path}")
    console.print(f"Manifest: {manifest_path}")
    console.print(f"Audit log: {audit_log_path}")


@app.command("gate-gold")
def gate_gold_command() -> None:
    """Evaluate Gold publishing gates and emit KPI export decisions plus an audit scorecard."""

    settings = get_settings()
    session = begin_command_audit("gate-gold")
    gold_inputs = [
        settings.data_gold / "reconciled_core_metrics.parquet",
        settings.data_gold / "provenance_registry.parquet",
        settings.data_gold / "validation_flags.parquet",
        settings.data_gold / "reference_coverage.parquet",
        settings.data_gold / "syntheticness_signals.parquet",
    ]
    try:
        run = gate_gold_publish_pipeline(settings=settings)
    except Exception as exc:
        _record_command_failure(
            settings=settings, session=session, input_paths=gold_inputs, exc=exc
        )
        raise
    manifest_path, audit_log_path = _record_command_success(
        settings=settings,
        session=session,
        input_paths=gold_inputs,
        output_paths=[
            run.artifacts.decisions_path,
            run.artifacts.publishable_path,
            run.artifacts.blocked_path,
            run.artifacts.scorecard_markdown_path,
            run.artifacts.summary_json_path,
        ],
        row_counts={
            "decision_rows": len(run.decisions),
            "publishable_rows": int(run.summary["publishable_count"]),
            "advisory_rows": int(run.summary["advisory_count"]),
            "blocked_rows": int(run.summary["blocked_count"]),
        },
        data_classification=DataClassification.INTERNAL,
        intended_audience="release_manager",
        publish_status_scope="publishable_advisory_blocked",
        warnings_count=int(run.summary["advisory_count"]),
        errors_count=int(run.summary["blocked_count"]),
        upstream_artifact_references=[
            latest_manifest_path(settings, "validate-workbook"),
            latest_manifest_path(settings, "run-syntheticness"),
            latest_manifest_path(settings, "reconcile"),
        ],
    )
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
    console.print(f"Manifest: {manifest_path}")
    console.print(f"Audit log: {audit_log_path}")


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


@app.command("build-rag-corpus")
def build_rag_corpus_command(
    output_root: RagOutputOption = None,
) -> None:
    """Build a retrieval-only corpus from vetted Gold and provenance-aware artifacts."""

    try:
        run = build_rag_corpus_pipeline(
            settings=get_settings(),
            output_root=output_root,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    console.print("[bold blue]RAG corpus built[/bold blue]")
    console.print(f"Chunks: {len(run.corpus.index)}")
    console.print(f"Documents: {run.manifest['document_count']}")
    console.print(f"Corpus parquet: {run.artifacts.corpus_parquet_path}")
    console.print(f"Corpus JSONL: {run.artifacts.corpus_jsonl_path}")
    console.print(f"Manifest: {run.artifacts.manifest_path}")


@app.command("init-rag-benchmark")
def init_rag_benchmark_command(
    name: BenchmarkNameOption,
    author: BenchmarkAuthorsOption = None,
    notes: BenchmarkNotesOption = "",
) -> None:
    """Initialize a local benchmark pack with templates, metadata, and an analyst checklist."""

    try:
        run = init_rag_benchmark_pipeline(
            name=name,
            settings=get_settings(),
            authors=tuple(author or []),
            notes=notes,
        )
    except (FileExistsError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    console.print("[bold blue]RAG benchmark pack initialized[/bold blue]")
    console.print(f"Benchmark dir: {run.benchmark_dir}")
    console.print(f"Metadata: {run.artifacts.metadata_path}")
    console.print(f"README: {run.artifacts.readme_path}")
    console.print(f"Checklist: {run.artifacts.checklist_path}")
    console.print(f"Queries template: {run.artifacts.queries_path}")
    console.print(f"Judgments template: {run.artifacts.judgments_path}")


@app.command("bootstrap-rag-judgments")
def bootstrap_rag_judgments_command(
    benchmark_dir: RequiredBenchmarkDirOption,
    corpus_path: CorpusPathOption = None,
    retriever_name: str = typer.Option(
        "bm25",
        "--retriever",
        help="Retriever slug such as `bm25`, `dense-minilm`, or `dense-bge-small`.",
    ),
    top_k: int = typer.Option(
        10,
        "--top-k",
        min=1,
        help="Number of first-pass candidate chunks to suggest per query.",
    ),
    allow_model_download: bool = typer.Option(
        False,
        "--allow-model-download/--skip-model-download",
        help="Allow optional dense retrievers to download local model weights when not already cached.",
    ),
) -> None:
    """Bootstrap candidate retrieval suggestions and first-pass candidate chunks for manual benchmark judgment authoring."""

    try:
        run = bootstrap_rag_judgments_pipeline(
            benchmark_dir=benchmark_dir,
            settings=get_settings(),
            corpus_path=corpus_path,
            retriever_name=retriever_name,
            top_k=top_k,
            allow_model_download=allow_model_download,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    console.print("[bold blue]RAG judgment bootstrap complete[/bold blue]")
    console.print(f"Queries: {run.query_count}")
    console.print(f"Candidate rows: {run.candidate_count}")
    console.print(f"Query specs: {run.artifacts.query_specs_json_path}")
    console.print(f"Candidate parquet: {run.artifacts.candidate_results_parquet_path}")
    console.print(f"Candidate CSV: {run.artifacts.candidate_results_csv_path}")
    console.print(f"Judgment workspace: {run.artifacts.judgment_workspace_csv_path}")
    console.print(f"Bootstrap manifest: {run.artifacts.bootstrap_manifest_path}")


@app.command("seed-rag-queries")
def seed_rag_queries_command(
    benchmark_dir: RequiredBenchmarkDirOption,
    corpus_path: CorpusPathOption = None,
) -> None:
    """Seed deterministic analyst query suggestions into `working/` without touching final `queries.csv`."""

    try:
        run = seed_rag_queries_pipeline(
            benchmark_dir=benchmark_dir,
            settings=get_settings(),
            corpus_path=corpus_path,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    console.print("[bold blue]RAG query seeding complete[/bold blue]")
    console.print(f"Suggestions: {run.suggestion_count}")
    console.print(f"Suggested queries CSV: {run.artifacts.suggested_queries_csv_path}")
    console.print(f"Suggested queries summary: {run.artifacts.suggested_queries_markdown_path}")


@app.command("validate-rag-benchmark")
def validate_rag_benchmark_command(
    benchmark_dir: RequiredBenchmarkDirOption,
    corpus_path: CorpusPathOption = None,
) -> None:
    """Validate an analyst-authored benchmark pack built from `queries.csv` and `judgments.csv`."""

    settings = get_settings()
    resolved_corpus_path = resolve_rag_corpus_path(
        settings=settings,
        corpus_path=corpus_path,
    )
    if not resolved_corpus_path.exists():
        raise typer.BadParameter(
            "Benchmark validation requires an existing corpus parquet. Run "
            "`qsr-audit build-rag-corpus` first or pass `--corpus-path`."
        )

    from qsr_audit.rag import load_rag_corpus

    try:
        run = validate_rag_benchmark_pack_pipeline(
            benchmark_dir=benchmark_dir,
            corpus=load_rag_corpus(resolved_corpus_path),
            settings=settings,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    error_count = len([issue for issue in run.issues if issue["severity"] == "error"])
    warning_count = len([issue for issue in run.issues if issue["severity"] == "warning"])
    console.print("[bold blue]RAG benchmark validation complete[/bold blue]")
    console.print(f"Queries: {len(run.pack.queries.index)}")
    console.print(f"Judgments: {len(run.pack.judgments.index)}")
    console.print(f"Errors: {error_count}")
    console.print(f"Warnings: {warning_count}")
    console.print(f"Validation JSON: {run.artifacts.validation_json_path}")
    console.print(f"Validation summary: {run.artifacts.validation_markdown_path}")
    console.print(f"Query specs: {run.artifacts.query_specs_json_path}")
    if not run.passed:
        raise typer.Exit(code=1)


@app.command("validate-rag-reviewer-file")
def validate_rag_reviewer_file_command(
    benchmark_dir: RequiredBenchmarkDirOption,
    reviewer: ReviewerNameOption,
    corpus_path: CorpusPathOption = None,
) -> None:
    """Validate one reviewer-specific `reviewers/<name>/judgments.csv` file against the corpus."""

    try:
        run = validate_rag_reviewer_file_pipeline(
            benchmark_dir=benchmark_dir,
            reviewer=reviewer,
            settings=get_settings(),
            corpus_path=corpus_path,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    error_count = len([issue for issue in run.issues if issue["severity"] == "error"])
    warning_count = len([issue for issue in run.issues if issue["severity"] == "warning"])
    console.print("[bold blue]RAG reviewer validation complete[/bold blue]")
    console.print(f"Reviewer judgments: {run.pack.judgments_path}")
    console.print(f"Errors: {error_count}")
    console.print(f"Warnings: {warning_count}")
    console.print(f"Validation JSON: {run.artifacts.validation_json_path}")
    console.print(f"Validation summary: {run.artifacts.validation_markdown_path}")
    if not run.passed:
        raise typer.Exit(code=1)


@app.command("eval-rag-retrieval")
def eval_rag_retrieval_command(
    corpus_path: CorpusPathOption = None,
    benchmark_path: BenchmarkPathOption = None,
    benchmark_dir: BenchmarkDirOption = None,
    output_root: RagOutputOption = None,
    retriever: list[str] = RetrieverOption,
    top_k: int = typer.Option(
        5,
        "--top-k",
        min=1,
        help="Top-k depth for retrieval metrics such as Recall@k and nDCG@k.",
    ),
    reranker_name: RerankerOption = None,
    rerank_top_n: int = typer.Option(
        10,
        "--rerank-top-n",
        min=1,
        help=(
            "Candidate set depth to rerank when `--reranker` is enabled. The reranker remains "
            "opt-in and offline-only."
        ),
    ),
    allow_model_download: bool = typer.Option(
        False,
        "--allow-model-download/--skip-model-download",
        help="Allow optional dense retrievers to download local model weights when not already cached.",
    ),
) -> None:
    """Evaluate retrieval-only baselines over the default fixture or an analyst-authored benchmark pack."""

    try:
        run = eval_rag_retrieval_pipeline(
            settings=get_settings(),
            corpus_path=corpus_path,
            benchmark_path=benchmark_path,
            benchmark_dir=benchmark_dir,
            output_root=output_root,
            retrievers=retriever or ["bm25"],
            top_k=top_k,
            allow_model_download=allow_model_download,
            reranker_name=reranker_name,
            rerank_top_n=rerank_top_n,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    console.print("[bold blue]RAG retrieval benchmark complete[/bold blue]")
    console.print(f"Corpus chunks: {run.summary['corpus_chunk_count']}")
    console.print(f"Queries: {run.summary['query_count']}")
    console.print(f"Judged queries: {run.summary['judged_query_count']}")
    console.print(f"Metrics JSON: {run.artifacts.metrics_json_path}")
    console.print(f"Metrics CSV: {run.artifacts.metrics_csv_path}")
    console.print(f"Results parquet: {run.artifacts.results_parquet_path}")
    console.print(f"Failure cases: {run.artifacts.failure_cases_markdown_path}")
    console.print(f"Bucket metrics CSV: {run.artifacts.query_bucket_metrics_csv_path}")
    if run.artifacts.rerank_delta_csv_path is not None:
        console.print(f"Rerank delta CSV: {run.artifacts.rerank_delta_csv_path}")
    console.print(f"Summary: {run.artifacts.summary_markdown_path}")


@app.command("mine-rag-hard-negatives")
def mine_rag_hard_negatives_command(
    benchmark_dir: RequiredBenchmarkDirOption,
    run_dir: RunDirOption,
    corpus_path: CorpusPathOption = None,
) -> None:
    """Mine review-candidate hard negatives from one retrieval run without changing final judgments."""

    try:
        run = mine_rag_hard_negatives_pipeline(
            benchmark_dir=benchmark_dir,
            run_dir=run_dir,
            settings=get_settings(),
            corpus_path=corpus_path,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    console.print("[bold blue]RAG hard-negative mining complete[/bold blue]")
    console.print(f"Suggestions: {len(run.suggestions.index)}")
    console.print(f"Suggestion CSV: {run.artifacts.suggestions_csv_path}")
    console.print(f"Summary: {run.artifacts.summary_markdown_path}")


@app.command("adjudicate-rag-benchmark")
def adjudicate_rag_benchmark_command(
    benchmark_dir: RequiredBenchmarkDirOption,
    corpus_path: CorpusPathOption = None,
    force: bool = typer.Option(
        False,
        "--force/--no-force",
        help=(
            "Allow adjudicated_judgments.csv to be written even when reviewer conflicts remain. "
            "Use sparingly and treat the result as an explicit override."
        ),
    ),
) -> None:
    """Compare reviewer judgments, write conflict reports, and allow an explicit override when forced."""

    try:
        run = adjudicate_rag_benchmark_pipeline(
            benchmark_dir=benchmark_dir,
            settings=get_settings(),
            corpus_path=corpus_path,
            force=force,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    console.print("[bold blue]RAG benchmark adjudication complete[/bold blue]")
    console.print(f"Reviewers: {', '.join(run.reviewer_names)}")
    console.print(f"Conflicts: {run.conflict_count}")
    console.print(f"Agreement summary JSON: {run.artifacts.agreement_summary_json_path}")
    console.print(f"Agreement summary: {run.artifacts.agreement_summary_markdown_path}")
    console.print(f"Conflict report: {run.artifacts.conflicts_csv_path}")
    if run.artifacts.adjudicated_judgments_path is not None:
        console.print(f"Adjudicated judgments: {run.artifacts.adjudicated_judgments_path}")


@app.command("summarize-rag-failures")
def summarize_rag_failures_command(
    benchmark_dir: RequiredBenchmarkDirOption,
    run_dir: RunDirOption,
) -> None:
    """Bucket retrieval benchmark failures into triage categories for benchmark cleanup."""

    try:
        run = summarize_rag_failures_pipeline(
            benchmark_dir=benchmark_dir,
            run_dir=run_dir,
            settings=get_settings(),
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    console.print("[bold blue]RAG failure triage complete[/bold blue]")
    console.print(f"Triaged rows: {len(run.triage_rows.index)}")
    console.print(f"Triage CSV: {run.artifacts.triage_csv_path}")
    console.print(f"Triage JSON: {run.artifacts.triage_json_path}")
    console.print(f"Triage summary: {run.artifacts.triage_markdown_path}")


@app.command("summarize-rag-benchmark-authoring")
def summarize_rag_benchmark_authoring_command(
    benchmark_dir: RequiredBenchmarkDirOption,
    run_dir: Annotated[
        Path | None,
        typer.Option(
            "--run-dir",
            exists=True,
            file_okay=False,
            dir_okay=True,
            readable=True,
            path_type=Path,
            help=(
                "Optional benchmark run directory under `artifacts/rag/benchmarks/` used to "
                "surface dominant failure buckets and hard-negative review gaps."
            ),
        ),
    ] = None,
) -> None:
    """Summarize benchmark authoring coverage, gaps, and current judgment readiness."""

    try:
        run = summarize_rag_benchmark_authoring_pipeline(
            benchmark_dir=benchmark_dir,
            settings=get_settings(),
            run_dir=run_dir,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    console.print("[bold blue]RAG benchmark authoring summary complete[/bold blue]")
    console.print(f"Pack status: {run.summary['pack_status']}")
    console.print(f"Judgments source: {run.summary['judgments_source']}")
    console.print(f"Unjudged queries: {len(run.summary['unjudged_queries'])}")
    console.print(f"Hard-negative suggestions: {run.summary['hard_negative_suggestion_count']}")
    console.print(f"Summary JSON: {run.artifacts.summary_json_path}")
    console.print(f"Summary: {run.artifacts.summary_markdown_path}")
    console.print(f"Coverage rows CSV: {run.artifacts.coverage_rows_csv_path}")


@app.command("rag-search")
def rag_search_command(
    query: Annotated[
        str,
        typer.Option(
            ...,
            "--query",
            help="Retrieval-only analyst query. Returns chunks and metadata, not generated answers.",
        ),
    ],
    corpus_path: CorpusPathOption = None,
    top_k: int = typer.Option(
        5,
        "--top-k",
        min=1,
        help="Maximum number of retrieved chunks to return.",
    ),
    retriever_name: str = typer.Option(
        "bm25",
        "--retriever",
        help="Retriever slug such as `bm25`, `dense-minilm`, or `dense-bge-small`.",
    ),
    allow_model_download: bool = typer.Option(
        False,
        "--allow-model-download/--skip-model-download",
        help="Allow optional dense retrievers to download local model weights when not already cached.",
    ),
) -> None:
    """Run retrieval-only search and print ranked chunks plus metadata as JSON, not generated answers."""

    settings = get_settings()
    resolved_corpus_path = resolve_rag_corpus_path(
        settings=settings,
        corpus_path=corpus_path,
    )
    if not resolved_corpus_path.exists():
        raise typer.BadParameter(
            "RAG search requires an existing corpus parquet. Run `qsr-audit build-rag-corpus` first "
            "or pass `--corpus-path`."
        )

    from qsr_audit.rag import load_rag_corpus

    run = rag_search_pipeline(
        corpus=load_rag_corpus(resolved_corpus_path),
        query=query,
        top_k=top_k,
        retriever_name=retriever_name,
        allow_model_download=allow_model_download,
    )
    if run.status != "ok":
        raise typer.BadParameter(run.reason or "RAG search could not be executed.")
    typer.echo(run.results.to_json(orient="records", force_ascii=False))


@app.command("inspect-rag-benchmark")
def inspect_rag_benchmark_command(
    query_id: Annotated[
        str,
        typer.Option(
            ...,
            "--query-id",
            help="Benchmark query_id to inspect against the current corpus and retriever.",
        ),
    ],
    benchmark_dir: Annotated[
        Path,
        typer.Option(
            ...,
            "--benchmark-dir",
            exists=True,
            file_okay=False,
            dir_okay=True,
            readable=True,
            path_type=Path,
            help="Directory containing analyst-authored benchmark CSV files.",
        ),
    ],
    corpus_path: CorpusPathOption = None,
    top_k: int = typer.Option(
        5,
        "--top-k",
        min=1,
        help="Maximum number of retrieved chunks to inspect.",
    ),
    retriever_name: str = typer.Option(
        "bm25",
        "--retriever",
        help="Retriever slug such as `bm25`, `dense-minilm`, or `dense-bge-small`.",
    ),
    reranker_name: RerankerOption = None,
    rerank_top_n: int = typer.Option(
        10,
        "--rerank-top-n",
        min=1,
        help="Candidate depth to rerank when `--reranker` is enabled.",
    ),
    allow_model_download: bool = typer.Option(
        False,
        "--allow-model-download/--skip-model-download",
        help="Allow optional dense retrieval or reranking models to download weights when not already cached.",
    ),
) -> None:
    """Inspect one benchmark query and print expected evidence, retrieved chunks, and failure diagnosis."""

    try:
        payload = inspect_rag_benchmark_query_pipeline(
            settings=get_settings(),
            corpus_path=corpus_path,
            benchmark_dir=benchmark_dir,
            query_id=query_id,
            retriever_name=retriever_name,
            top_k=top_k,
            allow_model_download=allow_model_download,
            reranker_name=reranker_name,
            rerank_top_n=rerank_top_n,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


@app.command()
def report(
    output: ReportOutputOption = Path("reports"),
) -> None:
    """Generate Markdown/HTML/JSON audit reports and Gold-derived strategy outputs."""

    settings = get_settings()
    session = begin_command_audit("report")
    report_inputs = [
        settings.data_gold / "gold_publish_decisions.parquet",
        settings.data_gold / "reconciled_core_metrics.parquet",
        settings.data_gold / "provenance_registry.parquet",
        settings.data_gold / "reference_coverage.parquet",
        settings.data_gold / "validation_flags.parquet",
        settings.data_gold / "syntheticness_signals.parquet",
        settings.reports_dir / "validation" / "validation_results.json",
    ]
    try:
        artifacts = write_reports_pipeline(output_root=output, settings=settings)
        strategy_run = generate_strategy_outputs_pipeline(
            settings=settings,
            report_dir=output / "strategy",
        )
    except Exception as exc:
        _record_command_failure(
            settings=settings, session=session, input_paths=report_inputs, exc=exc
        )
        raise
    manifest_path, audit_log_path = _record_command_success(
        settings=settings,
        session=session,
        input_paths=report_inputs,
        output_paths=[
            artifacts.global_markdown,
            artifacts.global_html,
            artifacts.global_json,
            *artifacts.brand_markdown_paths.values(),
            *artifacts.brand_html_paths.values(),
            *artifacts.brand_json_paths.values(),
            strategy_run.artifacts.recommendations_parquet_path,
            strategy_run.artifacts.recommendations_json_path,
            strategy_run.artifacts.playbook_markdown_path,
        ],
        row_counts={"brand_reports": len(artifacts.brand_markdown_paths)},
        data_classification=DataClassification.INTERNAL,
        intended_audience="analyst",
        publish_status_scope="publishable_plus_internal_context",
        warnings_count=0,
        errors_count=0,
        upstream_artifact_references=[
            latest_manifest_path(settings, "gate-gold"),
            latest_manifest_path(settings, "reconcile"),
            latest_manifest_path(settings, "validate-workbook"),
            latest_manifest_path(settings, "run-syntheticness"),
        ],
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
    console.print(f"Manifest: {manifest_path}")
    console.print(f"Audit log: {audit_log_path}")


@app.command("preflight-release")
def preflight_release_command() -> None:
    """Check whether Gold outputs and lineage artifacts are ready for external-facing handoff."""

    settings = get_settings()
    session = begin_command_audit("preflight-release")
    input_paths = [
        settings.data_gold / "gold_publish_decisions.parquet",
        settings.data_gold / "publishable_kpis.parquet",
        settings.data_gold / "blocked_kpis.parquet",
        latest_manifest_path(settings, "validate-workbook"),
        latest_manifest_path(settings, "run-syntheticness"),
        latest_manifest_path(settings, "reconcile"),
        latest_manifest_path(settings, "gate-gold"),
    ]
    try:
        run = preflight_release_pipeline(settings=settings)
    except Exception as exc:
        _record_command_failure(
            settings=settings, session=session, input_paths=input_paths, exc=exc
        )
        raise
    manifest_path, audit_log_path = _record_command_success(
        settings=settings,
        session=session,
        input_paths=input_paths,
        output_paths=[run.artifacts.summary_json_path, run.artifacts.summary_markdown_path],
        row_counts={"checks": len(run.checks)},
        data_classification=DataClassification.INTERNAL,
        intended_audience="release_manager",
        publish_status_scope="release_readiness",
        warnings_count=int(run.summary["warning_check_count"]),
        errors_count=int(run.summary["failed_check_count"]),
        upstream_artifact_references=[
            latest_manifest_path(settings, "validate-workbook"),
            latest_manifest_path(settings, "run-syntheticness"),
            latest_manifest_path(settings, "reconcile"),
            latest_manifest_path(settings, "gate-gold"),
        ],
    )
    console.print(
        f"[bold {'green' if run.passed else 'red'}]Release preflight {'passed' if run.passed else 'failed'}[/bold {'green' if run.passed else 'red'}]"
    )
    console.print(f"Failed checks: {run.summary['failed_check_count']}")
    console.print(f"Warning checks: {run.summary['warning_check_count']}")
    console.print(f"Summary JSON: {run.artifacts.summary_json_path}")
    console.print(f"Summary Markdown: {run.artifacts.summary_markdown_path}")
    console.print(f"Manifest: {manifest_path}")
    console.print(f"Audit log: {audit_log_path}")
    if not run.passed:
        raise typer.Exit(code=1)


@app.command("demo-happy-path")
def demo_happy_path_command(
    input_path: OptionalInputWorkbookOption = None,
    reference_dir: OptionalReferenceDirOption = None,
) -> None:
    """Run the five-brand demo slice from raw workbook through final demo scorecard outputs."""

    settings = get_settings()
    session = begin_command_audit("demo-happy-path")
    input_paths: list[Path | str] = [
        input_path or settings.data_raw,
        reference_dir or settings.data_reference,
    ]
    try:
        run = run_demo_happy_path_pipeline(
            settings=settings,
            input_path=input_path,
            reference_dir=reference_dir,
        )
    except (FileNotFoundError, ValueError) as exc:
        _record_command_failure(
            settings=settings, session=session, input_paths=input_paths, exc=exc
        )
        console.print(f"[bold red]Happy-path demo failed[/bold red] - {exc}")
        raise typer.Exit(code=1) from exc
    except Exception as exc:
        _record_command_failure(
            settings=settings, session=session, input_paths=input_paths, exc=exc
        )
        raise

    manifest_path, audit_log_path = _record_command_success(
        settings=settings,
        session=session,
        input_paths=input_paths,
        output_paths=[
            run.artifacts.demo_hub_html_path,
            run.artifacts.core_scorecard_html_path,
            run.artifacts.brand_deltas_csv_path,
            run.artifacts.top_risks_markdown_path,
            run.artifacts.demo_gold_parquet_path,
            run.artifacts.demo_syntheticness_parquet_path,
        ],
        row_counts={
            "demo_gold_rows": len(run.demo_gold),
            "demo_syntheticness_rows": len(run.demo_syntheticness),
            "brand_delta_rows": len(run.brand_deltas),
        },
        data_classification=DataClassification.INTERNAL,
        intended_audience="analyst",
        publish_status_scope="five_brand_demo",
        warnings_count=len(run.warnings),
        errors_count=0,
    )

    console.print("[bold green]Five-brand happy-path demo complete[/bold green]")
    console.print(f"Core scorecard HTML: {run.artifacts.core_scorecard_html_path}")
    console.print(f"Demo hub HTML: {run.artifacts.demo_hub_html_path}")
    console.print(f"Brand deltas CSV: {run.artifacts.brand_deltas_csv_path}")
    console.print(f"Top risks Markdown: {run.artifacts.top_risks_markdown_path}")
    console.print(f"Demo Gold parquet: {run.artifacts.demo_gold_parquet_path}")
    console.print(f"Demo syntheticness parquet: {run.artifacts.demo_syntheticness_parquet_path}")
    console.print(f"Warnings: {len(run.warnings)}")
    console.print(f"Manifest: {manifest_path}")
    console.print(f"Audit log: {audit_log_path}")


@app.command("package-demo")
def package_demo_command(
    input_path: OptionalInputWorkbookOption = None,
    reference_dir: OptionalReferenceDirOption = None,
) -> None:
    """Bundle the five-brand demo outputs into a shareable artifact directory."""

    settings = get_settings()
    session = begin_command_audit("package-demo")
    input_paths: list[Path | str] = [
        input_path or settings.data_raw,
        reference_dir or settings.data_reference,
    ]
    try:
        run = run_demo_happy_path_pipeline(
            settings=settings,
            input_path=input_path,
            reference_dir=reference_dir,
        )
        bundle = package_demo_bundle_pipeline(
            settings=settings,
            bundle_root=None,
            source_paths=[
                run.artifacts.demo_hub_html_path,
                run.artifacts.core_scorecard_html_path,
                run.artifacts.brand_deltas_csv_path,
                run.artifacts.top_risks_markdown_path,
                run.artifacts.demo_gold_parquet_path,
                run.artifacts.demo_syntheticness_parquet_path,
            ],
        )
    except (FileNotFoundError, ValueError) as exc:
        _record_command_failure(
            settings=settings, session=session, input_paths=input_paths, exc=exc
        )
        console.print(f"[bold red]Demo packaging failed[/bold red] - {exc}")
        raise typer.Exit(code=1) from exc
    except Exception as exc:
        _record_command_failure(
            settings=settings, session=session, input_paths=input_paths, exc=exc
        )
        raise

    manifest_path, audit_log_path = _record_command_success(
        settings=settings,
        session=session,
        input_paths=input_paths,
        output_paths=[
            run.artifacts.demo_hub_html_path,
            bundle.bundle_root,
            bundle.manifest_json_path,
            *bundle.copied_paths,
        ],
        row_counts={
            "demo_gold_rows": len(run.demo_gold),
            "demo_syntheticness_rows": len(run.demo_syntheticness),
            "brand_delta_rows": len(run.brand_deltas),
            "bundle_file_count": len(bundle.copied_paths),
        },
        data_classification=DataClassification.INTERNAL,
        intended_audience="analyst",
        publish_status_scope="five_brand_demo_bundle",
        warnings_count=len(run.warnings),
        errors_count=0,
    )

    console.print("[bold green]Five-brand demo bundle created[/bold green]")
    console.print(f"Bundle root: {bundle.bundle_root}")
    console.print(f"Bundle manifest: {bundle.manifest_json_path}")
    console.print(f"Demo hub HTML: {run.artifacts.demo_hub_html_path}")
    console.print(f"Core scorecard HTML: {run.artifacts.core_scorecard_html_path}")
    console.print(f"Brand deltas CSV: {run.artifacts.brand_deltas_csv_path}")
    console.print(f"Top risks Markdown: {run.artifacts.top_risks_markdown_path}")
    console.print(f"Warnings: {len(run.warnings)}")
    console.print(f"Manifest: {manifest_path}")
    console.print(f"Audit log: {audit_log_path}")


if __name__ == "__main__":
    app()
