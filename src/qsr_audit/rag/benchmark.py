"""Retrieval-only benchmark harness for local RAG experiments."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.config import Settings
from qsr_audit.rag.benchmark_pack import (
    ADJUDICATED_JUDGMENTS_FILENAME,
    DEFAULT_BENCHMARK_VALIDATION_SUBDIR,
    RagBenchmarkValidationRun,
    resolve_preferred_judgments_path,
    validate_rag_benchmark_pack,
)
from qsr_audit.rag.corpus import load_rag_corpus, resolve_rag_corpus_path
from qsr_audit.rag.retrieval import (
    prepare_reranker,
    prepare_retriever,
    rag_search,
    rerank_results,
)

DEFAULT_BENCHMARKS_SUBDIR = Path("rag/benchmarks")
DEFAULT_RETRIEVAL_BENCHMARK: tuple[dict[str, Any], ...] = (
    {
        "query_id": "blocked-kpi-smoke",
        "query": "Which KPI rows are blocked for external export?",
        "metadata_filters": {"publish_status": "blocked"},
        "relevant_filters": {
            "source_kind": "gold_publish_decision",
            "publish_status": "blocked",
        },
        "notes": "Smoke benchmark for blocked Gold decision retrieval.",
    },
    {
        "query_id": "validation-error-smoke",
        "query": "Show validation findings with error severity.",
        "metadata_filters": {"source_kind": "gold_validation_flag"},
        "relevant_filters": {
            "source_kind": "gold_validation_flag",
            "severity": "error",
        },
        "notes": "Smoke benchmark for structured validation findings.",
    },
    {
        "query_id": "provenance-smoke",
        "query": "Find provenance records for Taco Bell.",
        "metadata_filters": {
            "source_kind": "gold_provenance_registry",
            "brand_names": ["Taco Bell"],
        },
        "relevant_filters": {
            "source_kind": "gold_provenance_registry",
            "brand_names": ["Taco Bell"],
        },
        "notes": "Smoke benchmark for brand-scoped provenance lookup.",
    },
)


@dataclass(frozen=True)
class RagBenchmarkArtifacts:
    """Written outputs for a retrieval benchmark run."""

    metrics_json_path: Path
    metrics_csv_path: Path
    results_parquet_path: Path
    summary_markdown_path: Path
    failure_cases_markdown_path: Path
    benchmark_queries_json_path: Path
    query_bucket_metrics_csv_path: Path
    rerank_delta_csv_path: Path | None


@dataclass(frozen=True)
class RagBenchmarkRun:
    """Result of evaluating retrieval baselines against a benchmark fixture."""

    corpus: pd.DataFrame
    queries: list[dict[str, Any]]
    results: pd.DataFrame
    metrics: pd.DataFrame
    query_bucket_metrics: pd.DataFrame
    rerank_delta: pd.DataFrame
    summary: dict[str, Any]
    failure_cases: list[dict[str, Any]]
    artifacts: RagBenchmarkArtifacts
    validation: RagBenchmarkValidationRun | None


def eval_rag_retrieval(
    *,
    settings: Settings | None = None,
    corpus_path: Path | None = None,
    benchmark_path: Path | None = None,
    benchmark_dir: Path | None = None,
    output_root: Path | None = None,
    retrievers: tuple[str, ...] = ("bm25",),
    top_k: int = 5,
    allow_model_download: bool = False,
    reranker_name: str | None = None,
    rerank_top_n: int = 10,
) -> RagBenchmarkRun:
    """Run retrieval-only evaluation over a local benchmark fixture or benchmark pack."""

    resolved_settings = settings or Settings()
    resolved_corpus_path = resolve_rag_corpus_path(
        settings=resolved_settings,
        corpus_path=corpus_path,
    )
    if not resolved_corpus_path.exists():
        raise FileNotFoundError(
            "Retrieval benchmark requires an existing corpus parquet. Run "
            "`qsr-audit build-rag-corpus` first or pass `--corpus-path`."
        )
    resolved_output_root = _resolve_output_root(
        output_root=output_root,
        settings=resolved_settings,
    )
    resolved_output_root.mkdir(parents=True, exist_ok=True)

    corpus = load_rag_corpus(resolved_corpus_path)
    queries, validation = _load_query_specs(
        corpus=corpus,
        benchmark_path=benchmark_path,
        benchmark_dir=benchmark_dir,
        settings=resolved_settings,
        output_root=resolved_output_root,
    )

    candidate_top_k = max(top_k, rerank_top_n) if reranker_name else top_k
    diagnostic_top_k = max(candidate_top_k, top_k * 5)

    results_frames: list[pd.DataFrame] = []
    metrics_rows: list[dict[str, Any]] = []
    query_bucket_metrics_rows: list[dict[str, Any]] = []
    failure_cases: list[dict[str, Any]] = []
    rerank_delta_rows: list[dict[str, Any]] = []
    all_query_metrics: list[dict[str, Any]] = []

    for retriever_name in retrievers:
        prepared_retriever = prepare_retriever(
            corpus=corpus,
            retriever_name=retriever_name,
            allow_model_download=allow_model_download,
        )
        probe_run = rag_search(
            corpus=corpus,
            query="",
            top_k=candidate_top_k,
            retriever_name=retriever_name,
            metadata_filters={},
            allow_model_download=allow_model_download,
            prepared_retriever=prepared_retriever,
        )
        if probe_run.status != "ok":
            metrics_rows.append(
                _skipped_metrics_row(
                    retriever_name=retriever_name,
                    run_label=retriever_name,
                    stage="retrieval",
                    query_count=len(queries),
                    reason=probe_run.reason,
                    index_size_bytes=0,
                )
            )
            continue

        prepared_reranker = None
        if reranker_name:
            prepared_reranker = prepare_reranker(
                reranker_name=reranker_name,
                allow_model_download=allow_model_download,
            )

        retrieval_query_metrics: list[dict[str, Any]] = []
        reranked_query_metrics: list[dict[str, Any]] = []

        for query_spec in queries:
            search_run = rag_search(
                corpus=corpus,
                query=query_spec["query"],
                top_k=candidate_top_k,
                retriever_name=retriever_name,
                metadata_filters=query_spec.get("metadata_filters") or {},
                allow_model_download=allow_model_download,
                prepared_retriever=prepared_retriever,
            )
            candidate_results = search_run.results.copy()
            retrieval_results = candidate_results.head(top_k).copy()
            failure_source = _diagnose_failure_source(
                corpus=corpus,
                query_spec=query_spec,
                final_results=retrieval_results,
                filtered_candidates=candidate_results,
                prepared_retriever=prepared_retriever,
                retriever_name=retriever_name,
                allow_model_download=allow_model_download,
                diagnostic_top_k=diagnostic_top_k,
            )
            retrieval_metric = _score_query_results(
                query_spec=query_spec,
                results=retrieval_results,
                latency_ms=search_run.latency_ms,
                top_k=top_k,
                failure_source=failure_source,
                retriever_name=retriever_name,
                run_label=retriever_name,
                stage="retrieval",
                reranker_name=None,
                index_size_bytes=probe_run.index_size_bytes,
            )
            retrieval_query_metrics.append(retrieval_metric)
            all_query_metrics.append(retrieval_metric)
            results_frames.append(
                _annotate_result_rows(
                    results=retrieval_results,
                    query_spec=query_spec,
                    query_metric=retrieval_metric,
                    run_label=retriever_name,
                    stage="retrieval",
                    reranker_name=None,
                )
            )
            if retrieval_metric["status"] != "ok":
                failure_cases.append(_failure_case_from_metric(retrieval_metric))

            if not reranker_name:
                continue
            rerank_run = rerank_results(
                query=query_spec["query"],
                results=candidate_results,
                top_k=top_k,
                reranker_name=reranker_name,
                allow_model_download=allow_model_download,
                prepared_reranker=prepared_reranker,
            )
            rerank_label = f"{retriever_name}+{reranker_name}"
            if rerank_run.status != "ok":
                reranked_query_metrics.append(
                    _skipped_query_metric(
                        query_spec=query_spec,
                        retriever_name=retriever_name,
                        run_label=rerank_label,
                        stage="reranked",
                        reranker_name=reranker_name,
                        reason=rerank_run.reason,
                    )
                )
                rerank_delta_rows.append(
                    {
                        "retriever_name": retriever_name,
                        "reranker_name": reranker_name,
                        "query_id": query_spec["query_id"],
                        "run_label": rerank_label,
                        "status": "skipped",
                        "status_reason": rerank_run.reason,
                        "mrr_delta": None,
                        "ndcg_delta": None,
                        "recall_delta": None,
                        "latency_delta_ms": None,
                    }
                )
                continue

            reranked_results = rerank_run.results.copy()
            reranked_failure_source = _diagnose_failure_source(
                corpus=corpus,
                query_spec=query_spec,
                final_results=reranked_results,
                filtered_candidates=candidate_results,
                prepared_retriever=prepared_retriever,
                retriever_name=retriever_name,
                allow_model_download=allow_model_download,
                diagnostic_top_k=diagnostic_top_k,
            )
            reranked_metric = _score_query_results(
                query_spec=query_spec,
                results=reranked_results,
                latency_ms=search_run.latency_ms + rerank_run.latency_ms,
                top_k=top_k,
                failure_source=reranked_failure_source,
                retriever_name=retriever_name,
                run_label=rerank_label,
                stage="reranked",
                reranker_name=reranker_name,
                index_size_bytes=probe_run.index_size_bytes,
            )
            reranked_query_metrics.append(reranked_metric)
            all_query_metrics.append(reranked_metric)
            results_frames.append(
                _annotate_result_rows(
                    results=reranked_results,
                    query_spec=query_spec,
                    query_metric=reranked_metric,
                    run_label=rerank_label,
                    stage="reranked",
                    reranker_name=reranker_name,
                )
            )
            rerank_delta_rows.append(
                _rerank_delta_row(
                    retrieval_metric=retrieval_metric,
                    reranked_metric=reranked_metric,
                    reranker_name=reranker_name,
                )
            )
            if reranked_metric["status"] != "ok":
                failure_cases.append(_failure_case_from_metric(reranked_metric))

        metrics_rows.append(
            _aggregate_query_metrics(
                retriever_name=retriever_name,
                run_label=retriever_name,
                stage="retrieval",
                reranker_name=None,
                query_metrics=retrieval_query_metrics,
                index_size_bytes=probe_run.index_size_bytes,
            )
        )
        query_bucket_metrics_rows.extend(
            _aggregate_query_bucket_metrics(
                query_metrics=retrieval_query_metrics,
                run_label=retriever_name,
                stage="retrieval",
            )
        )

        if reranker_name:
            rerank_label = f"{retriever_name}+{reranker_name}"
            metrics_rows.append(
                _aggregate_query_metrics(
                    retriever_name=retriever_name,
                    run_label=rerank_label,
                    stage="reranked",
                    reranker_name=reranker_name,
                    query_metrics=reranked_query_metrics,
                    index_size_bytes=probe_run.index_size_bytes,
                )
            )
            query_bucket_metrics_rows.extend(
                _aggregate_query_bucket_metrics(
                    query_metrics=reranked_query_metrics,
                    run_label=rerank_label,
                    stage="reranked",
                )
            )

    results = (
        pd.concat(results_frames, ignore_index=True)
        if results_frames and any(not frame.empty for frame in results_frames)
        else pd.DataFrame(columns=_result_columns())
    )
    metrics = pd.DataFrame(metrics_rows)
    query_bucket_metrics = pd.DataFrame(
        query_bucket_metrics_rows,
        columns=[
            "bucket_name",
            "run_label",
            "stage",
            "query_count",
            "judged_query_count",
            "recall_at_k",
            "mrr",
            "ndcg_at_k",
            "citation_precision",
            "metadata_filter_correctness",
        ],
    )
    rerank_delta = pd.DataFrame(
        rerank_delta_rows,
        columns=[
            "retriever_name",
            "reranker_name",
            "query_id",
            "run_label",
            "status",
            "status_reason",
            "mrr_delta",
            "ndcg_delta",
            "recall_delta",
            "latency_delta_ms",
        ],
    )
    summary = _build_summary(
        corpus=corpus,
        queries=queries,
        metrics=metrics,
        query_bucket_metrics=query_bucket_metrics,
        failure_cases=failure_cases,
        rerank_delta=rerank_delta,
        query_metrics=all_query_metrics,
        top_k=top_k,
        query_count=len(queries),
        judged_query_count=len(
            [
                query
                for query in queries
                if query.get("relevant_chunk_ids") or query.get("relevant_doc_ids")
            ]
        ),
        benchmark_path=benchmark_path,
        benchmark_dir=benchmark_dir,
        corpus_path=resolved_corpus_path,
        validation=validation,
    )
    artifacts = _write_outputs(
        output_root=resolved_output_root,
        queries=queries,
        results=results,
        metrics=metrics,
        query_bucket_metrics=query_bucket_metrics,
        rerank_delta=rerank_delta,
        summary=summary,
        failure_cases=failure_cases,
    )
    return RagBenchmarkRun(
        corpus=corpus,
        queries=queries,
        results=results,
        metrics=metrics,
        query_bucket_metrics=query_bucket_metrics,
        rerank_delta=rerank_delta,
        summary=summary,
        failure_cases=failure_cases,
        artifacts=artifacts,
        validation=validation,
    )


def inspect_rag_benchmark_query(
    *,
    settings: Settings | None = None,
    corpus_path: Path | None = None,
    benchmark_dir: Path,
    query_id: str,
    retriever_name: str = "bm25",
    top_k: int = 5,
    allow_model_download: bool = False,
    reranker_name: str | None = None,
    rerank_top_n: int = 10,
) -> dict[str, Any]:
    """Inspect one benchmark query and return retrieved rows plus failure diagnostics."""

    resolved_settings = settings or Settings()
    resolved_corpus_path = resolve_rag_corpus_path(
        settings=resolved_settings,
        corpus_path=corpus_path,
    )
    if not resolved_corpus_path.exists():
        raise FileNotFoundError(
            "Benchmark inspection requires an existing corpus parquet. Run "
            "`qsr-audit build-rag-corpus` first or pass `--corpus-path`."
        )

    corpus = load_rag_corpus(resolved_corpus_path)
    judgments_path = resolve_preferred_judgments_path(benchmark_dir)
    validation = validate_rag_benchmark_pack(
        benchmark_dir=benchmark_dir,
        corpus=corpus,
        settings=resolved_settings,
        judgments_path=judgments_path,
    )
    if not validation.passed:
        raise ValueError(
            "Benchmark pack validation failed. Resolve validation errors before inspection. "
            f"See {validation.artifacts.validation_markdown_path}."
        )

    query_spec = next(
        (candidate for candidate in validation.query_specs if candidate["query_id"] == query_id),
        None,
    )
    if query_spec is None:
        raise ValueError(f"Benchmark query_id `{query_id}` was not found in `{benchmark_dir}`.")

    candidate_top_k = max(top_k, rerank_top_n) if reranker_name else top_k
    prepared_retriever = prepare_retriever(
        corpus=corpus,
        retriever_name=retriever_name,
        allow_model_download=allow_model_download,
    )
    search_run = rag_search(
        corpus=corpus,
        query=query_spec["query"],
        top_k=candidate_top_k,
        retriever_name=retriever_name,
        metadata_filters=query_spec.get("metadata_filters") or {},
        allow_model_download=allow_model_download,
        prepared_retriever=prepared_retriever,
    )
    if search_run.status != "ok":
        raise ValueError(search_run.reason or "Benchmark inspection search could not be executed.")

    final_results = search_run.results.head(top_k).copy()
    if reranker_name:
        rerank_run = rerank_results(
            query=query_spec["query"],
            results=search_run.results,
            top_k=top_k,
            reranker_name=reranker_name,
            allow_model_download=allow_model_download,
        )
        if rerank_run.status == "ok":
            final_results = rerank_run.results.copy()

    failure_source = _diagnose_failure_source(
        corpus=corpus,
        query_spec=query_spec,
        final_results=final_results,
        filtered_candidates=search_run.results,
        prepared_retriever=prepared_retriever,
        retriever_name=retriever_name,
        allow_model_download=allow_model_download,
        diagnostic_top_k=max(candidate_top_k, top_k * 5),
    )
    relevant_rows = corpus.loc[
        corpus["chunk_id"].isin(query_spec["relevant_chunk_ids"])
        | corpus["doc_id"].isin(query_spec.get("relevant_doc_ids") or []),
        [
            "doc_id",
            "chunk_id",
            "source_kind",
            "title",
            "artifact_path",
            "brand_names",
            "metric_names",
            "publish_status",
            "source_name",
            "source_url_or_doc_id",
        ],
    ]
    payload = {
        "query_id": query_spec["query_id"],
        "query_text": query_spec["query"],
        "metadata_filters": query_spec.get("metadata_filters") or {},
        "query_buckets": query_spec.get("query_buckets") or [],
        "expected_relevant_chunks": relevant_rows.to_dict(orient="records"),
        "retrieved_chunks": final_results.to_dict(orient="records"),
        "failure_source": failure_source,
    }
    if failure_source == "benchmark_labeling":
        payload["diagnosis"] = (
            "No judged relevant chunk or document targets matched the current corpus."
        )
    elif failure_source == "filtering":
        payload["diagnosis"] = (
            "The retriever can find relevant material without filters, but the active metadata "
            "filters exclude it."
        )
    elif failure_source == "ranking":
        payload["diagnosis"] = (
            "Relevant chunks exist in the candidate set, but they do not land in the final top-k."
        )
    elif failure_source == "retrieval":
        payload["diagnosis"] = (
            "The retriever is not surfacing judged relevant chunks even before filter or rerank handling."
        )
    else:
        payload["diagnosis"] = "Query retrieved the expected evidence within the configured top-k."
    return payload


def render_rag_benchmark_summary(summary: dict[str, Any]) -> str:
    """Render a concise markdown summary for retrieval benchmark results."""

    lines = [
        "# RAG Retrieval Benchmark Summary",
        "",
        f"- Corpus chunks: `{summary['corpus_chunk_count']}`",
        f"- Corpus documents: `{summary['corpus_document_count']}`",
        f"- Queries: `{summary['query_count']}`",
        f"- Judged queries: `{summary['judged_query_count']}`",
        f"- Top-k: `{summary['top_k']}`",
        "",
        "## Benchmark Status",
        "",
        f"- Pack status: `{summary['benchmark_pack_status']}`",
        f"- Judgments source: `{summary['judgments_source']}`",
    ]
    if summary["benchmark_warnings"]:
        for warning in summary["benchmark_warnings"]:
            lines.append(f"- Warning: {warning}")
    else:
        lines.append("- Warnings: none.")

    lines.extend(
        [
            "",
            "## Retriever Results",
            "",
            (
                "| Run | Status | Recall@k | MRR | nDCG@k | Citation precision | "
                "Metadata filter correctness | Latency ms | Index size bytes |"
            ),
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in summary["retriever_results"]:
        lines.append(
            "| "
            + " | ".join(
                [
                    row["run_label"],
                    row["status"],
                    _format_metric(row.get("recall_at_k")),
                    _format_metric(row.get("mrr")),
                    _format_metric(row.get("ndcg_at_k")),
                    _format_metric(row.get("citation_precision")),
                    _format_metric(row.get("metadata_filter_correctness")),
                    _format_metric(row.get("latency_ms")),
                    _format_metric(row.get("index_size_bytes")),
                ]
            )
            + " |"
        )
        if row.get("status_reason"):
            lines.append(f"- `{row['run_label']}` skipped: {row['status_reason']}")

    lines.extend(
        [
            "",
            "## Query Bucket Results",
            "",
            "| Bucket | Run | Query count | Recall@k | MRR | nDCG@k | Citation precision | Metadata filter correctness |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    if summary["query_bucket_results"]:
        for row in summary["query_bucket_results"]:
            lines.append(
                "| "
                + " | ".join(
                    [
                        row["bucket_name"],
                        row["run_label"],
                        _format_metric(row.get("query_count")),
                        _format_metric(row.get("recall_at_k")),
                        _format_metric(row.get("mrr")),
                        _format_metric(row.get("ndcg_at_k")),
                        _format_metric(row.get("citation_precision")),
                        _format_metric(row.get("metadata_filter_correctness")),
                    ]
                )
                + " |"
            )
    else:
        lines.append("| - | - | 0 | - | - | - | - | - |")

    lines.extend(
        [
            "",
            "## Ambiguous Query Handling",
            "",
            f"- Ambiguous queries: `{summary['ambiguous_query_count']}`",
            f"- Ambiguous queries with full recall: `{summary['ambiguous_query_success_count']}`",
            "",
            "## Top Failure Categories",
            "",
        ]
    )
    if not summary["top_failure_categories"]:
        lines.append("- None.")
    else:
        for category, count in summary["top_failure_categories"]:
            lines.append(f"- `{category}`: {count}")

    if summary["rerank_results"]:
        lines.extend(
            [
                "",
                "## Rerank Delta",
                "",
                "| Run | Status | MRR delta | nDCG delta | Recall delta | Latency delta ms |",
                "| --- | --- | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in summary["rerank_results"]:
            lines.append(
                "| "
                + " | ".join(
                    [
                        row["run_label"],
                        row["status"],
                        _format_metric(row.get("mrr_delta")),
                        _format_metric(row.get("ndcg_delta")),
                        _format_metric(row.get("recall_delta")),
                        _format_metric(row.get("latency_delta_ms")),
                    ]
                )
                + " |"
            )

    lines.extend(
        [
            "",
            "## Failure Cases",
            "",
        ]
    )
    if summary["failure_case_count"] == 0:
        lines.append("- None.")
    else:
        for failure in summary["failure_cases"]:
            lines.append(
                f"- `{failure['run_label']}` / `{failure['query_id']}`: "
                f"{failure['failure_source'] or 'unknown'}"
                f" - {failure['reason'] or 'retrieval missed one or more judged targets.'}"
            )

    return "\n".join(lines) + "\n"


def render_rag_failure_cases_markdown(failure_cases: list[dict[str, Any]]) -> str:
    """Render a failure-case markdown summary."""

    lines = [
        "# RAG Retrieval Failure Cases",
        "",
    ]
    if not failure_cases:
        lines.append("- None.")
        return "\n".join(lines) + "\n"

    for failure in failure_cases:
        lines.extend(
            [
                f"## {failure['run_label']} / {failure['query_id']}",
                "",
                f"- Query: `{failure['query_text']}`",
                f"- Failure source: `{failure['failure_source'] or 'unknown'}`",
                f"- Recall@k: `{_format_metric(failure.get('recall_at_k'))}`",
                f"- MRR: `{_format_metric(failure.get('mrr'))}`",
                f"- nDCG@k: `{_format_metric(failure.get('ndcg_at_k'))}`",
                f"- Reason: {failure['reason'] or 'Not all judged chunks were retrieved.'}",
                "",
            ]
        )
    return "\n".join(lines) + "\n"


def _resolve_output_root(*, output_root: Path | None, settings: Settings) -> Path:
    resolved = (
        (
            output_root
            if output_root is not None
            else settings.artifacts_dir / DEFAULT_BENCHMARKS_SUBDIR
        )
        .expanduser()
        .resolve()
    )
    for forbidden_root in (
        settings.reports_dir.expanduser().resolve(),
        settings.strategy_dir.expanduser().resolve(),
    ):
        if _is_relative_to(resolved, forbidden_root):
            raise ValueError(
                "RAG benchmark artifacts must not be written under analyst-facing paths like "
                f"{forbidden_root}."
            )
    return resolved


def _load_query_specs(
    *,
    corpus: pd.DataFrame,
    benchmark_path: Path | None,
    benchmark_dir: Path | None,
    settings: Settings,
    output_root: Path,
) -> tuple[list[dict[str, Any]], RagBenchmarkValidationRun | None]:
    if benchmark_dir is not None and benchmark_path is not None:
        raise ValueError("Use either `benchmark_dir` or `benchmark_path`, not both.")
    if benchmark_dir is not None:
        judgments_path = resolve_preferred_judgments_path(benchmark_dir)
        validation = validate_rag_benchmark_pack(
            benchmark_dir=benchmark_dir,
            corpus=corpus,
            settings=settings,
            output_root=output_root / DEFAULT_BENCHMARK_VALIDATION_SUBDIR.name,
            judgments_path=judgments_path,
        )
        if not validation.passed:
            raise ValueError(
                "Benchmark pack validation failed. Resolve the validation errors first. "
                f"See {validation.artifacts.validation_markdown_path}."
            )
        return validation.query_specs, validation
    if benchmark_path is None:
        return [
            _normalize_fixture_query_spec(corpus=corpus, query_spec=dict(query))
            for query in DEFAULT_RETRIEVAL_BENCHMARK
        ], None

    payload = json.loads(benchmark_path.read_text(encoding="utf-8"))
    raw_queries = _coerce_benchmark_query_payload(payload=payload, benchmark_path=benchmark_path)
    return [
        _normalize_fixture_query_spec(corpus=corpus, query_spec=query) for query in raw_queries
    ], None


def _normalize_fixture_query_spec(
    corpus: pd.DataFrame, query_spec: dict[str, Any]
) -> dict[str, Any]:
    relevant_chunk_ids = _resolve_relevant_chunk_ids(corpus, query_spec)
    relevant_doc_ids = _resolve_relevant_doc_ids(query_spec)
    return {
        "query_id": query_spec["query_id"],
        "query": query_spec["query"],
        "language": query_spec.get("language", "en"),
        "notes": query_spec.get("notes"),
        "metadata_filters": query_spec.get("metadata_filters") or {},
        "brand_filter_values": [],
        "metric_filter_values": [],
        "expected_source_kinds": [],
        "publish_status_scope": "all",
        "ambiguity_flag": False,
        "requires_citation": False,
        "query_groups": [],
        "query_buckets": [],
        "relevant_chunk_ids": sorted(relevant_chunk_ids),
        "relevant_doc_ids": sorted(relevant_doc_ids),
        "relevance_by_chunk_id": {chunk_id: 1 for chunk_id in relevant_chunk_ids},
        "relevance_by_doc_id": {doc_id: 1 for doc_id in relevant_doc_ids},
        "rationale_by_chunk_id": {},
        "rationale_by_doc_id": {},
        "must_appear_chunk_ids_in_top_k": {},
        "must_appear_doc_ids_in_top_k": {},
    }


def _coerce_benchmark_query_payload(*, payload: Any, benchmark_path: Path) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        if "queries" not in payload:
            raise ValueError(
                f"Benchmark JSON `{benchmark_path}` must be a list of query objects or an "
                "object containing a `queries` list."
            )
        raw_queries = payload["queries"]
    elif isinstance(payload, list):
        raw_queries = payload
    else:
        raise ValueError(
            f"Benchmark JSON `{benchmark_path}` must be a list of query objects or an object "
            "containing a `queries` list."
        )

    if not isinstance(raw_queries, list):
        raise ValueError(
            f"Benchmark JSON `{benchmark_path}` must provide `queries` as a list of objects."
        )

    normalized_queries: list[dict[str, Any]] = []
    for index, query in enumerate(raw_queries):
        if not isinstance(query, dict):
            raise ValueError(
                f"Benchmark JSON `{benchmark_path}` query at index {index} must be an object."
            )
        normalized_queries.append(dict(query))
    return normalized_queries


def _resolve_relevant_chunk_ids(corpus: pd.DataFrame, query_spec: dict[str, Any]) -> set[str]:
    explicit_chunk_ids = query_spec.get("relevant_chunk_ids") or []
    if explicit_chunk_ids:
        return {str(chunk_id) for chunk_id in explicit_chunk_ids}
    explicit_doc_ids = query_spec.get("relevant_doc_ids") or []
    if explicit_doc_ids:
        return {
            str(chunk_id)
            for chunk_id in corpus.loc[corpus["doc_id"].isin(explicit_doc_ids), "chunk_id"].tolist()
        }
    relevant_filters = (
        query_spec.get("relevant_filters") or query_spec.get("metadata_filters") or {}
    )
    if not relevant_filters:
        return set()
    relevant_rows = [
        row["chunk_id"]
        for row in corpus.to_dict(orient="records")
        if _row_matches_filters(row, relevant_filters)
    ]
    return {str(chunk_id) for chunk_id in relevant_rows}


def _resolve_relevant_doc_ids(query_spec: dict[str, Any]) -> set[str]:
    explicit_doc_ids = query_spec.get("relevant_doc_ids") or []
    return {str(doc_id) for doc_id in explicit_doc_ids}


def _score_query_results(
    *,
    query_spec: dict[str, Any],
    results: pd.DataFrame,
    latency_ms: float,
    top_k: int,
    failure_source: str | None,
    retriever_name: str,
    run_label: str,
    stage: str,
    reranker_name: str | None,
    index_size_bytes: int,
) -> dict[str, Any]:
    relevant_chunk_ids = set(query_spec.get("relevant_chunk_ids") or [])
    relevant_doc_ids = set(query_spec.get("relevant_doc_ids") or [])
    relevance_by_chunk_id = query_spec.get("relevance_by_chunk_id") or {}
    relevance_by_doc_id = query_spec.get("relevance_by_doc_id") or {}
    must_appear_chunk_ids_in_top_k = query_spec.get("must_appear_chunk_ids_in_top_k") or {}
    must_appear_doc_ids_in_top_k = query_spec.get("must_appear_doc_ids_in_top_k") or {}
    total_judged_targets = len(relevant_chunk_ids) + len(relevant_doc_ids)
    if total_judged_targets == 0:
        return _skipped_query_metric(
            query_spec=query_spec,
            retriever_name=retriever_name,
            run_label=run_label,
            stage=stage,
            reranker_name=reranker_name,
            reason="No relevance judgments matched the current corpus.",
            index_size_bytes=index_size_bytes,
        )

    hits = results["chunk_id"].tolist()
    hit_doc_ids = results["doc_id"].tolist()
    satisfied_chunk_ids: set[str] = set()
    satisfied_doc_ids: set[str] = set()
    relevant_ranks: list[int] = []

    for rank, (chunk_id, doc_id) in enumerate(
        zip(hits[:top_k], hit_doc_ids[:top_k], strict=False), start=1
    ):
        target_hit = False
        if chunk_id in relevant_chunk_ids and chunk_id not in satisfied_chunk_ids:
            satisfied_chunk_ids.add(chunk_id)
            target_hit = True
        if doc_id in relevant_doc_ids and doc_id not in satisfied_doc_ids:
            satisfied_doc_ids.add(doc_id)
            target_hit = True
        if target_hit:
            relevant_ranks.append(rank)

    recall = (len(satisfied_chunk_ids) + len(satisfied_doc_ids)) / total_judged_targets
    mrr = (1 / relevant_ranks[0]) if relevant_ranks else 0.0

    dcg = 0.0
    dcg_seen_chunk_ids: set[str] = set()
    dcg_seen_doc_ids: set[str] = set()
    for rank, (chunk_id, doc_id) in enumerate(
        zip(hits[:top_k], hit_doc_ids[:top_k], strict=False), start=1
    ):
        gain = 0
        if chunk_id in relevant_chunk_ids and chunk_id not in dcg_seen_chunk_ids:
            dcg_seen_chunk_ids.add(chunk_id)
            gain += relevance_by_chunk_id.get(chunk_id, 0)
        if doc_id in relevant_doc_ids and doc_id not in dcg_seen_doc_ids:
            dcg_seen_doc_ids.add(doc_id)
            gain += relevance_by_doc_id.get(doc_id, 0)
        if gain > 0:
            dcg += (2**gain - 1) / math.log2(rank + 1)
    ideal_gains = sorted(
        [
            *(gain for gain in relevance_by_chunk_id.values() if gain > 0),
            *(gain for gain in relevance_by_doc_id.values() if gain > 0),
        ],
        reverse=True,
    )
    ideal_dcg = 0.0
    for rank, gain in enumerate(ideal_gains[:top_k], start=1):
        ideal_dcg += (2**gain - 1) / math.log2(rank + 1)
    ndcg = dcg / ideal_dcg if ideal_dcg else None

    citation_precision = float(results["citation_present"].mean()) if not results.empty else 0.0
    metadata_filter_correctness = (
        float(results["filter_match"].mean())
        if query_spec.get("metadata_filters") and not results.empty
        else None
    )
    must_appear_chunk_violations = [
        chunk_id
        for chunk_id, threshold in must_appear_chunk_ids_in_top_k.items()
        if chunk_id not in hits or hits.index(chunk_id) + 1 > threshold
    ]
    must_appear_doc_violations = [
        doc_id
        for doc_id, threshold in must_appear_doc_ids_in_top_k.items()
        if doc_id not in hit_doc_ids or hit_doc_ids.index(doc_id) + 1 > threshold
    ]
    must_appear_violation_count = len(must_appear_chunk_violations) + len(
        must_appear_doc_violations
    )
    status = "ok"
    reasons: list[str] = []
    if recall < 1.0:
        status = "warning"
        reasons.append("Not all judged chunk or document targets were retrieved in the top-k.")
    if must_appear_violation_count:
        status = "warning"
        reasons.append("One or more must-appear judgments missed their required top-k threshold.")
    if metadata_filter_correctness is not None and metadata_filter_correctness < 1.0:
        status = "warning"
        reasons.append("Retrieved rows did not all satisfy the benchmark metadata filters.")

    return {
        "query_id": query_spec["query_id"],
        "query_text": query_spec["query"],
        "run_label": run_label,
        "retriever_name": retriever_name,
        "stage": stage,
        "reranker_name": reranker_name,
        "status": status,
        "status_reason": " ".join(reasons) if reasons else None,
        "recall_at_k": round(recall, 6),
        "mrr": round(mrr, 6),
        "ndcg_at_k": round(float(ndcg), 6) if ndcg is not None else None,
        "citation_precision": round(citation_precision, 6),
        "metadata_filter_correctness": (
            round(metadata_filter_correctness, 6)
            if metadata_filter_correctness is not None
            else None
        ),
        "latency_ms": round(latency_ms, 3),
        "failure_source": failure_source,
        "query_buckets": query_spec.get("query_buckets") or [],
        "ambiguity_flag": bool(query_spec.get("ambiguity_flag")),
        "judged_relevant_count": total_judged_targets,
        "must_appear_violation_count": must_appear_violation_count,
        "index_size_bytes": index_size_bytes,
    }


def _annotate_result_rows(
    *,
    results: pd.DataFrame,
    query_spec: dict[str, Any],
    query_metric: dict[str, Any],
    run_label: str,
    stage: str,
    reranker_name: str | None,
) -> pd.DataFrame:
    if results.empty:
        return pd.DataFrame(columns=_result_columns())
    annotated = results.copy()
    annotated["query_id"] = query_spec["query_id"]
    annotated["query_text"] = query_spec["query"]
    annotated["is_relevant"] = annotated.apply(
        lambda row: row["chunk_id"] in set(query_spec.get("relevant_chunk_ids") or [])
        or row["doc_id"] in set(query_spec.get("relevant_doc_ids") or []),
        axis=1,
    )
    annotated["relevance_label"] = annotated["chunk_id"].map(
        lambda chunk_id: query_spec["relevance_by_chunk_id"].get(chunk_id, 0)
    )
    annotated["relevance_label"] = annotated.apply(
        lambda row: max(
            int(row["relevance_label"]),
            int((query_spec.get("relevance_by_doc_id") or {}).get(row["doc_id"], 0)),
        ),
        axis=1,
    )
    annotated["metadata_filters_json"] = json.dumps(
        query_spec.get("metadata_filters") or {},
        ensure_ascii=False,
        sort_keys=True,
    )
    annotated["query_latency_ms"] = query_metric["latency_ms"]
    annotated["run_label"] = run_label
    annotated["stage"] = stage
    annotated["reranker_name"] = reranker_name
    annotated["failure_source"] = query_metric.get("failure_source")
    annotated["query_buckets_json"] = json.dumps(
        query_spec.get("query_buckets") or [],
        ensure_ascii=False,
    )
    return annotated[_result_columns()]


def _diagnose_failure_source(
    *,
    corpus: pd.DataFrame,
    query_spec: dict[str, Any],
    final_results: pd.DataFrame,
    filtered_candidates: pd.DataFrame,
    prepared_retriever: Any,
    retriever_name: str,
    allow_model_download: bool,
    diagnostic_top_k: int,
) -> str | None:
    relevant_chunk_ids = set(query_spec.get("relevant_chunk_ids") or [])
    relevant_doc_ids = set(query_spec.get("relevant_doc_ids") or [])
    if not relevant_chunk_ids and not relevant_doc_ids:
        return "benchmark_labeling"
    final_hit_chunk_ids = final_results["chunk_id"].tolist()
    final_hit_doc_ids = final_results["doc_id"].tolist()
    must_appear_chunk_ids_in_top_k = query_spec.get("must_appear_chunk_ids_in_top_k") or {}
    must_appear_doc_ids_in_top_k = query_spec.get("must_appear_doc_ids_in_top_k") or {}
    must_appear_violation = any(
        chunk_id not in final_hit_chunk_ids or final_hit_chunk_ids.index(chunk_id) + 1 > threshold
        for chunk_id, threshold in must_appear_chunk_ids_in_top_k.items()
    ) or any(
        doc_id not in final_hit_doc_ids or final_hit_doc_ids.index(doc_id) + 1 > threshold
        for doc_id, threshold in must_appear_doc_ids_in_top_k.items()
    )
    satisfied_chunks = relevant_chunk_ids.intersection(set(final_hit_chunk_ids))
    satisfied_docs = relevant_doc_ids.intersection(set(final_hit_doc_ids))
    if (
        len(satisfied_chunks) + len(satisfied_docs)
        == len(relevant_chunk_ids) + len(relevant_doc_ids)
        and not must_appear_violation
    ):
        return None

    unfiltered_run = rag_search(
        corpus=corpus,
        query=query_spec["query"],
        top_k=diagnostic_top_k,
        retriever_name=retriever_name,
        metadata_filters={},
        allow_model_download=allow_model_download,
        prepared_retriever=prepared_retriever,
    )
    if unfiltered_run.status != "ok":
        return "retrieval"

    unfiltered_hits = set(unfiltered_run.results["chunk_id"].tolist())
    unfiltered_doc_hits = set(unfiltered_run.results["doc_id"].tolist())
    filtered_hits = set(filtered_candidates["chunk_id"].tolist())
    filtered_doc_hits = set(filtered_candidates["doc_id"].tolist())
    if not relevant_chunk_ids.intersection(unfiltered_hits) and not relevant_doc_ids.intersection(
        unfiltered_doc_hits
    ):
        return "retrieval"
    if query_spec.get("metadata_filters") and (
        not relevant_chunk_ids.intersection(filtered_hits)
        and not relevant_doc_ids.intersection(filtered_doc_hits)
    ):
        return "filtering"
    return "ranking"


def _skipped_query_metric(
    *,
    query_spec: dict[str, Any],
    retriever_name: str,
    run_label: str,
    stage: str,
    reranker_name: str | None,
    reason: str | None,
    index_size_bytes: int = 0,
) -> dict[str, Any]:
    return {
        "query_id": query_spec["query_id"],
        "query_text": query_spec["query"],
        "run_label": run_label,
        "retriever_name": retriever_name,
        "stage": stage,
        "reranker_name": reranker_name,
        "status": "skipped",
        "status_reason": reason,
        "recall_at_k": None,
        "mrr": None,
        "ndcg_at_k": None,
        "citation_precision": None,
        "metadata_filter_correctness": None,
        "latency_ms": None,
        "failure_source": None,
        "query_buckets": query_spec.get("query_buckets") or [],
        "ambiguity_flag": bool(query_spec.get("ambiguity_flag")),
        "judged_relevant_count": len(query_spec.get("relevant_chunk_ids") or [])
        + len(query_spec.get("relevant_doc_ids") or []),
        "must_appear_violation_count": 0,
        "index_size_bytes": index_size_bytes,
    }


def _aggregate_query_metrics(
    *,
    retriever_name: str,
    run_label: str,
    stage: str,
    reranker_name: str | None,
    query_metrics: list[dict[str, Any]],
    index_size_bytes: int,
) -> dict[str, Any]:
    evaluable = [row for row in query_metrics if row["status"] != "skipped"]
    skipped_reasons = [
        row["status_reason"]
        for row in query_metrics
        if row["status"] == "skipped" and row.get("status_reason")
    ]
    return {
        "retriever_name": retriever_name,
        "run_label": run_label,
        "stage": stage,
        "reranker_name": reranker_name,
        "status": "ok" if evaluable else "skipped",
        "status_reason": None
        if evaluable
        else (
            skipped_reasons[0]
            if skipped_reasons
            else "No evaluable benchmark queries matched the corpus or reranker was unavailable."
        ),
        "query_count": len(query_metrics),
        "evaluable_query_count": len(evaluable),
        "recall_at_k": _average_metric(evaluable, "recall_at_k"),
        "mrr": _average_metric(evaluable, "mrr"),
        "ndcg_at_k": _average_metric(evaluable, "ndcg_at_k"),
        "citation_precision": _average_metric(evaluable, "citation_precision"),
        "metadata_filter_correctness": _average_metric(evaluable, "metadata_filter_correctness"),
        "latency_ms": _average_metric(evaluable, "latency_ms"),
        "index_size_bytes": index_size_bytes,
        "ambiguous_query_count": len([row for row in query_metrics if row["ambiguity_flag"]]),
    }


def _aggregate_query_bucket_metrics(
    *,
    query_metrics: list[dict[str, Any]],
    run_label: str,
    stage: str,
) -> list[dict[str, Any]]:
    bucket_rows: list[dict[str, Any]] = []
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in query_metrics:
        for bucket in row.get("query_buckets") or []:
            buckets.setdefault(bucket, []).append(row)
    for bucket_name in sorted(buckets):
        bucket_rows.append(
            {
                "bucket_name": bucket_name,
                "run_label": run_label,
                "stage": stage,
                "query_count": len(buckets[bucket_name]),
                "judged_query_count": len(
                    [row for row in buckets[bucket_name] if row["status"] != "skipped"]
                ),
                "recall_at_k": _average_metric(buckets[bucket_name], "recall_at_k"),
                "mrr": _average_metric(buckets[bucket_name], "mrr"),
                "ndcg_at_k": _average_metric(buckets[bucket_name], "ndcg_at_k"),
                "citation_precision": _average_metric(
                    buckets[bucket_name],
                    "citation_precision",
                ),
                "metadata_filter_correctness": _average_metric(
                    buckets[bucket_name],
                    "metadata_filter_correctness",
                ),
            }
        )
    return bucket_rows


def _rerank_delta_row(
    *,
    retrieval_metric: dict[str, Any],
    reranked_metric: dict[str, Any],
    reranker_name: str,
) -> dict[str, Any]:
    return {
        "retriever_name": retrieval_metric["retriever_name"],
        "reranker_name": reranker_name,
        "query_id": retrieval_metric["query_id"],
        "run_label": reranked_metric["run_label"],
        "status": reranked_metric["status"],
        "status_reason": reranked_metric["status_reason"],
        "mrr_delta": _delta(retrieval_metric.get("mrr"), reranked_metric.get("mrr")),
        "ndcg_delta": _delta(
            retrieval_metric.get("ndcg_at_k"),
            reranked_metric.get("ndcg_at_k"),
        ),
        "recall_delta": _delta(
            retrieval_metric.get("recall_at_k"),
            reranked_metric.get("recall_at_k"),
        ),
        "latency_delta_ms": _delta(
            retrieval_metric.get("latency_ms"),
            reranked_metric.get("latency_ms"),
        ),
    }


def _delta(before: float | None, after: float | None) -> float | None:
    if before is None or after is None:
        return None
    return round(float(after) - float(before), 6)


def _failure_case_from_metric(metric: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_label": metric["run_label"],
        "retriever_name": metric["retriever_name"],
        "stage": metric["stage"],
        "reranker_name": metric["reranker_name"],
        "query_id": metric["query_id"],
        "query_text": metric["query_text"],
        "reason": metric["status_reason"],
        "recall_at_k": metric["recall_at_k"],
        "mrr": metric["mrr"],
        "ndcg_at_k": metric["ndcg_at_k"],
        "failure_source": metric["failure_source"],
        "query_buckets": metric["query_buckets"],
        "ambiguity_flag": metric["ambiguity_flag"],
    }


def _skipped_metrics_row(
    *,
    retriever_name: str,
    run_label: str,
    stage: str,
    query_count: int,
    reason: str | None,
    index_size_bytes: int,
) -> dict[str, Any]:
    return {
        "retriever_name": retriever_name,
        "run_label": run_label,
        "stage": stage,
        "reranker_name": None,
        "status": "skipped",
        "status_reason": reason,
        "query_count": query_count,
        "evaluable_query_count": 0,
        "recall_at_k": None,
        "mrr": None,
        "ndcg_at_k": None,
        "citation_precision": None,
        "metadata_filter_correctness": None,
        "latency_ms": None,
        "index_size_bytes": index_size_bytes,
        "ambiguous_query_count": 0,
    }


def _average_metric(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _build_summary(
    *,
    corpus: pd.DataFrame,
    queries: list[dict[str, Any]],
    metrics: pd.DataFrame,
    query_bucket_metrics: pd.DataFrame,
    failure_cases: list[dict[str, Any]],
    rerank_delta: pd.DataFrame,
    query_metrics: list[dict[str, Any]],
    top_k: int,
    query_count: int,
    judged_query_count: int,
    benchmark_path: Path | None,
    benchmark_dir: Path | None,
    corpus_path: Path,
    validation: RagBenchmarkValidationRun | None,
) -> dict[str, Any]:
    failure_category_counts: dict[str, int] = {}
    for failure in failure_cases:
        category = failure["failure_source"] or "unknown"
        failure_category_counts[category] = failure_category_counts.get(category, 0) + 1
    ambiguous_query_ids = {
        query["query_id"] for query in queries if bool(query.get("ambiguity_flag"))
    }
    ambiguous_query_success_count = len(
        {
            row["query_id"]
            for row in query_metrics
            if row.get("ambiguity_flag")
            and row.get("stage") == "retrieval"
            and row["status"] == "ok"
        }
    )
    benchmark_pack_status = (
        validation.pack.metadata.get("pack_status", "draft")
        if validation is not None
        else "fixture"
    )
    judgments_source = validation.pack.judgments_path.name if validation is not None else "fixture"
    benchmark_warnings: list[str] = []
    if validation is not None:
        if judgments_source != ADJUDICATED_JUDGMENTS_FILENAME:
            benchmark_warnings.append(
                "This benchmark run is using draft or single-reviewer judgments instead of an adjudicated pack."
            )
            provisional_adjudicated_path = (
                validation.pack.benchmark_dir / ADJUDICATED_JUDGMENTS_FILENAME
            )
            if provisional_adjudicated_path.exists():
                benchmark_warnings.append(
                    "A provisional adjudicated_judgments.csv exists, but it is being ignored because the pack is not truly adjudicated."
                )
        if benchmark_pack_status != "adjudicated":
            benchmark_warnings.append(
                f"Benchmark pack status is `{benchmark_pack_status}`; treat retrieval metrics as provisional."
            )
    return {
        "built_at_utc": datetime.now(UTC).isoformat(),
        "benchmark_path": str(benchmark_path) if benchmark_path is not None else None,
        "benchmark_dir": str(benchmark_dir) if benchmark_dir is not None else "default_fixture",
        "benchmark_validation_path": (
            str(validation.artifacts.validation_markdown_path) if validation is not None else None
        ),
        "corpus_path": str(corpus_path),
        "corpus_chunk_count": int(len(corpus)),
        "corpus_document_count": int(corpus["doc_id"].nunique()) if not corpus.empty else 0,
        "query_count": query_count,
        "judged_query_count": judged_query_count,
        "top_k": top_k,
        "retriever_results": metrics.to_dict(orient="records"),
        "query_bucket_results": query_bucket_metrics.to_dict(orient="records"),
        "failure_case_count": len(failure_cases),
        "failure_cases": failure_cases,
        "top_failure_categories": sorted(
            failure_category_counts.items(),
            key=lambda item: (-item[1], item[0]),
        ),
        "ambiguous_query_count": len(ambiguous_query_ids),
        "ambiguous_query_success_count": ambiguous_query_success_count,
        "rerank_results": rerank_delta.to_dict(orient="records"),
        "benchmark_pack_status": benchmark_pack_status,
        "judgments_source": judgments_source,
        "benchmark_warnings": benchmark_warnings,
    }


def _write_outputs(
    *,
    output_root: Path,
    queries: list[dict[str, Any]],
    results: pd.DataFrame,
    metrics: pd.DataFrame,
    query_bucket_metrics: pd.DataFrame,
    rerank_delta: pd.DataFrame,
    summary: dict[str, Any],
    failure_cases: list[dict[str, Any]],
) -> RagBenchmarkArtifacts:
    metrics_json_path = output_root / "metrics.json"
    metrics_csv_path = output_root / "metrics.csv"
    results_parquet_path = output_root / "per_query_results.parquet"
    summary_markdown_path = output_root / "summary.md"
    failure_cases_markdown_path = output_root / "failure_cases.md"
    benchmark_queries_json_path = output_root / "benchmark_queries.json"
    query_bucket_metrics_csv_path = output_root / "query_bucket_metrics.csv"
    rerank_delta_csv_path = output_root / "rerank_delta.csv" if not rerank_delta.empty else None

    metrics_json_path.write_text(
        json.dumps(metrics.to_dict(orient="records"), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    metrics.to_csv(metrics_csv_path, index=False)
    results.to_parquet(results_parquet_path, index=False)
    summary_markdown_path.write_text(render_rag_benchmark_summary(summary), encoding="utf-8")
    failure_cases_markdown_path.write_text(
        render_rag_failure_cases_markdown(failure_cases),
        encoding="utf-8",
    )
    benchmark_queries_json_path.write_text(
        json.dumps(queries, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    query_bucket_metrics.to_csv(query_bucket_metrics_csv_path, index=False)
    if rerank_delta_csv_path is not None:
        rerank_delta.to_csv(rerank_delta_csv_path, index=False)

    return RagBenchmarkArtifacts(
        metrics_json_path=metrics_json_path,
        metrics_csv_path=metrics_csv_path,
        results_parquet_path=results_parquet_path,
        summary_markdown_path=summary_markdown_path,
        failure_cases_markdown_path=failure_cases_markdown_path,
        benchmark_queries_json_path=benchmark_queries_json_path,
        query_bucket_metrics_csv_path=query_bucket_metrics_csv_path,
        rerank_delta_csv_path=rerank_delta_csv_path,
    )


def _result_columns() -> list[str]:
    return [
        "query_id",
        "query_text",
        "run_label",
        "stage",
        "reranker_name",
        "retriever_name",
        "rank",
        "score",
        "doc_id",
        "chunk_id",
        "source_kind",
        "title",
        "text",
        "artifact_path",
        "brand_names",
        "metric_names",
        "as_of_date",
        "publish_status",
        "confidence_score",
        "source_name",
        "source_url_or_doc_id",
        "metadata_json",
        "filter_match",
        "citation_present",
        "is_relevant",
        "relevance_label",
        "metadata_filters_json",
        "query_latency_ms",
        "failure_source",
        "query_buckets_json",
    ]


def _row_matches_filters(row: dict[str, Any], metadata_filters: dict[str, Any]) -> bool:
    for key, expected in metadata_filters.items():
        actual = row.get(key)
        if actual is None:
            actual = _metadata_json(row).get(key)
        if key in {"brand_names", "metric_names"}:
            if not _list_matches(_json_list(actual), expected):
                return False
            continue
        if isinstance(expected, list | tuple | set):
            if str(actual) not in {str(value) for value in expected}:
                return False
        elif str(actual) != str(expected):
            return False
    return True


def _metadata_json(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("metadata_json")
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _json_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return [stripped]
        if isinstance(parsed, list):
            return [str(item) for item in parsed if str(item).strip()]
        return [str(parsed)]
    return [str(value)]


def _list_matches(actual: list[str], expected: Any) -> bool:
    if isinstance(expected, list | tuple | set):
        expected_values = {str(value) for value in expected}
        return bool(expected_values.intersection({str(value) for value in actual}))
    return str(expected) in {str(value) for value in actual}


def _format_metric(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def _is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
    except ValueError:
        return False
    return True


__all__ = [
    "DEFAULT_RETRIEVAL_BENCHMARK",
    "RagBenchmarkArtifacts",
    "RagBenchmarkRun",
    "eval_rag_retrieval",
    "inspect_rag_benchmark_query",
    "render_rag_benchmark_summary",
]
