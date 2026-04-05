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
from qsr_audit.rag.corpus import load_rag_corpus
from qsr_audit.rag.retrieval import prepare_retriever, rag_search

DEFAULT_BENCHMARKS_SUBDIR = Path("rag/benchmarks")
DEFAULT_CORPUS_PATH = Path("artifacts/rag/corpus/corpus.parquet")
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
    failure_cases_json_path: Path
    benchmark_queries_json_path: Path


@dataclass(frozen=True)
class RagBenchmarkRun:
    """Result of evaluating retrieval baselines against a benchmark fixture."""

    corpus: pd.DataFrame
    queries: list[dict[str, Any]]
    results: pd.DataFrame
    metrics: pd.DataFrame
    summary: dict[str, Any]
    failure_cases: list[dict[str, Any]]
    artifacts: RagBenchmarkArtifacts


def eval_rag_retrieval(
    *,
    settings: Settings | None = None,
    corpus_path: Path | None = None,
    benchmark_path: Path | None = None,
    output_root: Path | None = None,
    retrievers: tuple[str, ...] = ("bm25",),
    top_k: int = 5,
    allow_model_download: bool = False,
) -> RagBenchmarkRun:
    """Run retrieval-only evaluation over a local benchmark fixture."""

    resolved_settings = settings or Settings()
    resolved_corpus_path = (
        corpus_path if corpus_path is not None else resolved_settings.artifacts_dir / DEFAULT_CORPUS_PATH
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
    queries = _load_benchmark_queries(benchmark_path)

    results_frames: list[pd.DataFrame] = []
    metrics_rows: list[dict[str, Any]] = []
    failure_cases: list[dict[str, Any]] = []

    for retriever_name in retrievers:
        prepared = prepare_retriever(
            corpus=corpus,
            retriever_name=retriever_name,
            allow_model_download=allow_model_download,
        )
        first_search = rag_search(
            corpus=corpus,
            query="",
            top_k=top_k,
            retriever_name=retriever_name,
            metadata_filters={},
            allow_model_download=allow_model_download,
            prepared_retriever=prepared,
        )
        if first_search.status != "ok":
            metrics_rows.append(
                {
                    "retriever_name": retriever_name,
                    "status": "skipped",
                    "status_reason": first_search.reason,
                    "query_count": len(queries),
                    "evaluable_query_count": 0,
                    "recall_at_k": None,
                    "mrr": None,
                    "ndcg_at_k": None,
                    "citation_precision": None,
                    "metadata_filter_correctness": None,
                    "latency_ms": None,
                    "index_size_bytes": 0,
                }
            )
            continue

        per_query_rows: list[dict[str, Any]] = []
        per_query_metrics: list[dict[str, Any]] = []

        for query_spec in queries:
            search_run = rag_search(
                corpus=corpus,
                query=query_spec["query"],
                top_k=top_k,
                retriever_name=retriever_name,
                metadata_filters=query_spec.get("metadata_filters") or {},
                allow_model_download=allow_model_download,
                prepared_retriever=prepared,
            )
            search_results = search_run.results.copy()
            relevant_chunk_ids = _resolve_relevant_chunk_ids(corpus, query_spec)
            query_metrics = _score_query_results(
                query_id=query_spec["query_id"],
                query=query_spec["query"],
                results=search_results,
                relevant_chunk_ids=relevant_chunk_ids,
                metadata_filters=query_spec.get("metadata_filters") or {},
                latency_ms=search_run.latency_ms,
                top_k=top_k,
            )
            per_query_metrics.append(query_metrics)
            if search_results.empty:
                enriched_results = pd.DataFrame(columns=_result_columns())
            else:
                search_results["query_id"] = query_spec["query_id"]
                search_results["query_text"] = query_spec["query"]
                search_results["is_relevant"] = search_results["chunk_id"].isin(relevant_chunk_ids)
                search_results["metadata_filters_json"] = json.dumps(
                    query_spec.get("metadata_filters") or {},
                    ensure_ascii=False,
                    sort_keys=True,
                )
                search_results["query_latency_ms"] = search_run.latency_ms
                enriched_results = search_results[_result_columns()]
            per_query_rows.append(enriched_results)

            if query_metrics["status"] != "ok" or (query_metrics["recall_at_k"] or 0.0) < 1.0:
                failure_cases.append(
                    {
                        "retriever_name": retriever_name,
                        "query_id": query_spec["query_id"],
                        "query": query_spec["query"],
                        "status": query_metrics["status"],
                        "reason": query_metrics["status_reason"],
                        "recall_at_k": query_metrics["recall_at_k"],
                        "mrr": query_metrics["mrr"],
                        "ndcg_at_k": query_metrics["ndcg_at_k"],
                    }
                )

        retriever_metrics = _aggregate_query_metrics(
            retriever_name=retriever_name,
            query_metrics=per_query_metrics,
            index_size_bytes=first_search.index_size_bytes,
        )
        metrics_rows.append(retriever_metrics)
        if per_query_rows:
            results_frames.append(
                pd.concat(per_query_rows, ignore_index=True)
                if any(not frame.empty for frame in per_query_rows)
                else pd.DataFrame(columns=_result_columns())
            )

    results = (
        pd.concat(results_frames, ignore_index=True)
        if results_frames and any(not frame.empty for frame in results_frames)
        else pd.DataFrame(columns=_result_columns())
    )
    metrics = pd.DataFrame(metrics_rows)
    summary = _build_summary(
        corpus=corpus,
        metrics=metrics,
        failure_cases=failure_cases,
        top_k=top_k,
        query_count=len(queries),
        benchmark_path=benchmark_path,
        corpus_path=resolved_corpus_path,
    )
    artifacts = _write_outputs(
        output_root=resolved_output_root,
        queries=queries,
        results=results,
        metrics=metrics,
        summary=summary,
        failure_cases=failure_cases,
    )
    return RagBenchmarkRun(
        corpus=corpus,
        queries=queries,
        results=results,
        metrics=metrics,
        summary=summary,
        failure_cases=failure_cases,
        artifacts=artifacts,
    )


def render_rag_benchmark_summary(summary: dict[str, Any]) -> str:
    """Render a concise markdown summary for retrieval benchmark results."""

    lines = [
        "# RAG Retrieval Benchmark Summary",
        "",
        f"- Corpus chunks: `{summary['corpus_chunk_count']}`",
        f"- Corpus documents: `{summary['corpus_document_count']}`",
        f"- Queries: `{summary['query_count']}`",
        f"- Top-k: `{summary['top_k']}`",
        "",
        "## Retriever Results",
        "",
        (
            "| Retriever | Status | Recall@k | MRR | nDCG@k | Citation precision | "
            "Metadata filter correctness | Latency ms | Index size bytes |"
        ),
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary["retriever_results"]:
        lines.append(
            "| "
            + " | ".join(
                [
                    row["retriever_name"],
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
            lines.append(f"- `{row['retriever_name']}` skipped: {row['status_reason']}")

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
                f"- `{failure['retriever_name']}` / `{failure['query_id']}`: "
                f"{failure['reason'] or 'retrieval missed one or more judged chunks.'}"
            )

    return "\n".join(lines) + "\n"


def _resolve_output_root(*, output_root: Path | None, settings: Settings) -> Path:
    resolved = (
        output_root
        if output_root is not None
        else settings.artifacts_dir / DEFAULT_BENCHMARKS_SUBDIR
    ).expanduser().resolve()
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


def _load_benchmark_queries(benchmark_path: Path | None) -> list[dict[str, Any]]:
    if benchmark_path is None:
        return [dict(query) for query in DEFAULT_RETRIEVAL_BENCHMARK]
    payload = json.loads(benchmark_path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return list(payload.get("queries", []))
    if isinstance(payload, list):
        return list(payload)
    raise ValueError("Benchmark file must be a JSON list or a JSON object with a `queries` key.")


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
    relevant_filters = query_spec.get("relevant_filters") or query_spec.get("metadata_filters") or {}
    if not relevant_filters:
        return set()
    relevant_rows = [
        row["chunk_id"]
        for row in corpus.to_dict(orient="records")
        if _row_matches_filters(row, relevant_filters)
    ]
    return {str(chunk_id) for chunk_id in relevant_rows}


def _score_query_results(
    *,
    query_id: str,
    query: str,
    results: pd.DataFrame,
    relevant_chunk_ids: set[str],
    metadata_filters: dict[str, Any],
    latency_ms: float,
    top_k: int,
) -> dict[str, Any]:
    if not relevant_chunk_ids:
        return {
            "query_id": query_id,
            "query": query,
            "status": "skipped",
            "status_reason": "No relevance judgments matched the current corpus.",
            "recall_at_k": None,
            "mrr": None,
            "ndcg_at_k": None,
            "citation_precision": None,
            "metadata_filter_correctness": None,
            "latency_ms": latency_ms,
        }
    hits = results["chunk_id"].tolist()
    relevant_ranks = [
        rank
        for rank, chunk_id in enumerate(hits[:top_k], start=1)
        if chunk_id in relevant_chunk_ids
    ]
    recall = len(relevant_ranks) / len(relevant_chunk_ids)
    mrr = (1 / relevant_ranks[0]) if relevant_ranks else 0.0
    dcg = sum(1 / math.log2(rank + 1) for rank in relevant_ranks)
    ideal_hits = min(len(relevant_chunk_ids), top_k)
    ideal_dcg = sum(1 / math.log2(rank + 1) for rank in range(1, ideal_hits + 1))
    ndcg = dcg / ideal_dcg if ideal_dcg else None
    citation_precision = (
        float(results["citation_present"].mean()) if not results.empty else 0.0
    )
    metadata_filter_correctness = (
        float(results["filter_match"].mean()) if metadata_filters and not results.empty else None
    )
    return {
        "query_id": query_id,
        "query": query,
        "status": "ok" if recall == 1.0 else "warning",
        "status_reason": None if recall == 1.0 else "Not all judged chunks were retrieved in the top-k.",
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
    }


def _aggregate_query_metrics(
    *,
    retriever_name: str,
    query_metrics: list[dict[str, Any]],
    index_size_bytes: int,
) -> dict[str, Any]:
    evaluable = [row for row in query_metrics if row["status"] != "skipped"]
    return {
        "retriever_name": retriever_name,
        "status": "ok" if evaluable else "skipped",
        "status_reason": None if evaluable else "No evaluable benchmark queries matched the corpus.",
        "query_count": len(query_metrics),
        "evaluable_query_count": len(evaluable),
        "recall_at_k": _average_metric(evaluable, "recall_at_k"),
        "mrr": _average_metric(evaluable, "mrr"),
        "ndcg_at_k": _average_metric(evaluable, "ndcg_at_k"),
        "citation_precision": _average_metric(evaluable, "citation_precision"),
        "metadata_filter_correctness": _average_metric(evaluable, "metadata_filter_correctness"),
        "latency_ms": _average_metric(evaluable, "latency_ms"),
        "index_size_bytes": index_size_bytes,
    }


def _average_metric(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _build_summary(
    *,
    corpus: pd.DataFrame,
    metrics: pd.DataFrame,
    failure_cases: list[dict[str, Any]],
    top_k: int,
    query_count: int,
    benchmark_path: Path | None,
    corpus_path: Path,
) -> dict[str, Any]:
    return {
        "built_at_utc": datetime.now(UTC).isoformat(),
        "benchmark_path": str(benchmark_path) if benchmark_path is not None else "default_fixture",
        "corpus_path": str(corpus_path),
        "corpus_chunk_count": int(len(corpus)),
        "corpus_document_count": int(corpus["doc_id"].nunique()) if not corpus.empty else 0,
        "query_count": query_count,
        "top_k": top_k,
        "retriever_results": metrics.to_dict(orient="records"),
        "failure_case_count": len(failure_cases),
        "failure_cases": failure_cases,
    }


def _write_outputs(
    *,
    output_root: Path,
    queries: list[dict[str, Any]],
    results: pd.DataFrame,
    metrics: pd.DataFrame,
    summary: dict[str, Any],
    failure_cases: list[dict[str, Any]],
) -> RagBenchmarkArtifacts:
    metrics_json_path = output_root / "retrieval_metrics.json"
    metrics_csv_path = output_root / "retrieval_metrics.csv"
    results_parquet_path = output_root / "retrieval_results.parquet"
    summary_markdown_path = output_root / "retrieval_summary.md"
    failure_cases_json_path = output_root / "failure_cases.json"
    benchmark_queries_json_path = output_root / "benchmark_queries.json"

    metrics_json_path.write_text(
        json.dumps(metrics.to_dict(orient="records"), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    metrics.to_csv(metrics_csv_path, index=False)
    results.to_parquet(results_parquet_path, index=False)
    summary_markdown_path.write_text(render_rag_benchmark_summary(summary), encoding="utf-8")
    failure_cases_json_path.write_text(
        json.dumps(failure_cases, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    benchmark_queries_json_path.write_text(
        json.dumps(queries, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return RagBenchmarkArtifacts(
        metrics_json_path=metrics_json_path,
        metrics_csv_path=metrics_csv_path,
        results_parquet_path=results_parquet_path,
        summary_markdown_path=summary_markdown_path,
        failure_cases_json_path=failure_cases_json_path,
        benchmark_queries_json_path=benchmark_queries_json_path,
    )


def _result_columns() -> list[str]:
    return [
        "query_id",
        "query_text",
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
        "metadata_filters_json",
        "query_latency_ms",
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
        if isinstance(expected, (list, tuple, set)):
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
    if isinstance(expected, (list, tuple, set)):
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
    "render_rag_benchmark_summary",
]
