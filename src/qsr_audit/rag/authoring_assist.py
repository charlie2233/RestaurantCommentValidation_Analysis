"""Analyst-assist helpers for seeding, triage, and hard-negative review."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import combinations
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.config import Settings
from qsr_audit.rag.benchmark_pack import load_rag_benchmark_pack
from qsr_audit.rag.corpus import load_rag_corpus, resolve_rag_corpus_path
from qsr_audit.rag.retrieval import prepare_retriever, rag_search, row_matches_filters

DEFAULT_QUERY_SEED_LIMIT_PER_GROUP = 24
DEFAULT_HARD_NEGATIVE_SEARCH_TOP_K = 10
DEFAULT_FAILURE_TRIAGE_FILENAME = "failure_triage.csv"
DEFAULT_FAILURE_TRIAGE_MARKDOWN = "failure_triage.md"
DEFAULT_FAILURE_TRIAGE_JSON = "failure_triage.json"
DEFAULT_HARD_NEGATIVE_SUMMARY_MARKDOWN = "hard_negative_summary.md"
SUGGESTED_QUERIES_FILENAME = "suggested_queries.csv"
SUGGESTED_QUERIES_MARKDOWN_FILENAME = "suggested_queries.md"
HARD_NEGATIVE_SUGGESTIONS_FILENAME = "hard_negative_suggestions.csv"


@dataclass(frozen=True)
class RagQuerySeedArtifacts:
    suggested_queries_csv_path: Path
    suggested_queries_markdown_path: Path


@dataclass(frozen=True)
class RagQuerySeedRun:
    benchmark_dir: Path
    suggestion_count: int
    suggestions: pd.DataFrame
    artifacts: RagQuerySeedArtifacts


@dataclass(frozen=True)
class RagHardNegativeMiningArtifacts:
    suggestions_csv_path: Path
    summary_markdown_path: Path


@dataclass(frozen=True)
class RagHardNegativeMiningRun:
    benchmark_dir: Path
    run_dir: Path
    suggestions: pd.DataFrame
    summary: dict[str, Any]
    artifacts: RagHardNegativeMiningArtifacts


@dataclass(frozen=True)
class RagFailureTriageArtifacts:
    triage_csv_path: Path
    triage_markdown_path: Path
    triage_json_path: Path


@dataclass(frozen=True)
class RagFailureTriageRun:
    benchmark_dir: Path
    run_dir: Path
    triage_rows: pd.DataFrame
    summary: dict[str, Any]
    artifacts: RagFailureTriageArtifacts


def seed_rag_queries(
    *,
    benchmark_dir: Path,
    settings: Settings | None = None,
    corpus_path: Path | None = None,
) -> RagQuerySeedRun:
    """Generate deterministic analyst-style query suggestions from the local corpus."""

    resolved_settings = settings or Settings()
    resolved_benchmark_dir = benchmark_dir.expanduser().resolve()
    resolved_corpus_path = resolve_rag_corpus_path(
        settings=resolved_settings,
        corpus_path=corpus_path,
    )
    if not resolved_corpus_path.exists():
        raise FileNotFoundError(
            "Query seeding requires an existing corpus parquet. Run `qsr-audit build-rag-corpus` "
            "first or pass `--corpus-path`."
        )

    pack = load_rag_benchmark_pack(resolved_benchmark_dir)
    corpus = load_rag_corpus(resolved_corpus_path)
    working_dir = resolved_benchmark_dir / "working"
    working_dir.mkdir(parents=True, exist_ok=True)

    existing_query_ids = {str(value).strip() for value in pack.queries["query_id"].tolist()}
    existing_query_texts = {
        str(value).strip().lower()
        for value in pack.queries["query_text"].tolist()
        if str(value).strip()
    }
    metadata = _build_corpus_seed_metadata(corpus)
    suggestions = _build_seed_suggestions(
        metadata=metadata,
        existing_query_ids=existing_query_ids,
        existing_query_texts=existing_query_texts,
    )
    suggestion_frame = pd.DataFrame(
        suggestions,
        columns=[
            "suggested_query_id",
            "query_text",
            "query_group",
            "brand_filter",
            "metric_filter",
            "publish_status_scope",
            "expected_source_kinds",
            "ambiguity_flag",
            "requires_citation",
            "suggestion_reason",
            "needs_human_review",
        ],
    )

    artifacts = RagQuerySeedArtifacts(
        suggested_queries_csv_path=working_dir / SUGGESTED_QUERIES_FILENAME,
        suggested_queries_markdown_path=working_dir / SUGGESTED_QUERIES_MARKDOWN_FILENAME,
    )
    suggestion_frame.to_csv(artifacts.suggested_queries_csv_path, index=False)
    artifacts.suggested_queries_markdown_path.write_text(
        _render_suggested_queries_markdown(
            benchmark_dir=resolved_benchmark_dir,
            suggestion_frame=suggestion_frame,
        ),
        encoding="utf-8",
    )
    return RagQuerySeedRun(
        benchmark_dir=resolved_benchmark_dir,
        suggestion_count=int(len(suggestion_frame.index)),
        suggestions=suggestion_frame,
        artifacts=artifacts,
    )


def mine_rag_hard_negatives(
    *,
    benchmark_dir: Path,
    run_dir: Path,
    settings: Settings | None = None,
    corpus_path: Path | None = None,
) -> RagHardNegativeMiningRun:
    """Propose hard-negative review candidates from retrieval benchmark outputs."""

    resolved_settings = settings or Settings()
    resolved_benchmark_dir = benchmark_dir.expanduser().resolve()
    resolved_run_dir = _resolve_run_dir(
        run_dir=run_dir,
        settings=resolved_settings,
    )
    context = _load_run_context(resolved_run_dir)
    resolved_corpus_path = resolve_rag_corpus_path(
        settings=resolved_settings,
        corpus_path=corpus_path,
    )
    corpus = (
        load_rag_corpus(resolved_corpus_path) if resolved_corpus_path.exists() else pd.DataFrame()
    )
    working_dir = resolved_benchmark_dir / "working"
    working_dir.mkdir(parents=True, exist_ok=True)

    query_specs = {row["query_id"]: row for row in context["query_specs"]}
    query_metrics = context["query_metrics"]
    results = context["results"]

    retriever_cache: dict[str, Any] = {}
    suggestion_rows: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str, str, str]] = set()

    for metric_row in query_metrics.to_dict(orient="records"):
        query_id = str(metric_row.get("query_id", "")).strip()
        query_spec = query_specs.get(query_id)
        if query_spec is None:
            continue

        result_rows = results.loc[
            (results["query_id"] == query_id)
            & (results["run_label"] == metric_row.get("run_label"))
        ]
        candidates = result_rows.to_dict(orient="records")
        if metric_row.get("failure_source") == "filtering" and not corpus.empty:
            retriever_name = str(metric_row.get("retriever_name", "")).strip()
            if retriever_name:
                prepared_retriever = retriever_cache.setdefault(
                    retriever_name,
                    prepare_retriever(
                        corpus=corpus,
                        retriever_name=retriever_name,
                        allow_model_download=False,
                    ),
                )
                search_run = rag_search(
                    corpus=corpus,
                    query=query_spec["query"],
                    top_k=DEFAULT_HARD_NEGATIVE_SEARCH_TOP_K,
                    retriever_name=retriever_name,
                    metadata_filters={},
                    allow_model_download=False,
                    prepared_retriever=prepared_retriever,
                )
                if search_run.status == "ok":
                    candidates.extend(search_run.results.to_dict(orient="records"))

        for candidate in candidates:
            if _parse_bool(candidate.get("is_relevant", False)):
                continue
            hard_negative_type = _classify_hard_negative_candidate(
                query_spec=query_spec,
                candidate=candidate,
            )
            if hard_negative_type is None:
                continue
            dedupe_key = (
                query_id,
                str(candidate.get("chunk_id", "")).strip()
                or str(candidate.get("doc_id", "")).strip(),
                str(metric_row.get("run_label", "")).strip(),
                hard_negative_type,
            )
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            suggestion_rows.append(
                {
                    "query_id": query_id,
                    "query_text": query_spec["query"],
                    "run_label": metric_row.get("run_label", ""),
                    "rank": candidate.get("rank"),
                    "doc_id": candidate.get("doc_id", ""),
                    "chunk_id": candidate.get("chunk_id", ""),
                    "source_kind": candidate.get("source_kind", ""),
                    "brand_names": _pipe_join(_coerce_list(candidate.get("brand_names"))),
                    "metric_names": _pipe_join(_coerce_list(candidate.get("metric_names"))),
                    "publish_status": _clean(candidate.get("publish_status")),
                    "hard_negative_type": hard_negative_type,
                    "suggested_relevance_label": "not_relevant",
                    "suggestion_reason": _hard_negative_reason(
                        hard_negative_type=hard_negative_type,
                        query_spec=query_spec,
                        candidate=candidate,
                    ),
                    "needs_human_review": "true",
                    "metadata_filters_json": json.dumps(
                        query_spec.get("metadata_filters") or {},
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    "query_buckets_json": json.dumps(
                        query_spec.get("query_buckets") or [],
                        ensure_ascii=False,
                    ),
                }
            )

    suggestion_frame = pd.DataFrame(
        suggestion_rows,
        columns=[
            "query_id",
            "query_text",
            "run_label",
            "rank",
            "doc_id",
            "chunk_id",
            "source_kind",
            "brand_names",
            "metric_names",
            "publish_status",
            "hard_negative_type",
            "suggested_relevance_label",
            "suggestion_reason",
            "needs_human_review",
            "metadata_filters_json",
            "query_buckets_json",
        ],
    )
    suggestions_csv_path = working_dir / HARD_NEGATIVE_SUGGESTIONS_FILENAME
    suggestion_frame.to_csv(suggestions_csv_path, index=False)

    summary = {
        "built_at_utc": datetime.now(UTC).isoformat(),
        "benchmark_dir": str(resolved_benchmark_dir),
        "run_dir": str(resolved_run_dir),
        "suggestion_count": int(len(suggestion_frame.index)),
        "type_counts": _count_records_by_key(
            suggestion_frame.to_dict(orient="records"),
            key="hard_negative_type",
        ),
    }
    summary_markdown_path = resolved_run_dir / DEFAULT_HARD_NEGATIVE_SUMMARY_MARKDOWN
    summary_markdown_path.write_text(
        _render_hard_negative_summary_markdown(summary, suggestion_frame),
        encoding="utf-8",
    )
    return RagHardNegativeMiningRun(
        benchmark_dir=resolved_benchmark_dir,
        run_dir=resolved_run_dir,
        suggestions=suggestion_frame,
        summary=summary,
        artifacts=RagHardNegativeMiningArtifacts(
            suggestions_csv_path=suggestions_csv_path,
            summary_markdown_path=summary_markdown_path,
        ),
    )


def summarize_rag_failures(
    *,
    benchmark_dir: Path,
    run_dir: Path,
    settings: Settings | None = None,
) -> RagFailureTriageRun:
    """Bucket retrieval benchmark failures into stable analyst-facing triage categories."""

    resolved_settings = settings or Settings()
    resolved_benchmark_dir = benchmark_dir.expanduser().resolve()
    resolved_run_dir = _resolve_run_dir(
        run_dir=run_dir,
        settings=resolved_settings,
    )
    context = _load_run_context(resolved_run_dir)
    query_specs = {row["query_id"]: row for row in context["query_specs"]}
    query_metrics = context["query_metrics"]
    results = context["results"]
    retrieval_rows = {
        (str(row.get("query_id", "")).strip(), str(row.get("retriever_name", "")).strip()): row
        for row in query_metrics.to_dict(orient="records")
        if str(row.get("stage", "")).strip() == "retrieval"
    }

    triage_rows: list[dict[str, Any]] = []
    for metric_row in query_metrics.to_dict(orient="records"):
        query_id = str(metric_row.get("query_id", "")).strip()
        query_spec = query_specs.get(query_id)
        if query_spec is None:
            continue
        triage_bucket = _classify_failure_bucket(
            metric_row=metric_row,
            query_spec=query_spec,
            retrieval_rows=retrieval_rows,
        )
        if triage_bucket is None:
            continue
        group_rows = results.loc[
            (results["query_id"] == query_id)
            & (results["run_label"] == metric_row.get("run_label"))
        ]
        top_rows = group_rows.sort_values(by="rank", kind="mergesort").head(3)
        triage_rows.append(
            {
                "query_id": query_id,
                "query_text": query_spec["query"],
                "run_label": metric_row.get("run_label", ""),
                "retriever_name": metric_row.get("retriever_name", ""),
                "stage": metric_row.get("stage", ""),
                "reranker_name": metric_row.get("reranker_name", ""),
                "triage_bucket": triage_bucket,
                "failure_source": metric_row.get("failure_source", ""),
                "status": metric_row.get("status", ""),
                "status_reason": metric_row.get("status_reason", ""),
                "recall_at_k": metric_row.get("recall_at_k"),
                "mrr": metric_row.get("mrr"),
                "ndcg_at_k": metric_row.get("ndcg_at_k"),
                "citation_precision": metric_row.get("citation_precision"),
                "metadata_filter_correctness": metric_row.get("metadata_filter_correctness"),
                "judged_relevant_count": metric_row.get("judged_relevant_count"),
                "must_appear_violation_count": metric_row.get("must_appear_violation_count"),
                "ambiguity_flag": _parse_bool(metric_row.get("ambiguity_flag")),
                "requires_citation": _parse_bool(query_spec.get("requires_citation")),
                "query_buckets_json": json.dumps(query_spec.get("query_buckets") or []),
                "expected_source_kinds": _pipe_join(query_spec.get("expected_source_kinds") or []),
                "top_source_kinds": _pipe_join(
                    sorted(top_rows["source_kind"].dropna().astype(str).unique().tolist())
                ),
                "top_doc_ids": _pipe_join(
                    sorted(top_rows["doc_id"].dropna().astype(str).unique().tolist())
                ),
                "triage_note": _failure_triage_note(
                    triage_bucket=triage_bucket,
                    metric_row=metric_row,
                    query_spec=query_spec,
                ),
            }
        )

    triage_frame = pd.DataFrame(
        triage_rows,
        columns=[
            "query_id",
            "query_text",
            "run_label",
            "retriever_name",
            "stage",
            "reranker_name",
            "triage_bucket",
            "failure_source",
            "status",
            "status_reason",
            "recall_at_k",
            "mrr",
            "ndcg_at_k",
            "citation_precision",
            "metadata_filter_correctness",
            "judged_relevant_count",
            "must_appear_violation_count",
            "ambiguity_flag",
            "requires_citation",
            "query_buckets_json",
            "expected_source_kinds",
            "top_source_kinds",
            "top_doc_ids",
            "triage_note",
        ],
    )
    summary = {
        "built_at_utc": datetime.now(UTC).isoformat(),
        "benchmark_dir": str(resolved_benchmark_dir),
        "run_dir": str(resolved_run_dir),
        "triaged_query_count": int(len(triage_frame.index)),
        "bucket_counts": _count_records_by_key(
            triage_frame.to_dict(orient="records"),
            key="triage_bucket",
        ),
        "query_bucket_counts": _count_query_buckets(triage_frame),
        "ambiguous_failure_count": int(triage_frame["ambiguity_flag"].map(_parse_bool).sum())
        if not triage_frame.empty
        else 0,
        "citation_sensitive_failure_count": int(
            triage_frame["requires_citation"].map(_parse_bool).sum()
        )
        if not triage_frame.empty
        else 0,
    }
    artifacts = RagFailureTriageArtifacts(
        triage_csv_path=resolved_run_dir / DEFAULT_FAILURE_TRIAGE_FILENAME,
        triage_markdown_path=resolved_run_dir / DEFAULT_FAILURE_TRIAGE_MARKDOWN,
        triage_json_path=resolved_run_dir / DEFAULT_FAILURE_TRIAGE_JSON,
    )
    triage_frame.to_csv(artifacts.triage_csv_path, index=False)
    artifacts.triage_json_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    artifacts.triage_markdown_path.write_text(
        _render_failure_triage_markdown(summary, triage_frame),
        encoding="utf-8",
    )
    return RagFailureTriageRun(
        benchmark_dir=resolved_benchmark_dir,
        run_dir=resolved_run_dir,
        triage_rows=triage_frame,
        summary=summary,
        artifacts=artifacts,
    )


def collect_authoring_run_context(
    *,
    benchmark_dir: Path,
    run_dir: Path,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Collect read-only run-informed coverage context for authoring summaries."""

    triage_run = summarize_rag_failures(
        benchmark_dir=benchmark_dir,
        run_dir=run_dir,
        settings=settings,
    )
    working_suggestions_path = (
        benchmark_dir.expanduser().resolve() / "working" / HARD_NEGATIVE_SUGGESTIONS_FILENAME
    )
    hard_negative_suggestions = (
        pd.read_csv(working_suggestions_path, dtype=str, keep_default_na=False)
        if working_suggestions_path.exists()
        else pd.DataFrame(columns=["hard_negative_type"])
    )
    return {
        "top_failure_buckets": triage_run.summary["bucket_counts"],
        "failure_query_count": triage_run.summary["triaged_query_count"],
        "hard_negative_suggestion_count": int(len(hard_negative_suggestions.index)),
        "hard_negative_type_counts": _count_records_by_key(
            hard_negative_suggestions.to_dict(orient="records"),
            key="hard_negative_type",
        ),
    }


def _build_corpus_seed_metadata(corpus: pd.DataFrame) -> dict[str, Any]:
    brand_metrics: dict[str, set[str]] = {}
    brand_sources: dict[str, set[str]] = {}
    brand_provenance_sources: dict[str, set[str]] = {}
    brand_metric_status: dict[tuple[str, str], set[str]] = {}
    brand_metric_sources: dict[tuple[str, str], set[str]] = {}

    for row in corpus.to_dict(orient="records"):
        brands = _coerce_list(row.get("brand_names"))
        metrics = _coerce_list(row.get("metric_names"))
        source_kind = _clean(row.get("source_kind"))
        publish_status = _clean(row.get("publish_status"))
        for brand in brands:
            brand_metrics.setdefault(brand, set()).update(metrics)
            if source_kind:
                brand_sources.setdefault(brand, set()).add(source_kind)
                if source_kind in {
                    "gold_provenance_registry",
                    "gold_reference_coverage",
                    "manual_reference_notes",
                    "validation_summary_markdown",
                }:
                    brand_provenance_sources.setdefault(brand, set()).add(source_kind)
            for metric in metrics:
                key = (brand, metric)
                if publish_status:
                    brand_metric_status.setdefault(key, set()).add(publish_status)
                if source_kind:
                    brand_metric_sources.setdefault(key, set()).add(source_kind)
    return {
        "brands": sorted(brand_metrics),
        "brand_metrics": {brand: sorted(values) for brand, values in brand_metrics.items()},
        "brand_sources": {brand: sorted(values) for brand, values in brand_sources.items()},
        "brand_provenance_sources": {
            brand: sorted(values) for brand, values in brand_provenance_sources.items()
        },
        "brand_metric_status": {key: sorted(values) for key, values in brand_metric_status.items()},
        "brand_metric_sources": {
            key: sorted(values) for key, values in brand_metric_sources.items()
        },
    }


def _build_seed_suggestions(
    *,
    metadata: dict[str, Any],
    existing_query_ids: set[str],
    existing_query_texts: set[str],
) -> list[dict[str, str]]:
    suggestions: list[dict[str, str]] = []
    seen_ids = set(existing_query_ids)
    seen_texts = set(existing_query_texts)

    def add_suggestion(payload: dict[str, str]) -> None:
        query_text_key = payload["query_text"].strip().lower()
        if query_text_key in seen_texts:
            return
        suggestion_id = _dedupe_suggested_query_id(
            base_id=payload["suggested_query_id"],
            seen_ids=seen_ids,
        )
        payload["suggested_query_id"] = suggestion_id
        seen_ids.add(suggestion_id)
        seen_texts.add(query_text_key)
        suggestions.append(payload)

    for brand in metadata["brands"]:
        metrics = metadata["brand_metrics"].get(brand, [])
        for metric in metrics[:3]:
            source_kinds = metadata["brand_metric_sources"].get((brand, metric), [])
            add_suggestion(
                {
                    "suggested_query_id": _slugify_identifier(
                        f"seed-brand-metric-{brand}-{metric}"
                    ),
                    "query_text": f"What evidence supports {brand} {metric}?",
                    "query_group": "brand_metric_lookup",
                    "brand_filter": brand,
                    "metric_filter": metric,
                    "publish_status_scope": "all",
                    "expected_source_kinds": _pipe_join(source_kinds),
                    "ambiguity_flag": "false",
                    "requires_citation": "true",
                    "suggestion_reason": (
                        "Derived from brand and metric coverage already present in the vetted corpus."
                    ),
                    "needs_human_review": "true",
                }
            )
        for metric in metrics[:3]:
            for publish_status in metadata["brand_metric_status"].get((brand, metric), []):
                add_suggestion(
                    {
                        "suggested_query_id": _slugify_identifier(
                            f"seed-publish-status-{brand}-{metric}-{publish_status}"
                        ),
                        "query_text": f"Why is {brand} {metric} {publish_status} for external use?",
                        "query_group": "publish_status_audit",
                        "brand_filter": brand,
                        "metric_filter": metric,
                        "publish_status_scope": publish_status,
                        "expected_source_kinds": _pipe_join(
                            metadata["brand_metric_sources"].get((brand, metric), [])
                        ),
                        "ambiguity_flag": "false",
                        "requires_citation": "true",
                        "suggestion_reason": (
                            "Seeded from observed publish-status decisions in the Gold retrieval corpus."
                        ),
                        "needs_human_review": "true",
                    }
                )
        provenance_sources = metadata["brand_provenance_sources"].get(brand, [])
        if provenance_sources:
            add_suggestion(
                {
                    "suggested_query_id": _slugify_identifier(f"seed-provenance-{brand}"),
                    "query_text": f"Show provenance for {brand}.",
                    "query_group": "provenance_citation",
                    "brand_filter": brand,
                    "metric_filter": "",
                    "publish_status_scope": "all",
                    "expected_source_kinds": _pipe_join(provenance_sources),
                    "ambiguity_flag": "false",
                    "requires_citation": "true",
                    "suggestion_reason": (
                        "Brand-level provenance sources are available and should be benchmarked directly."
                    ),
                    "needs_human_review": "true",
                }
            )
        source_kinds = metadata["brand_sources"].get(brand, [])
        if source_kinds:
            add_suggestion(
                {
                    "suggested_query_id": _slugify_identifier(
                        f"seed-source-kind-{brand}-{source_kinds[0]}"
                    ),
                    "query_text": f"Find {source_kinds[0].replace('_', ' ')} evidence for {brand}.",
                    "query_group": "source_kind_lookup",
                    "brand_filter": brand,
                    "metric_filter": "",
                    "publish_status_scope": "all",
                    "expected_source_kinds": source_kinds[0],
                    "ambiguity_flag": "false",
                    "requires_citation": "true",
                    "suggestion_reason": (
                        "Source-kind-specific lookup seeded from the current vetted corpus metadata."
                    ),
                    "needs_human_review": "true",
                }
            )

    pair_count = 0
    for left_brand, right_brand in combinations(metadata["brands"], 2):
        common_metrics = sorted(
            set(metadata["brand_metrics"].get(left_brand, []))
            & set(metadata["brand_metrics"].get(right_brand, []))
        )
        if not common_metrics:
            continue
        metric = common_metrics[0]
        add_suggestion(
            {
                "suggested_query_id": _slugify_identifier(
                    f"seed-compare-{left_brand}-{right_brand}-{metric}"
                ),
                "query_text": f"Compare {left_brand} and {right_brand} on {metric} export readiness.",
                "query_group": "cross_brand_comparison",
                "brand_filter": f"{left_brand}|{right_brand}",
                "metric_filter": metric,
                "publish_status_scope": "all",
                "expected_source_kinds": "gold_publish_decision",
                "ambiguity_flag": "true",
                "requires_citation": "true",
                "suggestion_reason": (
                    "Cross-brand comparison seeded from shared metric coverage in the vetted corpus."
                ),
                "needs_human_review": "true",
            }
        )
        pair_count += 1
        if pair_count >= DEFAULT_QUERY_SEED_LIMIT_PER_GROUP:
            break

    return suggestions


def _load_run_context(run_dir: Path) -> dict[str, Any]:
    benchmark_queries_path = run_dir / "benchmark_queries.json"
    per_query_metrics_path = run_dir / "per_query_metrics.json"
    results_path = run_dir / "per_query_results.parquet"
    metrics_path = run_dir / "metrics.csv"
    missing_paths = [
        path
        for path in (benchmark_queries_path, per_query_metrics_path, results_path, metrics_path)
        if not path.exists()
    ]
    if missing_paths:
        missing = ", ".join(str(path.name) for path in missing_paths)
        raise FileNotFoundError(
            "Benchmark run directory is missing required artifacts for authoring assist tooling: "
            f"{missing}. Re-run `qsr-audit eval-rag-retrieval` with the current code first."
        )
    query_specs = json.loads(benchmark_queries_path.read_text(encoding="utf-8"))
    if not isinstance(query_specs, list):
        raise ValueError(f"`{benchmark_queries_path}` must contain a list of query specs.")
    query_metrics = pd.DataFrame(json.loads(per_query_metrics_path.read_text(encoding="utf-8")))
    results = pd.read_parquet(results_path)
    metrics = pd.read_csv(metrics_path, dtype=str, keep_default_na=False)
    return {
        "query_specs": query_specs,
        "query_metrics": query_metrics,
        "results": results,
        "metrics": metrics,
    }


def _classify_hard_negative_candidate(
    *,
    query_spec: dict[str, Any],
    candidate: dict[str, Any],
) -> str | None:
    query_brands = set(query_spec.get("brand_filter_values") or [])
    row_brands = set(_coerce_list(candidate.get("brand_names")))
    if query_brands and row_brands and not query_brands.intersection(row_brands):
        return "wrong_brand_near_miss"

    query_metrics = set(query_spec.get("metric_filter_values") or [])
    row_metrics = set(_coerce_list(candidate.get("metric_names")))
    if query_metrics and row_metrics and not query_metrics.intersection(row_metrics):
        return "wrong_metric_near_miss"

    expected_statuses = _expected_publish_statuses(query_spec.get("publish_status_scope", "all"))
    candidate_status = _clean(candidate.get("publish_status"))
    if expected_statuses and candidate_status and candidate_status not in expected_statuses:
        return "publish_status_confusion"

    expected_source_kinds = set(query_spec.get("expected_source_kinds") or [])
    candidate_source_kind = _clean(candidate.get("source_kind"))
    if (
        expected_source_kinds
        and candidate_source_kind
        and candidate_source_kind not in expected_source_kinds
    ):
        return "provenance_source_confusion"

    metadata_filters = query_spec.get("metadata_filters") or {}
    if metadata_filters and not row_matches_filters(candidate, metadata_filters):
        return "metadata_filter_near_miss"
    return None


def _hard_negative_reason(
    *,
    hard_negative_type: str,
    query_spec: dict[str, Any],
    candidate: dict[str, Any],
) -> str:
    if hard_negative_type == "wrong_brand_near_miss":
        return (
            "Retrieved chunk matches the general lookup pattern but points to the wrong brand, "
            "so it is a good hard-negative review candidate."
        )
    if hard_negative_type == "wrong_metric_near_miss":
        return "Retrieved chunk is close to the query intent but covers the wrong metric."
    if hard_negative_type == "publish_status_confusion":
        return (
            "Retrieved chunk conflicts with the requested publish-status scope and should be "
            "reviewed as a hard negative."
        )
    if hard_negative_type == "provenance_source_confusion":
        return (
            "Retrieved chunk comes from the wrong source kind for this query and is useful for "
            "source-sensitive negative labeling."
        )
    return (
        "Retrieved chunk is a metadata-filter near miss that should be reviewed before any final "
        "judgment is added."
    )


def _classify_failure_bucket(
    *,
    metric_row: dict[str, Any],
    query_spec: dict[str, Any],
    retrieval_rows: dict[tuple[str, str], dict[str, Any]],
) -> str | None:
    judged_relevant_count = int(metric_row.get("judged_relevant_count") or 0)
    failure_source = _clean(metric_row.get("failure_source"))
    status = _clean(metric_row.get("status"))
    stage = _clean(metric_row.get("stage"))
    if judged_relevant_count == 0 or failure_source == "benchmark_labeling":
        return "benchmark-label issue"

    citation_precision = _as_float(metric_row.get("citation_precision"))
    metadata_filter_correctness = _as_float(metric_row.get("metadata_filter_correctness"))
    requires_citation = _parse_bool(query_spec.get("requires_citation"))
    ambiguity_flag = _parse_bool(query_spec.get("ambiguity_flag"))

    if stage == "reranked" and status != "ok":
        retrieval_row = retrieval_rows.get(
            (
                str(metric_row.get("query_id", "")).strip(),
                str(metric_row.get("retriever_name", "")).strip(),
            )
        )
        if retrieval_row is not None and _clean(retrieval_row.get("status")) == "ok":
            return "rerank miss"

    if failure_source == "filtering" or (
        metadata_filter_correctness is not None and metadata_filter_correctness < 1.0
    ):
        return "metadata filter miss"

    if requires_citation and (citation_precision is None or citation_precision < 1.0):
        return "citation/provenance miss"

    if ambiguity_flag and status != "ok":
        return "ambiguity/query-design issue"

    if status != "ok" or failure_source in {"retrieval", "ranking"}:
        return "retrieval miss"
    return None


def _failure_triage_note(
    *,
    triage_bucket: str,
    metric_row: dict[str, Any],
    query_spec: dict[str, Any],
) -> str:
    if triage_bucket == "benchmark-label issue":
        return "No judged relevant targets matched this query, so the benchmark contract should be reviewed before debugging retrieval."
    if triage_bucket == "rerank miss":
        return "The first-pass retriever was acceptable, but reranking degraded the result order for this query."
    if triage_bucket == "metadata filter miss":
        return "Metadata filtering appears to exclude or distort otherwise relevant evidence for this query."
    if triage_bucket == "citation/provenance miss":
        return "The query requires citation-bearing evidence, but the retrieved set is missing enough provenance context."
    if triage_bucket == "ambiguity/query-design issue":
        return "The query is still ambiguous under the current benchmark contract and may need to be split or clarified."
    return (
        metric_row.get("status_reason")
        or "Relevant evidence is not surfacing reliably within the current retrieval depth."
    )


def _render_suggested_queries_markdown(
    *,
    benchmark_dir: Path,
    suggestion_frame: pd.DataFrame,
) -> str:
    lines = [
        "# Suggested RAG Queries",
        "",
        f"- Benchmark dir: `{benchmark_dir}`",
        f"- Suggestions: `{len(suggestion_frame.index)}`",
        "- Suggestions are deterministic review candidates only. They are not ground truth and they never overwrite `queries.csv`.",
        "",
    ]
    if suggestion_frame.empty:
        lines.extend(["## Suggestions", "", "- None.", ""])
        return "\n".join(lines)

    group_counts = (
        suggestion_frame.groupby("query_group")["suggested_query_id"]
        .count()
        .sort_values(ascending=False)
    )
    lines.extend(["## Group Counts", ""])
    for query_group, count in group_counts.items():
        lines.append(f"- `{query_group}`: {count}")
    lines.append("")

    for query_group in group_counts.index.tolist():
        lines.extend([f"## {query_group}", ""])
        group_rows = suggestion_frame.loc[suggestion_frame["query_group"] == query_group]
        for row in group_rows.to_dict(orient="records"):
            lines.append(
                f"- `{row['suggested_query_id']}`: {row['query_text']} ({row['suggestion_reason']})"
            )
        lines.append("")
    return "\n".join(lines)


def _render_hard_negative_summary_markdown(
    summary: dict[str, Any],
    suggestion_frame: pd.DataFrame,
) -> str:
    lines = [
        "# Hard Negative Suggestions",
        "",
        f"- Benchmark dir: `{summary['benchmark_dir']}`",
        f"- Run dir: `{summary['run_dir']}`",
        f"- Suggestions: `{summary['suggestion_count']}`",
        "- These are reviewer candidates only. They are not final judgments.",
        "",
        "## Counts By Type",
        "",
    ]
    if not summary["type_counts"]:
        lines.append("- None.")
    else:
        for row in summary["type_counts"]:
            lines.append(f"- `{row['value']}`: {row['count']}")

    lines.extend(["", "## Suggested Rows", ""])
    if suggestion_frame.empty:
        lines.append("- None.")
    else:
        for row in suggestion_frame.head(20).to_dict(orient="records"):
            lines.append(
                f"- `{row['query_id']}` / `{row['hard_negative_type']}` -> `{row['chunk_id'] or row['doc_id']}`: "
                f"{row['suggestion_reason']}"
            )
    lines.append("")
    return "\n".join(lines)


def _render_failure_triage_markdown(
    summary: dict[str, Any],
    triage_frame: pd.DataFrame,
) -> str:
    lines = [
        "# RAG Failure Triage",
        "",
        f"- Benchmark dir: `{summary['benchmark_dir']}`",
        f"- Run dir: `{summary['run_dir']}`",
        f"- Triaged query rows: `{summary['triaged_query_count']}`",
        f"- Citation-sensitive failures: `{summary['citation_sensitive_failure_count']}`",
        f"- Ambiguous failures: `{summary['ambiguous_failure_count']}`",
        "",
        "## Bucket Counts",
        "",
    ]
    if not summary["bucket_counts"]:
        lines.append("- None.")
    else:
        for row in summary["bucket_counts"]:
            lines.append(f"- `{row['value']}`: {row['count']}")

    lines.extend(["", "## Query Bucket Coverage", ""])
    if not summary["query_bucket_counts"]:
        lines.append("- None.")
    else:
        for row in summary["query_bucket_counts"]:
            lines.append(f"- `{row['value']}`: {row['count']}")

    lines.extend(["", "## Query Details", ""])
    if triage_frame.empty:
        lines.append("- None.")
    else:
        for row in triage_frame.to_dict(orient="records"):
            lines.append(
                f"- `{row['query_id']}` / `{row['run_label']}` -> `{row['triage_bucket']}`: "
                f"{row['triage_note']}"
            )
    lines.append("")
    return "\n".join(lines)


def _count_query_buckets(triage_frame: pd.DataFrame) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for value in triage_frame.get("query_buckets_json", pd.Series(dtype=str)).tolist():
        for bucket in _coerce_list(value):
            counts[bucket] = counts.get(bucket, 0) + 1
    return [
        {"value": bucket, "count": count}
        for bucket, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _count_records_by_key(rows: list[dict[str, Any]], *, key: str) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in rows:
        value = _clean(row.get(key)) or "unknown"
        counts[value] = counts.get(value, 0) + 1
    return [
        {"value": value, "count": count}
        for value, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _dedupe_suggested_query_id(*, base_id: str, seen_ids: set[str]) -> str:
    if base_id not in seen_ids:
        return base_id
    suffix = 2
    while f"{base_id}-{suffix}" in seen_ids:
        suffix += 1
    return f"{base_id}-{suffix}"


def _expected_publish_statuses(scope: str) -> set[str]:
    normalized = _clean(scope).lower()
    if not normalized or normalized == "all":
        return set()
    if normalized == "non_blocked":
        return {"publishable", "advisory"}
    return {normalized}


def _resolve_run_dir(*, run_dir: Path, settings: Settings) -> Path:
    resolved_run_dir = run_dir.expanduser().resolve()
    if not resolved_run_dir.exists() or not resolved_run_dir.is_dir():
        raise FileNotFoundError(
            f"Benchmark run directory `{resolved_run_dir}` was not found or is not a directory."
        )
    _ensure_non_analyst_artifact_root(resolved_run_dir, settings=settings)
    return resolved_run_dir


def _ensure_non_analyst_artifact_root(path: Path, *, settings: Settings) -> None:
    for forbidden_root in (
        settings.reports_dir.expanduser().resolve(),
        settings.strategy_dir.expanduser().resolve(),
    ):
        try:
            path.relative_to(forbidden_root)
        except ValueError:
            continue
        raise ValueError(
            "RAG authoring-assist artifacts must not be written under analyst-facing paths like "
            f"{forbidden_root}."
        )


def _coerce_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    if "|" in text:
        return [item.strip() for item in text.split("|") if item.strip()]
    return [text]


def _pipe_join(values: list[str]) -> str:
    return "|".join(value for value in values if value)


def _slugify_identifier(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip().lower()).strip("-")
    return normalized or "suggested-query"


def _clean(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _parse_bool(value: Any) -> bool:
    return _clean(value).lower() in {"1", "true", "yes", "y"}


def _as_float(value: Any) -> float | None:
    text = _clean(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


__all__ = [
    "RagFailureTriageArtifacts",
    "RagFailureTriageRun",
    "RagHardNegativeMiningArtifacts",
    "RagHardNegativeMiningRun",
    "RagQuerySeedArtifacts",
    "RagQuerySeedRun",
    "collect_authoring_run_context",
    "mine_rag_hard_negatives",
    "seed_rag_queries",
    "summarize_rag_failures",
]
