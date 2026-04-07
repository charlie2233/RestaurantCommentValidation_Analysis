"""Human-in-the-loop authoring helpers for retrieval benchmark packs."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.config import Settings
from qsr_audit.rag.authoring_assist import collect_authoring_run_context
from qsr_audit.rag.benchmark_pack import (
    ADJUDICATED_JUDGMENTS_FILENAME,
    ALLOWED_RELEVANCE_LABELS,
    JUDGMENT_COLUMNS,
    JUDGMENTS_FILENAME,
    QUERY_GROUP_COLUMNS,
    RagBenchmarkValidationRun,
    build_default_rag_benchmark_metadata,
    load_rag_benchmark_metadata,
    load_rag_benchmark_pack,
    validate_rag_benchmark_pack,
    write_rag_benchmark_metadata,
)
from qsr_audit.rag.corpus import load_rag_corpus, resolve_rag_corpus_path
from qsr_audit.rag.retrieval import prepare_retriever, rag_search

DEFAULT_RAG_BENCHMARKS_ROOT = Path("data/rag_benchmarks")
DEFAULT_ADJUDICATION_SUBDIR = Path("rag/benchmarks/adjudication")
DEFAULT_AUTHORING_SUBDIR = Path("rag/benchmarks/authoring")
REVIEWERS_DIRNAME = "reviewers"
WORKING_DIRNAME = "working"
UNDER_COVERED_QUERY_GROUP_THRESHOLD = 2
MIN_ADJUDICATION_REVIEWER_COUNT = 2


@dataclass(frozen=True)
class RagBenchmarkInitArtifacts:
    metadata_path: Path
    readme_path: Path
    checklist_path: Path
    queries_path: Path
    judgments_path: Path
    filters_path: Path
    query_groups_path: Path


@dataclass(frozen=True)
class RagBenchmarkInitRun:
    benchmark_dir: Path
    metadata: dict[str, Any]
    artifacts: RagBenchmarkInitArtifacts


@dataclass(frozen=True)
class RagJudgmentBootstrapArtifacts:
    query_specs_json_path: Path
    candidate_results_parquet_path: Path
    candidate_results_csv_path: Path
    judgment_workspace_csv_path: Path
    bootstrap_manifest_path: Path


@dataclass(frozen=True)
class RagJudgmentBootstrapRun:
    benchmark_dir: Path
    query_count: int
    candidate_count: int
    artifacts: RagJudgmentBootstrapArtifacts
    validation: RagBenchmarkValidationRun


@dataclass(frozen=True)
class RagBenchmarkAdjudicationArtifacts:
    conflicts_csv_path: Path
    agreement_summary_json_path: Path
    agreement_summary_markdown_path: Path
    adjudicated_judgments_path: Path | None


@dataclass(frozen=True)
class RagBenchmarkAdjudicationRun:
    benchmark_dir: Path
    reviewer_names: list[str]
    conflict_count: int
    force_used: bool
    metadata: dict[str, Any]
    agreement_summary: dict[str, Any]
    artifacts: RagBenchmarkAdjudicationArtifacts


@dataclass(frozen=True)
class RagBenchmarkAuthoringSummaryArtifacts:
    summary_json_path: Path
    summary_markdown_path: Path
    coverage_rows_csv_path: Path


@dataclass(frozen=True)
class RagBenchmarkAuthoringSummaryRun:
    benchmark_dir: Path
    summary: dict[str, Any]
    artifacts: RagBenchmarkAuthoringSummaryArtifacts


def init_rag_benchmark(
    *,
    name: str,
    settings: Settings | None = None,
    root_dir: Path = DEFAULT_RAG_BENCHMARKS_ROOT,
    authors: tuple[str, ...] = (),
    notes: str = "",
) -> RagBenchmarkInitRun:
    """Initialize a local analyst benchmark pack from the committed templates."""

    safe_name = _slugify_pack_name(name)
    resolved_root = root_dir.expanduser().resolve()
    benchmark_dir = resolved_root / safe_name
    if benchmark_dir.exists():
        raise ValueError(f"Benchmark pack directory `{benchmark_dir}` already exists.")

    benchmark_dir.mkdir(parents=True, exist_ok=False)
    template_dir = _benchmark_templates_dir()
    artifacts = RagBenchmarkInitArtifacts(
        metadata_path=benchmark_dir / "metadata.json",
        readme_path=benchmark_dir / "README.md",
        checklist_path=benchmark_dir / "checklist.md",
        queries_path=benchmark_dir / "queries.csv",
        judgments_path=benchmark_dir / "judgments.csv",
        filters_path=benchmark_dir / "filters.csv",
        query_groups_path=benchmark_dir / "query_groups.csv",
    )
    for artifact_path in (
        artifacts.queries_path,
        artifacts.judgments_path,
        artifacts.filters_path,
        artifacts.query_groups_path,
    ):
        source_path = template_dir / artifact_path.name
        artifact_path.write_text(source_path.read_text(encoding="utf-8"), encoding="utf-8")

    resolved_settings = settings or Settings()
    metadata = build_default_rag_benchmark_metadata(
        corpus_manifest_path=str(
            resolved_settings.artifacts_dir / "rag" / "corpus" / "manifest.json"
        ),
        authors=list(authors),
        notes=notes,
    )
    write_rag_benchmark_metadata(benchmark_dir, metadata)
    artifacts.readme_path.write_text(_render_benchmark_pack_readme(safe_name), encoding="utf-8")
    artifacts.checklist_path.write_text(
        _render_benchmark_pack_checklist(safe_name),
        encoding="utf-8",
    )
    (benchmark_dir / REVIEWERS_DIRNAME).mkdir(parents=True, exist_ok=True)
    (benchmark_dir / WORKING_DIRNAME).mkdir(parents=True, exist_ok=True)

    return RagBenchmarkInitRun(
        benchmark_dir=benchmark_dir,
        metadata=metadata,
        artifacts=artifacts,
    )


def bootstrap_rag_judgments(
    *,
    benchmark_dir: Path,
    settings: Settings | None = None,
    corpus_path: Path | None = None,
    retriever_name: str = "bm25",
    top_k: int = 10,
    allow_model_download: bool = False,
) -> RagJudgmentBootstrapRun:
    """Generate candidate retrieval results and a manual judgment workspace."""

    resolved_settings = settings or Settings()
    resolved_corpus_path = resolve_rag_corpus_path(
        settings=resolved_settings,
        corpus_path=corpus_path,
    )
    if not resolved_corpus_path.exists():
        raise FileNotFoundError(
            "Judgment bootstrap requires an existing corpus parquet. Run "
            "`qsr-audit build-rag-corpus` first or pass `--corpus-path`."
        )
    corpus = load_rag_corpus(resolved_corpus_path)
    validation_output_root = (
        resolved_settings.artifacts_dir
        / "rag"
        / "benchmarks"
        / "validation"
        / _pack_name_from_dir(benchmark_dir)
        / "bootstrap"
    )
    validation = validate_rag_benchmark_pack(
        benchmark_dir=benchmark_dir,
        corpus=corpus,
        settings=resolved_settings,
        output_root=validation_output_root,
        require_judgments=False,
    )
    if not validation.passed:
        raise ValueError(
            "Benchmark authoring inputs failed validation. Resolve query/filter issues before "
            f"bootstrapping judgments. See {validation.artifacts.validation_markdown_path}."
        )

    prepared_retriever = prepare_retriever(
        corpus=corpus,
        retriever_name=retriever_name,
        allow_model_download=allow_model_download,
    )
    candidate_frames: list[pd.DataFrame] = []
    for query_spec in validation.query_specs:
        search_run = rag_search(
            corpus=corpus,
            query=query_spec["query"],
            top_k=top_k,
            retriever_name=retriever_name,
            metadata_filters=query_spec.get("metadata_filters") or {},
            allow_model_download=allow_model_download,
            prepared_retriever=prepared_retriever,
        )
        if search_run.status != "ok" or search_run.results.empty:
            candidate_frames.append(
                _empty_candidate_results_frame(
                    query_id=query_spec["query_id"],
                    query_text=query_spec["query"],
                    retriever_name=retriever_name,
                    status=search_run.status,
                    reason=search_run.reason,
                )
            )
            continue
        candidate = search_run.results.copy()
        candidate["query_id"] = query_spec["query_id"]
        candidate["query_text"] = query_spec["query"]
        candidate["metadata_filters_json"] = json.dumps(
            query_spec.get("metadata_filters") or {},
            ensure_ascii=False,
            sort_keys=True,
        )
        candidate["query_buckets_json"] = json.dumps(
            query_spec.get("query_buckets") or [],
            ensure_ascii=False,
        )
        candidate["ambiguity_flag"] = bool(query_spec.get("ambiguity_flag"))
        candidate["requires_citation"] = bool(query_spec.get("requires_citation"))
        candidate["suggestion_status"] = "candidate_suggestion"
        candidate["suggestion_note"] = (
            "First-pass retrieval suggestion only. Analyst review is required before this "
            "becomes a benchmark judgment."
        )
        candidate_frames.append(candidate)

    candidate_results = (
        pd.concat(candidate_frames, ignore_index=True) if candidate_frames else pd.DataFrame()
    )
    judgment_workspace = _build_judgment_workspace(candidate_results)
    working_dir = benchmark_dir.expanduser().resolve() / WORKING_DIRNAME
    working_dir.mkdir(parents=True, exist_ok=True)
    artifacts = RagJudgmentBootstrapArtifacts(
        query_specs_json_path=working_dir / "query_specs.json",
        candidate_results_parquet_path=working_dir / "candidate_results.parquet",
        candidate_results_csv_path=working_dir / "candidate_results.csv",
        judgment_workspace_csv_path=working_dir / "judgment_workspace.csv",
        bootstrap_manifest_path=working_dir / "bootstrap_manifest.json",
    )
    artifacts.query_specs_json_path.write_text(
        json.dumps(validation.query_specs, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    candidate_results.to_parquet(artifacts.candidate_results_parquet_path, index=False)
    candidate_results.to_csv(artifacts.candidate_results_csv_path, index=False)
    judgment_workspace.to_csv(artifacts.judgment_workspace_csv_path, index=False)
    artifacts.bootstrap_manifest_path.write_text(
        json.dumps(
            {
                "built_at_utc": datetime.now(UTC).isoformat(),
                "benchmark_dir": str(benchmark_dir.expanduser().resolve()),
                "corpus_path": str(resolved_corpus_path),
                "retriever_name": retriever_name,
                "top_k": top_k,
                "query_count": len(validation.query_specs),
                "candidate_count": len(candidate_results.index),
                "suggestion_label": "candidate_suggestion",
                "warning": "Bootstrap outputs are reviewer suggestions, not ground truth judgments.",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return RagJudgmentBootstrapRun(
        benchmark_dir=benchmark_dir.expanduser().resolve(),
        query_count=len(validation.query_specs),
        candidate_count=len(candidate_results.index),
        artifacts=artifacts,
        validation=validation,
    )


def validate_rag_reviewer_file(
    *,
    benchmark_dir: Path,
    reviewer: str,
    settings: Settings | None = None,
    corpus_path: Path | None = None,
) -> RagBenchmarkValidationRun:
    """Validate a reviewer-specific judgments file against the benchmark contract."""

    resolved_settings = settings or Settings()
    resolved_corpus_path = resolve_rag_corpus_path(
        settings=resolved_settings,
        corpus_path=corpus_path,
    )
    if not resolved_corpus_path.exists():
        raise FileNotFoundError(
            "Reviewer validation requires an existing corpus parquet. Run "
            "`qsr-audit build-rag-corpus` first or pass `--corpus-path`."
        )
    reviewer_path = _reviewer_judgments_path(benchmark_dir, reviewer)
    validation_output_root = (
        resolved_settings.artifacts_dir
        / "rag"
        / "benchmarks"
        / "validation"
        / _pack_name_from_dir(benchmark_dir)
        / f"reviewer-{reviewer}"
    )
    return validate_rag_benchmark_pack(
        benchmark_dir=benchmark_dir,
        corpus=load_rag_corpus(resolved_corpus_path),
        settings=resolved_settings,
        output_root=validation_output_root,
        judgments_path=reviewer_path,
        require_judgments=True,
    )


def adjudicate_rag_benchmark(
    *,
    benchmark_dir: Path,
    settings: Settings | None = None,
    corpus_path: Path | None = None,
    force: bool = False,
) -> RagBenchmarkAdjudicationRun:
    """Compare reviewer judgments, write conflict reports, and optionally emit adjudicated judgments."""

    resolved_settings = settings or Settings()
    reviewer_paths = _discover_reviewer_judgments_paths(benchmark_dir)
    if not reviewer_paths:
        raise ValueError(
            f"No reviewer judgments were found under `{benchmark_dir}`/{REVIEWERS_DIRNAME}/."
        )

    resolved_corpus_path = resolve_rag_corpus_path(
        settings=resolved_settings,
        corpus_path=corpus_path,
    )
    if not resolved_corpus_path.exists():
        raise FileNotFoundError(
            "Adjudication requires an existing corpus parquet. Run `qsr-audit build-rag-corpus` "
            "first or pass `--corpus-path`."
        )
    corpus = load_rag_corpus(resolved_corpus_path)
    chunk_to_doc_id = corpus.set_index("chunk_id")["doc_id"].to_dict() if not corpus.empty else {}

    reviewer_frames: list[pd.DataFrame] = []
    reviewer_validations: dict[str, RagBenchmarkValidationRun] = {}
    reviewer_names = sorted(reviewer_paths)
    for reviewer_name in reviewer_names:
        validation = validate_rag_reviewer_file(
            benchmark_dir=benchmark_dir,
            reviewer=reviewer_name,
            settings=resolved_settings,
            corpus_path=resolved_corpus_path,
        )
        reviewer_validations[reviewer_name] = validation
        if not validation.passed:
            raise ValueError(
                "Reviewer benchmark validation failed. Resolve reviewer errors before "
                f"adjudication. See {validation.artifacts.validation_markdown_path}."
            )
        reviewer_frame = validation.pack.judgments.copy()
        reviewer_frame["reviewer"] = reviewer_name
        reviewer_frames.append(reviewer_frame)

    all_rows = pd.concat(reviewer_frames, ignore_index=True)
    normalized_rows = _normalize_reviewer_rows(
        normalized_rows=all_rows,
        chunk_to_doc_id=chunk_to_doc_id,
    )
    conflicts, adjudicated_rows = _compare_reviewer_rows(
        normalized_rows=normalized_rows,
        reviewer_names=reviewer_names,
        force=force,
    )

    benchmark_dir_resolved = benchmark_dir.expanduser().resolve()
    run_id = f"{_pack_name_from_dir(benchmark_dir)}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    output_root = (
        (resolved_settings.artifacts_dir / DEFAULT_ADJUDICATION_SUBDIR / run_id)
        .expanduser()
        .resolve()
    )
    _ensure_non_analyst_artifact_root(output_root=output_root, settings=resolved_settings)
    output_root.mkdir(parents=True, exist_ok=True)

    conflicts_df = pd.DataFrame(conflicts)
    conflicts_csv_path = output_root / "conflicts.csv"
    conflicts_df.to_csv(conflicts_csv_path, index=False)

    unresolved_conflict_count = len(conflicts)
    minimum_reviewer_coverage_met = len(reviewer_names) >= MIN_ADJUDICATION_REVIEWER_COUNT
    true_adjudication_eligible = minimum_reviewer_coverage_met and unresolved_conflict_count == 0
    forced_provisional = force and not true_adjudication_eligible
    pack_status = "adjudicated" if true_adjudication_eligible else "in_review"
    metadata = load_rag_benchmark_pack(benchmark_dir_resolved).metadata
    metadata["pack_status"] = pack_status
    if not minimum_reviewer_coverage_met:
        metadata["notes"] = _append_metadata_note(
            metadata.get("notes", ""),
            (
                "Adjudication remains provisional until at least "
                f"{MIN_ADJUDICATION_REVIEWER_COUNT} reviewer submissions are available."
            ),
        )
    if forced_provisional:
        metadata["notes"] = _append_metadata_note(
            metadata.get("notes", ""),
            (
                "Forced provisional adjudication artifact generated without satisfying the "
                "normal adjudication requirements."
            ),
        )
    agreement_summary = {
        "built_at_utc": datetime.now(UTC).isoformat(),
        "benchmark_dir": str(benchmark_dir_resolved),
        "reviewer_count": len(reviewer_names),
        "reviewers": reviewer_names,
        "reviewer_validation_paths": {
            reviewer_name: str(
                reviewer_validations[reviewer_name].artifacts.validation_markdown_path
            )
            for reviewer_name in reviewer_names
        },
        "reference_target_count": int(
            normalized_rows.groupby(["query_id", "reference_type", "reference_value"]).ngroups
        ),
        "adjudicated_row_count": int(len(adjudicated_rows.index)),
        "conflict_count": unresolved_conflict_count,
        "minimum_reviewer_count_required": MIN_ADJUDICATION_REVIEWER_COUNT,
        "minimum_reviewer_coverage_met": minimum_reviewer_coverage_met,
        "true_adjudication_eligible": true_adjudication_eligible,
        "forced_provisional": forced_provisional,
        "force_used": force,
        "pack_status": pack_status,
        "top_conflict_types": _top_counts(conflicts, key="conflict_type"),
    }
    agreement_summary_json_path = output_root / "agreement_summary.json"
    agreement_summary_json_path.write_text(
        json.dumps(agreement_summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    agreement_summary_markdown_path = output_root / "agreement_summary.md"
    agreement_summary_markdown_path.write_text(
        _render_adjudication_summary_markdown(agreement_summary, conflicts_df),
        encoding="utf-8",
    )

    adjudicated_judgments_path: Path | None = None
    if unresolved_conflict_count and not force:
        write_rag_benchmark_metadata(benchmark_dir_resolved, metadata)
        raise ValueError(
            "Reviewer conflicts remain unresolved. Review the adjudication report before marking "
            f"the pack as final: {agreement_summary_markdown_path}."
        )

    if true_adjudication_eligible or force:
        adjudicated_judgments_path = benchmark_dir_resolved / ADJUDICATED_JUDGMENTS_FILENAME
        adjudicated_rows[JUDGMENT_COLUMNS].to_csv(adjudicated_judgments_path, index=False)
    write_rag_benchmark_metadata(benchmark_dir_resolved, metadata)
    return RagBenchmarkAdjudicationRun(
        benchmark_dir=benchmark_dir_resolved,
        reviewer_names=reviewer_names,
        conflict_count=unresolved_conflict_count,
        force_used=force,
        metadata=metadata,
        agreement_summary=agreement_summary,
        artifacts=RagBenchmarkAdjudicationArtifacts(
            conflicts_csv_path=conflicts_csv_path,
            agreement_summary_json_path=agreement_summary_json_path,
            agreement_summary_markdown_path=agreement_summary_markdown_path,
            adjudicated_judgments_path=adjudicated_judgments_path,
        ),
    )


def summarize_rag_benchmark_authoring(
    *,
    benchmark_dir: Path,
    settings: Settings | None = None,
    run_dir: Path | None = None,
) -> RagBenchmarkAuthoringSummaryRun:
    """Summarize benchmark authoring coverage and gaps across the current pack."""

    resolved_settings = settings or Settings()
    benchmark_dir_resolved = benchmark_dir.expanduser().resolve()
    pack = load_rag_benchmark_pack(benchmark_dir_resolved)
    judgments, judgments_source = _load_authoring_summary_judgments(benchmark_dir_resolved)
    query_groups = pack.query_groups.copy()
    if query_groups.empty:
        query_groups = pd.DataFrame(columns=QUERY_GROUP_COLUMNS)

    query_group_map: dict[str, list[str]] = {}
    for row in query_groups.to_dict(orient="records"):
        query_group_map.setdefault(row["query_id"], []).append(row["query_group"])
    judgments_by_query = judgments.groupby("query_id") if not judgments.empty else None
    judged_query_ids = set(judgments["query_id"].tolist()) if not judgments.empty else set()

    query_records: list[dict[str, Any]] = []
    for query in pack.queries.to_dict(orient="records"):
        query_id = query["query_id"]
        query_judgments = (
            judgments_by_query.get_group(query_id)
            if judgments_by_query is not None and query_id in judged_query_ids
            else pd.DataFrame(columns=JUDGMENT_COLUMNS)
        )
        query_records.append(
            {
                "query_id": query_id,
                "query_text": query["query_text"],
                "query_groups": sorted(set(query_group_map.get(query_id, []))) or ["(none)"],
                "brand_filter_values": _split_multivalue_or_default(query.get("brand_filter", "")),
                "metric_filter_values": _split_multivalue_or_default(
                    query.get("metric_filter", "")
                ),
                "publish_status_scope": query.get("publish_status_scope", "") or "all",
                "expected_source_kind_values": _split_multivalue_or_default(
                    query.get("expected_source_kinds", "")
                ),
                "ambiguity_flag": _parse_boolish(query.get("ambiguity_flag", "")),
                "requires_citation": _parse_boolish(query.get("requires_citation", "")),
                "judged": not query_judgments.empty,
                "has_hard_negative": (
                    not query_judgments.empty
                    and query_judgments["relevance_label"]
                    .map(_normalize_relevance_label)
                    .eq("not_relevant")
                    .any()
                ),
            }
        )
    query_frame = pd.DataFrame(query_records)

    summary = {
        "built_at_utc": datetime.now(UTC).isoformat(),
        "benchmark_dir": str(benchmark_dir_resolved),
        "pack_status": pack.metadata.get("pack_status", "draft"),
        "judgments_source": judgments_source,
        "query_count": int(len(query_frame.index)),
        "judged_query_count": int(query_frame["judged"].sum()) if not query_frame.empty else 0,
        "unjudged_queries": query_frame.loc[~query_frame["judged"], "query_id"].tolist()
        if not query_frame.empty
        else [],
        "coverage_rows": _build_authoring_coverage_rows(query_frame),
        "under_covered_query_groups": _collect_under_covered_query_groups(query_frame),
        "query_groups_without_hard_negatives": _collect_query_groups_without_hard_negatives(
            query_frame
        ),
        "query_groups_without_provenance_sensitive_queries": _collect_query_groups_without_provenance_sensitive_queries(
            query_frame
        ),
        "failure_query_count": 0,
        "top_failure_buckets": [],
        "hard_negative_suggestion_count": 0,
        "hard_negative_type_counts": [],
    }
    if run_dir is not None:
        summary.update(
            collect_authoring_run_context(
                benchmark_dir=benchmark_dir_resolved,
                run_dir=run_dir,
                settings=resolved_settings,
            )
        )

    output_root = (
        (
            resolved_settings.artifacts_dir
            / DEFAULT_AUTHORING_SUBDIR
            / _pack_name_from_dir(benchmark_dir_resolved)
        )
        .expanduser()
        .resolve()
    )
    _ensure_non_analyst_artifact_root(output_root=output_root, settings=resolved_settings)
    output_root.mkdir(parents=True, exist_ok=True)
    coverage_rows_csv_path = output_root / "coverage_rows.csv"
    pd.DataFrame(summary["coverage_rows"]).to_csv(coverage_rows_csv_path, index=False)
    summary_json_path = output_root / "summary.json"
    summary_json_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    summary_markdown_path = output_root / "summary.md"
    summary_markdown_path.write_text(
        _render_authoring_summary_markdown(summary),
        encoding="utf-8",
    )
    return RagBenchmarkAuthoringSummaryRun(
        benchmark_dir=benchmark_dir_resolved,
        summary=summary,
        artifacts=RagBenchmarkAuthoringSummaryArtifacts(
            summary_json_path=summary_json_path,
            summary_markdown_path=summary_markdown_path,
            coverage_rows_csv_path=coverage_rows_csv_path,
        ),
    )


def _benchmark_templates_dir() -> Path:
    return Path(__file__).resolve().parents[3] / "data" / "rag_benchmarks" / "templates"


def _slugify_pack_name(name: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", name.strip()).strip("-")
    if not normalized:
        raise ValueError("Benchmark pack name must contain at least one alphanumeric character.")
    return normalized


def _reviewer_judgments_path(benchmark_dir: Path, reviewer: str) -> Path:
    normalized_reviewer = _slugify_pack_name(reviewer)
    return (
        benchmark_dir.expanduser().resolve()
        / REVIEWERS_DIRNAME
        / normalized_reviewer
        / JUDGMENTS_FILENAME
    )


def _discover_reviewer_judgments_paths(benchmark_dir: Path) -> dict[str, Path]:
    reviewers_root = benchmark_dir.expanduser().resolve() / REVIEWERS_DIRNAME
    if not reviewers_root.exists():
        return {}
    reviewer_paths: dict[str, Path] = {}
    for candidate in sorted(reviewers_root.glob(f"*/{JUDGMENTS_FILENAME}")):
        reviewer_paths[candidate.parent.name] = candidate.resolve()
    return reviewer_paths


def _empty_candidate_results_frame(
    *,
    query_id: str,
    query_text: str,
    retriever_name: str,
    status: str,
    reason: str | None,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "rank": None,
                "score": None,
                "retriever_name": retriever_name,
                "doc_id": "",
                "chunk_id": "",
                "source_kind": "",
                "title": "",
                "artifact_path": "",
                "brand_names": "",
                "metric_names": "",
                "as_of_date": "",
                "publish_status": "",
                "confidence_score": None,
                "source_name": "",
                "source_url_or_doc_id": "",
                "metadata_json": "",
                "citation_present": False,
                "filter_match": False,
                "text": "",
                "query_id": query_id,
                "query_text": query_text,
                "metadata_filters_json": "{}",
                "query_buckets_json": "[]",
                "ambiguity_flag": False,
                "requires_citation": False,
                "suggestion_status": status,
                "suggestion_note": reason
                or "Retriever did not return candidate suggestions for this query.",
            }
        ]
    )


def _build_judgment_workspace(candidate_results: pd.DataFrame) -> pd.DataFrame:
    if candidate_results.empty:
        return pd.DataFrame(
            columns=[
                "query_id",
                "query_text",
                "doc_id",
                "chunk_id",
                "source_kind",
                "title",
                "artifact_path",
                "brand_names",
                "metric_names",
                "publish_status",
                "confidence_score",
                "source_name",
                "source_url_or_doc_id",
                "rank",
                "retriever_name",
                "suggestion_status",
                "suggestion_note",
                "relevance_label",
                "rationale",
                "must_appear_in_top_k",
            ]
        )
    workspace = candidate_results.copy()
    for column in ("relevance_label", "rationale", "must_appear_in_top_k"):
        workspace[column] = ""
    return workspace[
        [
            "query_id",
            "query_text",
            "doc_id",
            "chunk_id",
            "source_kind",
            "title",
            "artifact_path",
            "brand_names",
            "metric_names",
            "publish_status",
            "confidence_score",
            "source_name",
            "source_url_or_doc_id",
            "rank",
            "retriever_name",
            "suggestion_status",
            "suggestion_note",
            "relevance_label",
            "rationale",
            "must_appear_in_top_k",
        ]
    ]


def _normalize_reviewer_rows(
    *,
    normalized_rows: pd.DataFrame,
    chunk_to_doc_id: dict[str, str],
) -> pd.DataFrame:
    rows = normalized_rows.copy()
    rows["reference_type"] = rows.apply(
        lambda row: "chunk" if str(row.get("chunk_id", "")).strip() else "doc",
        axis=1,
    )
    rows["reference_value"] = rows.apply(
        lambda row: (
            str(row.get("chunk_id", "")).strip()
            if str(row.get("chunk_id", "")).strip()
            else str(row.get("doc_id", "")).strip()
        ),
        axis=1,
    )
    rows["doc_scope_id"] = rows.apply(
        lambda row: (
            str(row.get("doc_id", "")).strip()
            if str(row.get("doc_id", "")).strip()
            else str(chunk_to_doc_id.get(str(row.get("chunk_id", "")).strip(), ""))
        ),
        axis=1,
    )
    rows["relevance_label_normalized"] = (
        rows["relevance_label"].fillna("").map(_normalize_relevance_label)
    )
    rows["must_appear_normalized"] = (
        rows["must_appear_in_top_k"].fillna("").map(_normalize_must_appear_threshold)
    )
    rows["rationale_normalized"] = (
        rows["rationale"].fillna("").map(lambda value: str(value).strip())
    )
    return rows


def _compare_reviewer_rows(
    *,
    normalized_rows: pd.DataFrame,
    reviewer_names: list[str],
    force: bool,
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    conflicts: list[dict[str, Any]] = []
    adjudicated_rows: list[dict[str, Any]] = []
    reviewer_set = set(reviewer_names)
    grouped = normalized_rows.groupby(["query_id", "reference_type", "reference_value"], sort=True)
    for (query_id, reference_type, reference_value), group in grouped:
        present_reviewers = sorted(group["reviewer"].tolist())
        missing_reviewers = sorted(reviewer_set - set(present_reviewers))
        relevance_values = sorted(group["relevance_label_normalized"].unique().tolist())
        must_appear_values = sorted(group["must_appear_normalized"].unique().tolist())
        missing_rationale_reviewers = sorted(
            reviewer
            for reviewer, rationale in zip(
                group["reviewer"].tolist(),
                group["rationale_normalized"].tolist(),
                strict=False,
            )
            if not rationale
        )
        conflict_types: list[str] = []
        if missing_reviewers:
            conflict_types.append("missing_judgment")
        if len(relevance_values) > 1:
            conflict_types.append("relevance_label_conflict")
        if len(must_appear_values) > 1:
            conflict_types.append("must_appear_conflict")
        if missing_rationale_reviewers:
            conflict_types.append("missing_rationale")
        canonical_row = group.sort_values(
            by=["reviewer", "reference_type", "reference_value"],
            kind="mergesort",
        ).iloc[0]
        if conflict_types:
            conflicts.append(
                {
                    "query_id": query_id,
                    "reference_type": reference_type,
                    "reference_value": reference_value,
                    "doc_id": canonical_row.get("doc_id", ""),
                    "chunk_id": canonical_row.get("chunk_id", ""),
                    "doc_scope_id": canonical_row.get("doc_scope_id", ""),
                    "present_reviewers": "|".join(present_reviewers),
                    "missing_reviewers": "|".join(missing_reviewers),
                    "relevance_values": "|".join(relevance_values),
                    "must_appear_values": "|".join(must_appear_values),
                    "missing_rationale_reviewers": "|".join(missing_rationale_reviewers),
                    "conflict_type": "|".join(conflict_types),
                }
            )
            if not force:
                continue
        adjudicated_rows.append(
            {
                "query_id": query_id,
                "doc_id": canonical_row.get("doc_id", ""),
                "chunk_id": canonical_row.get("chunk_id", ""),
                "relevance_label": canonical_row.get("relevance_label_normalized", ""),
                "rationale": canonical_row.get("rationale", ""),
                "must_appear_in_top_k": canonical_row.get("must_appear_normalized", ""),
            }
        )
    adjudicated_frame = pd.DataFrame(adjudicated_rows, columns=JUDGMENT_COLUMNS)
    return conflicts, adjudicated_frame


def _load_authoring_summary_judgments(benchmark_dir: Path) -> tuple[pd.DataFrame, str]:
    adjudicated_path = benchmark_dir / ADJUDICATED_JUDGMENTS_FILENAME
    metadata = load_rag_benchmark_metadata(benchmark_dir)
    if adjudicated_path.exists() and metadata.get("pack_status") == "adjudicated":
        return (
            pd.read_csv(adjudicated_path, dtype=str, keep_default_na=False),
            ADJUDICATED_JUDGMENTS_FILENAME,
        )
    reviewer_paths = _discover_reviewer_judgments_paths(benchmark_dir)
    if reviewer_paths:
        reviewer_frames = []
        for reviewer_name in sorted(reviewer_paths):
            frame = pd.read_csv(reviewer_paths[reviewer_name], dtype=str, keep_default_na=False)
            if frame.empty:
                continue
            reviewer_frames.append(frame)
        if reviewer_frames:
            return pd.concat(reviewer_frames, ignore_index=True), "reviewer_union"
    root_judgments_path = benchmark_dir / JUDGMENTS_FILENAME
    return (
        pd.read_csv(root_judgments_path, dtype=str, keep_default_na=False),
        JUDGMENTS_FILENAME,
    )


def _build_authoring_coverage_rows(query_frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rows.extend(_dimension_coverage_rows(query_frame, "query_group", "query_groups"))
    rows.extend(_dimension_coverage_rows(query_frame, "brand_filter", "brand_filter_values"))
    rows.extend(_dimension_coverage_rows(query_frame, "metric_filter", "metric_filter_values"))
    rows.extend(
        _dimension_coverage_rows(
            query_frame,
            "publish_status_scope",
            "publish_status_scope",
            is_list=False,
        )
    )
    rows.extend(
        _dimension_coverage_rows(
            query_frame,
            "expected_source_kinds",
            "expected_source_kind_values",
        )
    )
    rows.extend(
        _dimension_coverage_rows(
            query_frame,
            "ambiguity_flag",
            "ambiguity_flag",
            is_list=False,
        )
    )
    rows.extend(
        _dimension_coverage_rows(
            query_frame,
            "requires_citation",
            "requires_citation",
            is_list=False,
        )
    )
    return sorted(
        rows,
        key=lambda row: (row["dimension"], row["value"]),
    )


def _dimension_coverage_rows(
    query_frame: pd.DataFrame,
    dimension: str,
    column: str,
    *,
    is_list: bool = True,
) -> list[dict[str, Any]]:
    counters: dict[str, dict[str, int]] = {}
    for _, row in query_frame.iterrows():
        values = row[column] if is_list else [row[column]]
        for value in values:
            value_key = str(value)
            counter = counters.setdefault(
                value_key,
                {"query_count": 0, "judged_query_count": 0, "hard_negative_query_count": 0},
            )
            counter["query_count"] += 1
            counter["judged_query_count"] += int(bool(row["judged"]))
            counter["hard_negative_query_count"] += int(bool(row["has_hard_negative"]))
    return [
        {
            "dimension": dimension,
            "value": value,
            **counts,
        }
        for value, counts in counters.items()
    ]


def _collect_under_covered_query_groups(query_frame: pd.DataFrame) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for groups in query_frame["query_groups"].tolist():
        for group in groups:
            counts[group] = counts.get(group, 0) + 1
    return [
        {"query_group": group, "query_count": count}
        for group, count in sorted(counts.items())
        if count < UNDER_COVERED_QUERY_GROUP_THRESHOLD
    ]


def _collect_query_groups_without_hard_negatives(query_frame: pd.DataFrame) -> list[str]:
    result: list[str] = []
    for group in sorted({group for groups in query_frame["query_groups"] for group in groups}):
        members = query_frame.loc[
            query_frame["query_groups"].map(lambda groups, group=group: group in groups)
        ]
        if not members.empty and not members["has_hard_negative"].any():
            result.append(group)
    return result


def _collect_query_groups_without_provenance_sensitive_queries(
    query_frame: pd.DataFrame,
) -> list[str]:
    result: list[str] = []
    for group in sorted({group for groups in query_frame["query_groups"] for group in groups}):
        members = query_frame.loc[
            query_frame["query_groups"].map(lambda groups, group=group: group in groups)
        ]
        if not members.empty and not members["requires_citation"].any():
            result.append(group)
    return result


def _top_counts(rows: list[dict[str, Any]], *, key: str) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key, "")).strip() or "unknown"
        counts[value] = counts.get(value, 0) + 1
    return [
        {"value": value, "count": count}
        for value, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _ensure_non_analyst_artifact_root(*, output_root: Path, settings: Settings) -> None:
    for forbidden_root in (
        settings.reports_dir.expanduser().resolve(),
        settings.strategy_dir.expanduser().resolve(),
    ):
        try:
            output_root.relative_to(forbidden_root)
        except ValueError:
            continue
        raise ValueError(
            "RAG authoring artifacts must not be written under analyst-facing paths like "
            f"{forbidden_root}."
        )


def _pack_name_from_dir(benchmark_dir: Path) -> str:
    return benchmark_dir.expanduser().resolve().name


def _render_benchmark_pack_readme(pack_name: str) -> str:
    return "\n".join(
        [
            f"# {pack_name}",
            "",
            "This pack is for retrieval benchmarking only. It does not create audited facts,",
            "does not authorize answer generation, and should only reference vetted Gold or",
            "provenance-aware local artifacts.",
            "",
            "## Files",
            "",
            "- `queries.csv`: analyst-authored lookup tasks.",
            "- `judgments.csv`: final pack-level judgments when they exist.",
            "- `filters.csv`: optional extra metadata filters.",
            "- `query_groups.csv`: optional grouping for analysis buckets.",
            "- `metadata.json`: pack status and authoring metadata.",
            "- `checklist.md`: authoring and review checklist.",
            "",
            "## Reviewer workflow",
            "",
            "- Create reviewer files under `reviewers/<name>/judgments.csv`.",
            "- Use `working/judgment_workspace.csv` as a suggestion workspace only.",
            "- Do not treat bootstrap suggestions as final judgments.",
            "",
            "## Commands",
            "",
            "```bash",
            "qsr-audit build-rag-corpus",
            f"qsr-audit bootstrap-rag-judgments --benchmark-dir data/rag_benchmarks/{pack_name}",
            f"qsr-audit validate-rag-reviewer-file --benchmark-dir data/rag_benchmarks/{pack_name} --reviewer alice",
            f"qsr-audit adjudicate-rag-benchmark --benchmark-dir data/rag_benchmarks/{pack_name}",
            f"qsr-audit eval-rag-retrieval --benchmark-dir data/rag_benchmarks/{pack_name} --retriever bm25",
            "```",
            "",
            "Generated workflow artifacts belong under `artifacts/rag/...`. Source benchmark pack",
            "files stay under `data/rag_benchmarks/...`.",
            "",
        ]
    )


def _render_benchmark_pack_checklist(pack_name: str) -> str:
    return "\n".join(
        [
            f"# {pack_name} Checklist",
            "",
            "- [ ] Corpus was built from vetted Gold and provenance-aware local artifacts.",
            "- [ ] No raw workbook, Bronze, or Silver evidence is referenced.",
            "- [ ] `query_id` values are unique and reflect realistic analyst lookups.",
            "- [ ] Ambiguity and citation requirements are marked explicitly where needed.",
            "- [ ] Reviewer files live under `reviewers/<name>/judgments.csv` and do not overwrite `judgments.csv`.",
            "- [ ] Reviewer conflicts were adjudicated before treating the benchmark as final.",
            "- [ ] Benchmark outputs are kept under `artifacts/rag/...`, not `reports/` or `strategy/`.",
            "",
        ]
    )


def _render_adjudication_summary_markdown(
    summary: dict[str, Any],
    conflicts_df: pd.DataFrame,
) -> str:
    lines = [
        "# RAG Benchmark Adjudication Summary",
        "",
        f"- Benchmark dir: `{summary['benchmark_dir']}`",
        f"- Reviewers: `{', '.join(summary['reviewers'])}`",
        f"- Reviewer count: `{summary['reviewer_count']}`",
        f"- Minimum reviewer count required: `{summary['minimum_reviewer_count_required']}`",
        f"- Minimum reviewer coverage met: `{summary['minimum_reviewer_coverage_met']}`",
        f"- Adjudicated rows: `{summary['adjudicated_row_count']}`",
        f"- Conflicts: `{summary['conflict_count']}`",
        f"- Force used: `{summary['force_used']}`",
        f"- Forced provisional: `{summary['forced_provisional']}`",
        f"- True adjudication eligible: `{summary['true_adjudication_eligible']}`",
        f"- Pack status: `{summary['pack_status']}`",
        "",
        "## Conflict Types",
        "",
    ]
    if not summary["top_conflict_types"]:
        lines.append("- None.")
    else:
        for row in summary["top_conflict_types"]:
            lines.append(f"- `{row['value']}`: {row['count']}")
    lines.extend(["", "## Conflict Details", ""])
    if conflicts_df.empty:
        lines.append("- None.")
    else:
        for row in conflicts_df.to_dict(orient="records"):
            lines.append(
                f"- `{row['query_id']}` / `{row['reference_type']}` `{row['reference_value']}`: "
                f"{row['conflict_type']}"
            )
    return "\n".join(lines) + "\n"


def _render_authoring_summary_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# RAG Benchmark Authoring Summary",
        "",
        f"- Benchmark dir: `{summary['benchmark_dir']}`",
        f"- Pack status: `{summary['pack_status']}`",
        f"- Judgments source: `{summary['judgments_source']}`",
        f"- Query count: `{summary['query_count']}`",
        f"- Judged query count: `{summary['judged_query_count']}`",
        "",
        "## Unjudged Queries",
        "",
    ]
    if not summary["unjudged_queries"]:
        lines.append("- None.")
    else:
        for query_id in summary["unjudged_queries"]:
            lines.append(f"- `{query_id}`")
    lines.extend(
        [
            "",
            "## Under-Covered Query Groups",
            "",
        ]
    )
    if not summary["under_covered_query_groups"]:
        lines.append("- None.")
    else:
        for row in summary["under_covered_query_groups"]:
            lines.append(f"- `{row['query_group']}`: {row['query_count']} query(s)")
    lines.extend(["", "## Query Groups Without Hard Negatives", ""])
    if not summary["query_groups_without_hard_negatives"]:
        lines.append("- None.")
    else:
        for group in summary["query_groups_without_hard_negatives"]:
            lines.append(f"- `{group}`")
    lines.extend(["", "## Query Groups Without Provenance-Sensitive Questions", ""])
    if not summary["query_groups_without_provenance_sensitive_queries"]:
        lines.append("- None.")
    else:
        for group in summary["query_groups_without_provenance_sensitive_queries"]:
            lines.append(f"- `{group}`")
    lines.extend(["", "## Failure Buckets", ""])
    if not summary["top_failure_buckets"]:
        lines.append("- None.")
    else:
        for row in summary["top_failure_buckets"]:
            lines.append(f"- `{row['value']}`: {row['count']}")
    lines.extend(
        [
            "",
            "## Hard Negative Review Gaps",
            "",
            f"- Suggested hard negatives: `{summary['hard_negative_suggestion_count']}`",
            f"- Failure queries observed: `{summary['failure_query_count']}`",
        ]
    )
    if not summary["hard_negative_type_counts"]:
        lines.append("- Suggested hard-negative types: none.")
    else:
        for row in summary["hard_negative_type_counts"]:
            lines.append(f"- Suggested `{row['value']}` rows: {row['count']}")
    lines.extend(
        [
            "",
            "## Coverage Rows",
            "",
            "| Dimension | Value | Query count | Judged query count | Hard-negative query count |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
    )
    for row in summary["coverage_rows"]:
        lines.append(
            "| "
            + " | ".join(
                [
                    row["dimension"],
                    row["value"],
                    str(row["query_count"]),
                    str(row["judged_query_count"]),
                    str(row["hard_negative_query_count"]),
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def _split_multivalue_or_default(value: str) -> list[str]:
    parts = [item.strip() for item in str(value).split("|") if item.strip()]
    return parts or ["(none)"]


def _parse_boolish(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _normalize_relevance_label(value: Any) -> str:
    normalized = str(value).strip().lower()
    return normalized if normalized in ALLOWED_RELEVANCE_LABELS else normalized


def _normalize_must_appear_threshold(value: Any) -> str:
    text = str(value).strip()
    if not text:
        return ""
    try:
        return str(int(text))
    except ValueError:
        return text


def _append_metadata_note(existing_notes: str, new_note: str) -> str:
    normalized_existing = str(existing_notes).strip()
    normalized_new = str(new_note).strip()
    if not normalized_new:
        return normalized_existing
    if normalized_new in normalized_existing:
        return normalized_existing
    if not normalized_existing:
        return normalized_new
    return f"{normalized_existing} {normalized_new}"


__all__ = [
    "DEFAULT_RAG_BENCHMARKS_ROOT",
    "RagBenchmarkAdjudicationArtifacts",
    "RagBenchmarkAdjudicationRun",
    "RagBenchmarkAuthoringSummaryArtifacts",
    "RagBenchmarkAuthoringSummaryRun",
    "RagBenchmarkInitArtifacts",
    "RagBenchmarkInitRun",
    "RagJudgmentBootstrapArtifacts",
    "RagJudgmentBootstrapRun",
    "adjudicate_rag_benchmark",
    "bootstrap_rag_judgments",
    "init_rag_benchmark",
    "summarize_rag_benchmark_authoring",
    "validate_rag_reviewer_file",
]
