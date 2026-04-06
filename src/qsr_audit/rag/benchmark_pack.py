"""Contracts and validation helpers for analyst-authored RAG benchmark packs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError

from qsr_audit.config import Settings
from qsr_audit.rag.retrieval import row_matches_filters

BENCHMARK_PACK_VERSION = "v1"
DEFAULT_BENCHMARK_VALIDATION_SUBDIR = Path("rag/benchmarks/validation")
BENCHMARK_METADATA_FILENAME = "metadata.json"
JUDGMENTS_FILENAME = "judgments.csv"
ADJUDICATED_JUDGMENTS_FILENAME = "adjudicated_judgments.csv"
ALLOWED_PACK_STATUSES = {"draft", "in_review", "adjudicated"}

QUERY_COLUMNS = [
    "query_id",
    "query_text",
    "language",
    "notes",
    "brand_filter",
    "metric_filter",
    "publish_status_scope",
    "expected_source_kinds",
    "ambiguity_flag",
    "requires_citation",
]
JUDGMENT_COLUMNS = [
    "query_id",
    "doc_id",
    "chunk_id",
    "relevance_label",
    "rationale",
    "must_appear_in_top_k",
]
FILTER_COLUMNS = [
    "query_id",
    "filter_key",
    "filter_value",
    "notes",
]
QUERY_GROUP_COLUMNS = [
    "query_id",
    "query_group",
    "notes",
]
ALLOWED_RELEVANCE_LABELS = {
    "not_relevant": 0,
    "relevant": 1,
    "highly_relevant": 2,
}
ALLOWED_PUBLISH_STATUS_SCOPES = {
    "all",
    "publishable",
    "advisory",
    "blocked",
    "non_blocked",
}
ALLOWED_FILTER_KEYS = {
    "as_of_date",
    "brand_names",
    "confidence_score",
    "doc_id",
    "metric_names",
    "publish_status",
    "source_kind",
    "source_name",
    "source_url_or_doc_id",
}


@dataclass(frozen=True)
class RagBenchmarkPack:
    """Normalized benchmark pack loaded from analyst-authored CSV files."""

    version: str
    benchmark_dir: Path
    metadata: dict[str, Any]
    queries: pd.DataFrame
    judgments: pd.DataFrame
    filters: pd.DataFrame
    query_groups: pd.DataFrame
    judgments_path: Path


@dataclass(frozen=True)
class RagBenchmarkValidationArtifacts:
    """Written output paths for benchmark validation."""

    validation_json_path: Path
    validation_markdown_path: Path
    query_specs_json_path: Path


@dataclass(frozen=True)
class RagBenchmarkValidationRun:
    """Result of validating and normalizing an analyst-authored benchmark pack."""

    pack: RagBenchmarkPack
    query_specs: list[dict[str, Any]]
    issues: list[dict[str, Any]]
    passed: bool
    artifacts: RagBenchmarkValidationArtifacts


def load_rag_benchmark_pack(
    benchmark_dir: Path,
    *,
    judgments_path: Path | None = None,
) -> RagBenchmarkPack:
    """Load a benchmark pack directory with required and optional CSV files."""

    resolved_dir = benchmark_dir.expanduser().resolve()
    if not resolved_dir.exists() or not resolved_dir.is_dir():
        raise FileNotFoundError(
            f"Benchmark directory `{resolved_dir}` was not found or is not a directory."
        )

    metadata = load_rag_benchmark_metadata(resolved_dir)
    resolved_judgments_path = (
        _resolve_pack_path(resolved_dir, judgments_path)
        if judgments_path is not None
        else resolved_dir / JUDGMENTS_FILENAME
    )
    queries = _read_csv(resolved_dir / "queries.csv", QUERY_COLUMNS, required=True)
    judgments = _read_csv(resolved_judgments_path, JUDGMENT_COLUMNS, required=True)
    filters = _read_csv(resolved_dir / "filters.csv", FILTER_COLUMNS, required=False)
    query_groups = _read_csv(
        resolved_dir / "query_groups.csv",
        QUERY_GROUP_COLUMNS,
        required=False,
    )
    return RagBenchmarkPack(
        version=BENCHMARK_PACK_VERSION,
        benchmark_dir=resolved_dir,
        metadata=metadata,
        queries=queries,
        judgments=judgments,
        filters=filters,
        query_groups=query_groups,
        judgments_path=resolved_judgments_path,
    )


def validate_rag_benchmark_pack(
    *,
    benchmark_dir: Path,
    corpus: pd.DataFrame,
    settings: Settings | None = None,
    output_root: Path | None = None,
    judgments_path: Path | None = None,
    require_judgments: bool = True,
) -> RagBenchmarkValidationRun:
    """Validate a benchmark pack and resolve it into normalized query specs."""

    pack = load_rag_benchmark_pack(
        benchmark_dir,
        judgments_path=judgments_path,
    )
    issues: list[dict[str, Any]] = []

    _validate_queries(pack.queries, issues)
    _validate_query_text_intent_conflicts(pack.queries, issues)
    _validate_filters(pack.filters, pack.queries, issues)
    _validate_query_groups(pack.query_groups, pack.queries, issues)
    if require_judgments:
        _validate_judgments(pack.judgments, pack.queries, corpus, issues)

    passed = not any(issue["severity"] == "error" for issue in issues)
    query_specs = []
    if passed:
        query_specs = (
            _build_query_specs(pack=pack, corpus=corpus, issues=issues)
            if require_judgments
            else build_authoring_query_specs_from_pack(pack)
        )

    resolved_settings = settings or Settings()
    resolved_output_root = _resolve_validation_output_root(
        settings=resolved_settings,
        output_root=output_root,
    )
    resolved_output_root.mkdir(parents=True, exist_ok=True)
    artifacts = _write_validation_outputs(
        output_root=resolved_output_root,
        benchmark_dir=pack.benchmark_dir,
        issues=issues,
        query_specs=query_specs,
        pack=pack,
    )
    return RagBenchmarkValidationRun(
        pack=pack,
        query_specs=query_specs,
        issues=issues,
        passed=passed,
        artifacts=artifacts,
    )


def build_query_specs_from_pack(
    pack: RagBenchmarkPack, corpus: pd.DataFrame
) -> list[dict[str, Any]]:
    """Build query specs directly from an already-validated benchmark pack."""

    return _build_query_specs(pack=pack, corpus=corpus, issues=[])


def build_authoring_query_specs_from_pack(pack: RagBenchmarkPack) -> list[dict[str, Any]]:
    """Build query specs from query metadata only, without requiring judgments."""

    filters_by_query: dict[str, list[dict[str, str]]] = {}
    for row in pack.filters.to_dict(orient="records"):
        filters_by_query.setdefault(row["query_id"], []).append(row)

    query_groups_by_query: dict[str, list[str]] = {}
    for row in pack.query_groups.to_dict(orient="records"):
        query_groups_by_query.setdefault(row["query_id"], []).append(row["query_group"])

    return [
        _build_metadata_only_query_spec(
            row=row,
            filters_by_query=filters_by_query,
            query_groups_by_query=query_groups_by_query,
        )
        for row in pack.queries.to_dict(orient="records")
    ]


def load_rag_benchmark_metadata(benchmark_dir: Path) -> dict[str, Any]:
    """Load pack metadata when present, or return a normalized draft default."""

    metadata_path = benchmark_dir / BENCHMARK_METADATA_FILENAME
    if not metadata_path.exists():
        return build_default_rag_benchmark_metadata()
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Benchmark metadata `{metadata_path}` must contain a top-level object.")
    return _normalize_benchmark_metadata(payload)


def build_default_rag_benchmark_metadata(
    *,
    corpus_manifest_path: str | None = None,
    authors: list[str] | None = None,
    pack_status: str = "draft",
    notes: str = "",
    created_at: str | None = None,
) -> dict[str, Any]:
    """Build the default metadata payload for a benchmark pack."""

    normalized_status = pack_status.strip().lower() or "draft"
    if normalized_status not in ALLOWED_PACK_STATUSES:
        raise ValueError(f"Unsupported benchmark pack status `{pack_status}`.")
    return {
        "benchmark_version": BENCHMARK_PACK_VERSION,
        "created_at": created_at or datetime.now(UTC).isoformat(),
        "corpus_manifest_path": corpus_manifest_path or "artifacts/rag/corpus/manifest.json",
        "authors": [str(author).strip() for author in (authors or []) if str(author).strip()],
        "pack_status": normalized_status,
        "notes": notes,
    }


def write_rag_benchmark_metadata(
    benchmark_dir: Path,
    metadata: dict[str, Any],
) -> Path:
    """Write normalized benchmark metadata to the pack directory."""

    metadata_path = benchmark_dir / BENCHMARK_METADATA_FILENAME
    metadata_path.write_text(
        json.dumps(_normalize_benchmark_metadata(metadata), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return metadata_path


def resolve_preferred_judgments_path(benchmark_dir: Path) -> Path:
    """Prefer adjudicated judgments when present, otherwise use the draft root file."""

    resolved_dir = benchmark_dir.expanduser().resolve()
    adjudicated_path = resolved_dir / ADJUDICATED_JUDGMENTS_FILENAME
    if adjudicated_path.exists():
        return adjudicated_path
    return resolved_dir / JUDGMENTS_FILENAME


def render_rag_benchmark_validation_summary(
    *,
    benchmark_dir: Path,
    issues: list[dict[str, Any]],
    pack: RagBenchmarkPack,
    query_specs: list[dict[str, Any]],
) -> str:
    """Render a human-readable markdown summary for benchmark validation."""

    errors = [issue for issue in issues if issue["severity"] == "error"]
    warnings = [issue for issue in issues if issue["severity"] == "warning"]
    infos = [issue for issue in issues if issue["severity"] == "info"]

    lines = [
        "# RAG Benchmark Validation Summary",
        "",
        f"- Contract version: `{pack.version}`",
        f"- Benchmark dir: `{benchmark_dir}`",
        f"- Status: `{'PASS' if not errors else 'FAIL'}`",
        f"- Pack status: `{pack.metadata.get('pack_status', 'draft')}`",
        f"- Queries: `{len(pack.queries.index)}`",
        f"- Judgments: `{len(pack.judgments.index)}`",
        f"- Judgments source: `{pack.judgments_path.name}`",
        f"- Query specs built: `{len(query_specs)}`",
        "",
        "## Errors",
        "",
    ]
    if not errors:
        lines.append("- None.")
    else:
        for issue in errors:
            lines.append(f"- {issue['message']}")

    lines.extend(
        [
            "",
            "## Warnings",
            "",
        ]
    )
    if not warnings:
        lines.append("- None.")
    else:
        for issue in warnings:
            lines.append(f"- {issue['message']}")

    lines.extend(
        [
            "",
            "## Info",
            "",
        ]
    )
    if not infos:
        lines.append("- None.")
    else:
        for issue in infos:
            lines.append(f"- {issue['message']}")

    lines.extend(
        [
            "",
            "## Required Files",
            "",
            "- `queries.csv`",
            "- `judgments.csv`",
            "",
            "## Optional Files",
            "",
            "- `filters.csv`",
            "- `query_groups.csv`",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def _read_csv(path: Path, columns: list[str], *, required: bool) -> pd.DataFrame:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Required benchmark file `{path}` is missing.")
        return pd.DataFrame(columns=columns)
    try:
        frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    except EmptyDataError:
        frame = pd.DataFrame(columns=columns)
    missing_columns = [column for column in columns if column not in frame.columns]
    if missing_columns:
        raise ValueError(
            f"`{path.name}` is missing required columns: {', '.join(missing_columns)}."
        )
    frame = frame[columns].copy()
    for column in columns:
        frame[column] = frame[column].map(
            lambda value: str(value).strip() if value is not None else ""
        )
    return frame


def _resolve_validation_output_root(*, settings: Settings, output_root: Path | None) -> Path:
    resolved = (
        (
            output_root
            if output_root is not None
            else settings.artifacts_dir / DEFAULT_BENCHMARK_VALIDATION_SUBDIR
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
                "RAG benchmark validation artifacts must not be written under analyst-facing "
                f"paths like {forbidden_root}."
            )
    return resolved


def _resolve_pack_path(benchmark_dir: Path, path: Path) -> Path:
    if path.is_absolute():
        return path.expanduser().resolve()
    return (benchmark_dir / path).expanduser().resolve()


def _validate_queries(queries: pd.DataFrame, issues: list[dict[str, Any]]) -> None:
    if queries.empty:
        _issue(issues, "error", "empty_pack", "Benchmark pack must include at least one query row.")
        return

    duplicate_ids = queries.loc[queries["query_id"].duplicated(keep=False), "query_id"].unique()
    for query_id in sorted(str(value) for value in duplicate_ids if str(value).strip()):
        _issue(issues, "error", "duplicate_query_id", f"Duplicate query_id `{query_id}` found.")

    for row in queries.to_dict(orient="records"):
        query_id = row.get("query_id", "")
        if not query_id:
            _issue(issues, "error", "missing_query_id", "A query row is missing `query_id`.")
        if not row.get("query_text"):
            _issue(
                issues,
                "error",
                "missing_query_text",
                f"Query `{query_id or '<missing>'}` is missing `query_text`.",
            )
        if not row.get("language"):
            _issue(
                issues,
                "error",
                "missing_language",
                f"Query `{query_id or '<missing>'}` is missing `language`.",
            )
        _validate_boolean_field(
            issues=issues,
            field_name="ambiguity_flag",
            query_id=query_id,
            value=row.get("ambiguity_flag", ""),
        )
        _validate_boolean_field(
            issues=issues,
            field_name="requires_citation",
            query_id=query_id,
            value=row.get("requires_citation", ""),
        )
        publish_status_scope = row.get("publish_status_scope", "").strip().lower()
        if publish_status_scope and publish_status_scope not in ALLOWED_PUBLISH_STATUS_SCOPES:
            _issue(
                issues,
                "error",
                "invalid_publish_status_scope",
                f"Query `{query_id}` has invalid publish_status_scope `{row['publish_status_scope']}`.",
            )


def _validate_query_text_intent_conflicts(
    queries: pd.DataFrame, issues: list[dict[str, Any]]
) -> None:
    if queries.empty:
        return

    normalized = queries.copy()
    normalized["query_text_key"] = normalized["query_text"].map(lambda value: value.strip().lower())
    signature_columns = [
        "language",
        "brand_filter",
        "metric_filter",
        "publish_status_scope",
        "expected_source_kinds",
        "ambiguity_flag",
        "requires_citation",
    ]
    for query_text, group in normalized.groupby("query_text_key"):
        if not query_text:
            continue
        signatures = {
            tuple(group.iloc[index][column] for column in signature_columns)
            for index in range(len(group.index))
        }
        if len(signatures) > 1:
            query_ids = ", ".join(sorted(group["query_id"].tolist()))
            _issue(
                issues,
                "error",
                "conflicting_query_intent",
                f"Duplicate query text `{group.iloc[0]['query_text']}` has conflicting intent metadata across query IDs: {query_ids}.",
            )


def _validate_filters(
    filters: pd.DataFrame,
    queries: pd.DataFrame,
    issues: list[dict[str, Any]],
) -> None:
    if filters.empty:
        return
    known_query_ids = set(queries["query_id"].tolist())
    for row in filters.to_dict(orient="records"):
        query_id = row.get("query_id", "")
        if not query_id or query_id not in known_query_ids:
            _issue(
                issues,
                "error",
                "dangling_filter_query_id",
                f"Filter row references unknown query_id `{query_id or '<missing>'}`.",
            )
        filter_key = row.get("filter_key", "")
        if not filter_key:
            _issue(
                issues,
                "error",
                "missing_filter_key",
                f"Filter row for query `{query_id or '<missing>'}` is missing `filter_key`.",
            )
        elif filter_key not in ALLOWED_FILTER_KEYS:
            _issue(
                issues,
                "error",
                "invalid_filter_key",
                f"Query `{query_id}` uses unsupported filter_key `{filter_key}`.",
            )
        if not row.get("filter_value"):
            _issue(
                issues,
                "error",
                "missing_filter_value",
                f"Filter row for query `{query_id or '<missing>'}` is missing `filter_value`.",
            )


def _validate_query_groups(
    query_groups: pd.DataFrame,
    queries: pd.DataFrame,
    issues: list[dict[str, Any]],
) -> None:
    if query_groups.empty:
        return
    known_query_ids = set(queries["query_id"].tolist())
    for row in query_groups.to_dict(orient="records"):
        query_id = row.get("query_id", "")
        if not query_id or query_id not in known_query_ids:
            _issue(
                issues,
                "error",
                "dangling_query_group",
                f"Query group row references unknown query_id `{query_id or '<missing>'}`.",
            )
        if not row.get("query_group"):
            _issue(
                issues,
                "error",
                "missing_query_group",
                f"Query group row for `{query_id or '<missing>'}` is missing `query_group`.",
            )


def _validate_judgments(
    judgments: pd.DataFrame,
    queries: pd.DataFrame,
    corpus: pd.DataFrame,
    issues: list[dict[str, Any]],
) -> None:
    if judgments.empty:
        _issue(
            issues,
            "error",
            "empty_pack",
            "Benchmark pack must include at least one judgment row.",
        )
        return

    known_query_ids = set(queries["query_id"].tolist())
    known_doc_ids = set(corpus["doc_id"].tolist())
    known_chunk_ids = set(corpus["chunk_id"].tolist())
    chunk_to_doc_id = corpus.set_index("chunk_id")["doc_id"].to_dict() if not corpus.empty else {}

    seen_refs: dict[tuple[str, str], dict[str, str]] = {}
    for row in judgments.to_dict(orient="records"):
        query_id = row.get("query_id", "")
        doc_id = row.get("doc_id", "")
        chunk_id = row.get("chunk_id", "")
        relevance_label = row.get("relevance_label", "").strip().lower()
        must_appear = row.get("must_appear_in_top_k", "")

        if not query_id or query_id not in known_query_ids:
            _issue(
                issues,
                "error",
                "dangling_judgment_query_id",
                f"Judgment row references unknown query_id `{query_id or '<missing>'}`.",
            )
        if not doc_id and not chunk_id:
            _issue(
                issues,
                "error",
                "missing_judgment_reference",
                f"Judgment row for query `{query_id or '<missing>'}` must provide `doc_id` or `chunk_id`.",
            )
        if relevance_label not in ALLOWED_RELEVANCE_LABELS:
            _issue(
                issues,
                "error",
                "invalid_relevance_label",
                f"Query `{query_id or '<missing>'}` uses invalid relevance_label `{row.get('relevance_label')}`.",
            )
        if not row.get("rationale", "").strip():
            _issue(
                issues,
                "error",
                "missing_rationale",
                f"Query `{query_id or '<missing>'}` is missing `rationale` for `{chunk_id or doc_id or '<missing>'}`.",
            )
        if doc_id and doc_id not in known_doc_ids:
            _issue(
                issues,
                "error",
                "dangling_doc_id",
                f"Query `{query_id}` references unknown doc_id `{doc_id}`.",
            )
        if chunk_id and chunk_id not in known_chunk_ids:
            _issue(
                issues,
                "error",
                "dangling_chunk_id",
                f"Query `{query_id}` references unknown chunk_id `{chunk_id}`.",
            )
        if (
            doc_id
            and chunk_id
            and chunk_id in chunk_to_doc_id
            and chunk_to_doc_id[chunk_id] != doc_id
        ):
            _issue(
                issues,
                "error",
                "mismatched_doc_chunk_reference",
                f"Query `{query_id}` references chunk_id `{chunk_id}` under doc_id `{doc_id}`, but the corpus maps it to `{chunk_to_doc_id[chunk_id]}`.",
            )
        if must_appear:
            try:
                parsed_threshold = int(must_appear)
            except ValueError:
                _issue(
                    issues,
                    "error",
                    "invalid_must_appear_threshold",
                    f"Query `{query_id}` has invalid must_appear_in_top_k `{must_appear}`.",
                )
            else:
                if parsed_threshold < 1:
                    _issue(
                        issues,
                        "error",
                        "invalid_must_appear_threshold",
                        f"Query `{query_id}` must_appear_in_top_k must be >= 1 when provided.",
                    )

        reference_key = chunk_id or f"doc::{doc_id}"
        dedupe_key = (query_id, reference_key)
        snapshot = {
            "relevance_label": relevance_label,
            "must_appear_in_top_k": must_appear or "",
        }
        if dedupe_key in seen_refs:
            prior = seen_refs[dedupe_key]
            if prior != snapshot:
                _issue(
                    issues,
                    "error",
                    "contradictory_judgment",
                    f"Query `{query_id}` has contradictory judgments for `{reference_key}`.",
                )
            else:
                _issue(
                    issues,
                    "error",
                    "duplicate_judgment",
                    f"Query `{query_id}` duplicates the same judgment for `{reference_key}`.",
                )
        else:
            seen_refs[dedupe_key] = snapshot


def _build_query_specs(
    *,
    pack: RagBenchmarkPack,
    corpus: pd.DataFrame,
    issues: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    judgments_by_query: dict[str, list[dict[str, Any]]] = {}
    for row in pack.judgments.to_dict(orient="records"):
        judgments_by_query.setdefault(row["query_id"], []).append(row)

    metadata_only_specs = {
        spec["query_id"]: spec for spec in build_authoring_query_specs_from_pack(pack)
    }

    chunk_to_doc_id = corpus.set_index("chunk_id")["doc_id"].to_dict() if not corpus.empty else {}
    doc_to_chunk_ids: dict[str, list[str]] = {}
    for doc_id, chunk_ids in corpus.groupby("doc_id")["chunk_id"]:
        doc_to_chunk_ids[str(doc_id)] = [str(chunk_id) for chunk_id in chunk_ids.tolist()]

    query_specs: list[dict[str, Any]] = []
    for row in pack.queries.to_dict(orient="records"):
        query_id = row["query_id"]
        query_judgments = judgments_by_query.get(query_id, [])
        if not query_judgments:
            _issue(
                issues,
                "warning",
                "query_without_judgments",
                f"Query `{query_id}` has no judgments and will be skipped during evaluation.",
            )
            continue
        metadata_spec = metadata_only_specs[query_id]

        relevant_chunk_ids: set[str] = set()
        relevant_doc_ids: set[str] = set()
        relevance_by_chunk_id: dict[str, int] = {}
        relevance_by_doc_id: dict[str, int] = {}
        rationale_by_chunk_id: dict[str, str] = {}
        rationale_by_doc_id: dict[str, str] = {}
        must_appear_chunk_ids_in_top_k: dict[str, int] = {}
        must_appear_doc_ids_in_top_k: dict[str, int] = {}
        for judgment in query_judgments:
            judgment_chunk_ids = _resolve_judgment_chunk_ids(judgment=judgment)
            judgment_doc_ids = _resolve_judgment_doc_ids(
                judgment=judgment,
                chunk_to_doc_id=chunk_to_doc_id,
                doc_to_chunk_ids=doc_to_chunk_ids,
            )
            label_score = ALLOWED_RELEVANCE_LABELS.get(
                judgment["relevance_label"].strip().lower(),
                0,
            )
            if label_score > 0:
                relevant_chunk_ids.update(judgment_chunk_ids)
                relevant_doc_ids.update(judgment_doc_ids)
            for chunk_id in sorted(judgment_chunk_ids):
                prior_score = relevance_by_chunk_id.get(chunk_id, -1)
                if label_score > prior_score:
                    relevance_by_chunk_id[chunk_id] = label_score
                    rationale_by_chunk_id[chunk_id] = judgment.get("rationale", "")
                if judgment.get("must_appear_in_top_k"):
                    must_appear_chunk_ids_in_top_k[chunk_id] = int(judgment["must_appear_in_top_k"])
            for doc_id in sorted(judgment_doc_ids):
                prior_score = relevance_by_doc_id.get(doc_id, -1)
                if label_score > prior_score:
                    relevance_by_doc_id[doc_id] = label_score
                    rationale_by_doc_id[doc_id] = judgment.get("rationale", "")
                if judgment.get("must_appear_in_top_k"):
                    must_appear_doc_ids_in_top_k[doc_id] = int(judgment["must_appear_in_top_k"])

        query_specs.append(
            {
                **metadata_spec,
                "relevant_chunk_ids": sorted(relevant_chunk_ids),
                "relevant_doc_ids": sorted(relevant_doc_ids),
                "relevance_by_chunk_id": relevance_by_chunk_id,
                "relevance_by_doc_id": relevance_by_doc_id,
                "rationale_by_chunk_id": rationale_by_chunk_id,
                "rationale_by_doc_id": rationale_by_doc_id,
                "must_appear_chunk_ids_in_top_k": must_appear_chunk_ids_in_top_k,
                "must_appear_doc_ids_in_top_k": must_appear_doc_ids_in_top_k,
            }
        )
    return query_specs


def _build_metadata_only_query_spec(
    *,
    row: dict[str, str],
    filters_by_query: dict[str, list[dict[str, str]]],
    query_groups_by_query: dict[str, list[str]],
) -> dict[str, Any]:
    query_id = row["query_id"]
    metadata_filters: dict[str, Any] = {}
    brand_filter_values = _split_multivalue(row.get("brand_filter", ""))
    if brand_filter_values:
        metadata_filters["brand_names"] = brand_filter_values
    metric_filter_values = _split_multivalue(row.get("metric_filter", ""))
    if metric_filter_values:
        metadata_filters["metric_names"] = metric_filter_values
    publish_status_filter = _publish_status_filter(row.get("publish_status_scope", ""))
    if publish_status_filter:
        metadata_filters["publish_status"] = publish_status_filter
    source_kind_values = _split_multivalue(row.get("expected_source_kinds", ""))
    if source_kind_values:
        metadata_filters["source_kind"] = source_kind_values

    for filter_row in filters_by_query.get(query_id, []):
        metadata_filters[filter_row["filter_key"]] = _filter_value(filter_row["filter_value"])

    query_groups = sorted(set(query_groups_by_query.get(query_id, [])))
    built_in_buckets = _derive_query_buckets(
        query=row,
        metadata_filters=metadata_filters,
        query_groups=query_groups,
    )
    return {
        "query_id": query_id,
        "query": row["query_text"],
        "language": row["language"],
        "notes": row.get("notes") or None,
        "metadata_filters": metadata_filters,
        "brand_filter_values": brand_filter_values,
        "metric_filter_values": metric_filter_values,
        "expected_source_kinds": source_kind_values,
        "publish_status_scope": row.get("publish_status_scope", "") or "all",
        "ambiguity_flag": _parse_bool(row.get("ambiguity_flag", "")),
        "requires_citation": _parse_bool(row.get("requires_citation", "")),
        "query_groups": query_groups,
        "query_buckets": built_in_buckets,
    }


def _resolve_judgment_chunk_ids(
    *,
    judgment: dict[str, str],
) -> list[str]:
    chunk_id = judgment.get("chunk_id", "")
    if chunk_id:
        return [chunk_id]
    return []


def _resolve_judgment_doc_ids(
    *,
    judgment: dict[str, str],
    chunk_to_doc_id: dict[str, str],
    doc_to_chunk_ids: dict[str, list[str]],
) -> list[str]:
    doc_id = judgment.get("doc_id", "")
    if doc_id and doc_id in doc_to_chunk_ids:
        return [doc_id]
    chunk_id = judgment.get("chunk_id", "")
    if chunk_id and chunk_id in chunk_to_doc_id:
        return []
    return []


def _derive_query_buckets(
    *,
    query: dict[str, str],
    metadata_filters: dict[str, Any],
    query_groups: list[str],
) -> list[str]:
    buckets = set(query_groups)
    if _split_multivalue(query.get("brand_filter", "")):
        buckets.add("brand_specific")
    if _split_multivalue(query.get("metric_filter", "")):
        buckets.add("metric_specific")
    if (
        _parse_bool(query.get("requires_citation", ""))
        or "provenance" in " ".join(query_groups).lower()
    ):
        buckets.add("provenance_citation")
    brand_filter_values = _split_multivalue(query.get("brand_filter", ""))
    query_text = query.get("query_text", "").lower()
    if len(brand_filter_values) > 1 or "compare" in query_text or " vs " in query_text:
        buckets.add("cross_brand_comparison")
    if len(metadata_filters) >= 2:
        buckets.add("metadata_filter_heavy")
    if _parse_bool(query.get("ambiguity_flag", "")):
        buckets.add("ambiguous")
    return sorted(buckets)


def _publish_status_filter(value: str) -> list[str] | str | None:
    normalized = value.strip().lower()
    if not normalized or normalized == "all":
        return None
    if normalized == "non_blocked":
        return ["publishable", "advisory"]
    return normalized


def _write_validation_outputs(
    *,
    output_root: Path,
    benchmark_dir: Path,
    issues: list[dict[str, Any]],
    query_specs: list[dict[str, Any]],
    pack: RagBenchmarkPack,
) -> RagBenchmarkValidationArtifacts:
    validation_json_path = output_root / "validation_results.json"
    validation_markdown_path = output_root / "validation_summary.md"
    query_specs_json_path = output_root / "query_specs.json"

    validation_payload = {
        "contract_version": pack.version,
        "validated_at_utc": datetime.now(UTC).isoformat(),
        "benchmark_dir": str(benchmark_dir),
        "query_count": int(len(pack.queries.index)),
        "judgment_count": int(len(pack.judgments.index)),
        "issue_count": len(issues),
        "issues": issues,
    }
    validation_json_path.write_text(
        json.dumps(validation_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    validation_markdown_path.write_text(
        render_rag_benchmark_validation_summary(
            benchmark_dir=benchmark_dir,
            issues=issues,
            pack=pack,
            query_specs=query_specs,
        ),
        encoding="utf-8",
    )
    query_specs_json_path.write_text(
        json.dumps(query_specs, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return RagBenchmarkValidationArtifacts(
        validation_json_path=validation_json_path,
        validation_markdown_path=validation_markdown_path,
        query_specs_json_path=query_specs_json_path,
    )


def _filter_value(value: str) -> list[str] | str:
    parsed = _split_multivalue(value)
    return parsed if len(parsed) > 1 else (parsed[0] if parsed else value)


def _split_multivalue(value: str) -> list[str]:
    return [item.strip() for item in value.split("|") if item.strip()]


def _validate_boolean_field(
    *,
    issues: list[dict[str, Any]],
    field_name: str,
    query_id: str,
    value: str,
) -> None:
    if not value:
        return
    try:
        _parse_bool(value)
    except ValueError:
        _issue(
            issues,
            "error",
            "invalid_boolean_field",
            f"Query `{query_id or '<missing>'}` has invalid {field_name} `{value}`.",
        )


def _parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if not normalized:
        return False
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"Unsupported boolean value `{value}`.")


def _issue(
    issues: list[dict[str, Any]],
    severity: str,
    category: str,
    message: str,
    *,
    details: dict[str, Any] | None = None,
) -> None:
    issues.append(
        {
            "severity": severity,
            "category": category,
            "message": message,
            "details": details or {},
        }
    )


def _is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
    except ValueError:
        return False
    return True


def _normalize_benchmark_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    baseline = build_default_rag_benchmark_metadata()
    normalized = {
        "benchmark_version": str(
            payload.get("benchmark_version", baseline["benchmark_version"])
        ).strip()
        or baseline["benchmark_version"],
        "created_at": str(payload.get("created_at", baseline["created_at"])).strip()
        or baseline["created_at"],
        "corpus_manifest_path": str(
            payload.get("corpus_manifest_path", baseline["corpus_manifest_path"])
        ).strip()
        or baseline["corpus_manifest_path"],
        "authors": [
            str(author).strip()
            for author in payload.get("authors", baseline["authors"])
            if str(author).strip()
        ],
        "pack_status": str(payload.get("pack_status", baseline["pack_status"])).strip().lower()
        or baseline["pack_status"],
        "notes": str(payload.get("notes", baseline["notes"])),
    }
    if normalized["pack_status"] not in ALLOWED_PACK_STATUSES:
        raise ValueError(
            f"Benchmark metadata has unsupported pack_status `{normalized['pack_status']}`."
        )
    return normalized


def benchmark_query_matches_row(query_spec: dict[str, Any], row: dict[str, Any]) -> bool:
    """Apply a normalized benchmark query's metadata filters to a corpus row."""

    return row_matches_filters(row, query_spec.get("metadata_filters") or {})


__all__ = [
    "ALLOWED_RELEVANCE_LABELS",
    "ADJUDICATED_JUDGMENTS_FILENAME",
    "BENCHMARK_PACK_VERSION",
    "BENCHMARK_METADATA_FILENAME",
    "DEFAULT_BENCHMARK_VALIDATION_SUBDIR",
    "FILTER_COLUMNS",
    "JUDGMENT_COLUMNS",
    "JUDGMENTS_FILENAME",
    "QUERY_COLUMNS",
    "QUERY_GROUP_COLUMNS",
    "RagBenchmarkPack",
    "RagBenchmarkValidationArtifacts",
    "RagBenchmarkValidationRun",
    "benchmark_query_matches_row",
    "build_authoring_query_specs_from_pack",
    "build_default_rag_benchmark_metadata",
    "build_query_specs_from_pack",
    "load_rag_benchmark_pack",
    "load_rag_benchmark_metadata",
    "resolve_preferred_judgments_path",
    "render_rag_benchmark_validation_summary",
    "validate_rag_benchmark_pack",
    "write_rag_benchmark_metadata",
]
