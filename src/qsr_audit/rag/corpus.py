"""Local corpus builder for retrieval-only RAG experiments."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.config import Settings

DEFAULT_CORPUS_SUBDIR = Path("rag/corpus")
DEFAULT_CHUNK_CHARS = 900
DEFAULT_OVERLAP_CHARS = 120
MANUAL_REFERENCE_NOTE_CANDIDATES = (
    "manual_reference_notes.parquet",
    "manual_reference_notes.csv",
)


@dataclass(frozen=True)
class RagCorpusArtifacts:
    """Written output paths for a retrieval corpus build."""

    corpus_parquet_path: Path
    corpus_jsonl_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class RagCorpusRun:
    """Result of building a retrieval corpus from vetted local artifacts."""

    corpus: pd.DataFrame
    manifest: dict[str, Any]
    artifacts: RagCorpusArtifacts


def build_rag_corpus(
    *,
    settings: Settings | None = None,
    output_root: Path | None = None,
    max_chunk_chars: int = DEFAULT_CHUNK_CHARS,
    overlap_chars: int = DEFAULT_OVERLAP_CHARS,
) -> RagCorpusRun:
    """Build a retrieval-only corpus from vetted Gold and reviewed local artifacts."""

    resolved_settings = settings or Settings()
    resolved_output_root = _resolve_output_root(
        output_root=output_root,
        settings=resolved_settings,
    )
    resolved_output_root.mkdir(parents=True, exist_ok=True)

    source_records: list[dict[str, Any]] = []
    documents: list[dict[str, Any]] = []

    gold_decisions_path = resolved_settings.data_gold / "gold_publish_decisions.parquet"
    publishable_path = resolved_settings.data_gold / "publishable_kpis.parquet"
    blocked_path = resolved_settings.data_gold / "blocked_kpis.parquet"

    if gold_decisions_path.exists():
        decision_docs = _load_gold_publish_decisions(gold_decisions_path)
        documents.extend(decision_docs)
        source_records.append(
            _source_record(
                source_kind="gold_publish_decisions",
                artifact_path=gold_decisions_path,
                status="included",
                doc_count=len(decision_docs),
            )
        )
        for redundant_path, source_kind in [
            (publishable_path, "gold_publishable_subset"),
            (blocked_path, "gold_blocked_subset"),
        ]:
            source_records.append(
                _source_record(
                    source_kind=source_kind,
                    artifact_path=redundant_path,
                    status="skipped_redundant" if redundant_path.exists() else "missing",
                    doc_count=0,
                    reason=(
                        "Skipped because `gold_publish_decisions.parquet` already provides the "
                        "full publish-status audit log."
                        if redundant_path.exists()
                        else "Optional artifact was not present."
                    ),
                )
            )
    else:
        source_records.append(
            _source_record(
                source_kind="gold_publish_decisions",
                artifact_path=gold_decisions_path,
                status="missing",
                doc_count=0,
                reason="Optional artifact was not present.",
            )
        )
        for subset_path, source_kind in [
            (publishable_path, "gold_publishable_subset"),
            (blocked_path, "gold_blocked_subset"),
        ]:
            documents_for_subset = _maybe_load_parquet_docs(
                subset_path,
                loader=_load_gold_publish_decisions,
            )
            documents.extend(documents_for_subset)
            source_records.append(
                _source_record(
                    source_kind=source_kind,
                    artifact_path=subset_path,
                    status="included" if documents_for_subset else "missing",
                    doc_count=len(documents_for_subset),
                    reason=None if documents_for_subset else "Optional artifact was not present.",
                )
            )

    for source_kind, path, loader in [
        (
            "gold_reconciled_core_metrics",
            resolved_settings.data_gold / "reconciled_core_metrics.parquet",
            _load_reconciled_core_metrics,
        ),
        (
            "gold_reference_coverage",
            resolved_settings.data_gold / "reference_coverage.parquet",
            _load_reference_coverage,
        ),
        (
            "gold_validation_flags",
            resolved_settings.data_gold / "validation_flags.parquet",
            _load_validation_flags,
        ),
        (
            "gold_provenance_registry",
            resolved_settings.data_gold / "provenance_registry.parquet",
            _load_provenance_registry,
        ),
        (
            "validation_summary_markdown",
            resolved_settings.reports_dir / "validation" / "validation_summary.md",
            _load_validation_summary_markdown,
        ),
    ]:
        loaded_docs = _maybe_load_source(path=path, loader=loader)
        documents.extend(loaded_docs)
        source_records.append(
            _source_record(
                source_kind=source_kind,
                artifact_path=path,
                status="included" if loaded_docs else "missing",
                doc_count=len(loaded_docs),
                reason=None if loaded_docs else "Optional artifact was not present.",
            )
        )

    manual_notes_docs, manual_notes_path = _load_manual_reference_notes(resolved_settings)
    documents.extend(manual_notes_docs)
    source_records.append(
        _source_record(
            source_kind="manual_reference_notes",
            artifact_path=manual_notes_path,
            status="included" if manual_notes_docs else "missing",
            doc_count=len(manual_notes_docs),
            reason=None if manual_notes_docs else "Optional artifact was not present.",
        )
    )

    chunk_rows = _chunk_documents(
        documents=documents,
        max_chunk_chars=max_chunk_chars,
        overlap_chars=overlap_chars,
    )
    corpus = pd.DataFrame(columns=_corpus_columns()) if not chunk_rows else pd.DataFrame(chunk_rows)
    if not corpus.empty:
        corpus = corpus[_corpus_columns()]

    corpus_parquet_path = resolved_output_root / "corpus.parquet"
    corpus_jsonl_path = resolved_output_root / "corpus.jsonl"
    manifest_path = resolved_output_root / "manifest.json"

    corpus.to_parquet(corpus_parquet_path, index=False)
    _write_jsonl(corpus, corpus_jsonl_path)

    for record in source_records:
        if record["status"] == "included":
            matching = corpus.loc[corpus["artifact_path"] == record["artifact_path"]]
            record["chunk_count"] = int(len(matching))

    manifest = {
        "corpus_version": "v1",
        "built_at_utc": datetime.now(UTC).isoformat(),
        "chunking_version": "v1",
        "chunk_count": int(len(corpus)),
        "document_count": int(corpus["doc_id"].nunique()) if not corpus.empty else 0,
        "source_kinds": sorted(corpus["source_kind"].unique().tolist()) if not corpus.empty else [],
        "policy_exclusions": [
            {
                "path": str(resolved_settings.data_raw),
                "reason": "Raw workbook inputs are excluded from retrieval by default.",
            },
            {
                "path": str(resolved_settings.data_bronze),
                "reason": "Bronze artifacts are working-layer dumps and are excluded from retrieval.",
            },
            {
                "path": str(resolved_settings.data_silver),
                "reason": (
                    "Silver artifacts are working-layer tables and are excluded from retrieval by "
                    "default. Only optional manual reference notes under `data/reference/` may be "
                    "indexed when present."
                ),
            },
        ],
        "sources": source_records,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    return RagCorpusRun(
        corpus=corpus,
        manifest=manifest,
        artifacts=RagCorpusArtifacts(
            corpus_parquet_path=corpus_parquet_path,
            corpus_jsonl_path=corpus_jsonl_path,
            manifest_path=manifest_path,
        ),
    )


def load_rag_corpus(corpus_path: Path) -> pd.DataFrame:
    """Load a previously built corpus parquet file."""

    return pd.read_parquet(corpus_path)


def _resolve_output_root(*, output_root: Path | None, settings: Settings) -> Path:
    resolved = (
        (output_root if output_root is not None else settings.artifacts_dir / DEFAULT_CORPUS_SUBDIR)
        .expanduser()
        .resolve()
    )
    for forbidden_root in (
        settings.reports_dir.expanduser().resolve(),
        settings.strategy_dir.expanduser().resolve(),
    ):
        if _is_relative_to(resolved, forbidden_root):
            raise ValueError(
                "RAG corpus artifacts must not be written under analyst-facing paths like "
                f"{forbidden_root}."
            )
    return resolved


def _maybe_load_parquet_docs(
    path: Path, *, loader: Callable[[Path], list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return loader(path)


def _maybe_load_source(
    *, path: Path, loader: Callable[[Path], list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return loader(path)


def _load_gold_publish_decisions(path: Path) -> list[dict[str, Any]]:
    frame = pd.read_parquet(path)
    documents: list[dict[str, Any]] = []
    for row in frame.to_dict(orient="records"):
        brand_name = _first_non_blank(row.get("canonical_brand_name"), row.get("brand_name"))
        metric_name = row.get("metric_name")
        publish_status = row.get("publish_status")
        blocking_reasons = _json_list(row.get("blocking_reasons"))
        warning_reasons = _json_list(row.get("warning_reasons"))
        validation_references = _json_list(row.get("validation_references"))
        title = f"Gold publish decision - {brand_name or 'unknown brand'} - {metric_name}"
        text = "\n".join(
            [
                f"Gold publish decision for {brand_name or 'unknown brand'}.",
                f"Metric: {metric_name}",
                f"Metric value: {row.get('metric_value')}",
                f"Publish status: {publish_status}",
                f"Blocking reasons: {_join_or_none(blocking_reasons)}",
                f"Warning reasons: {_join_or_none(warning_reasons)}",
                f"Validation references: {_join_or_none(validation_references)}",
                f"Source type: {row.get('source_type') or 'unknown'}",
                f"Source name: {row.get('source_name') or 'unknown'}",
                f"Source citation: {row.get('source_url_or_doc_id') or 'none'}",
                f"As of date: {row.get('as_of_date') or 'unknown'}",
                f"Method: {row.get('method_reported_or_estimated') or 'unknown'}",
                f"Confidence score: {row.get('confidence_score')}",
            ]
        )
        documents.append(
            {
                "doc_id": _make_doc_id(
                    "gold_publish_decision",
                    brand_name,
                    metric_name,
                    publish_status,
                ),
                "source_kind": "gold_publish_decision",
                "title": title,
                "text": text,
                "artifact_path": str(path),
                "brand_names": _stable_json_list([brand_name, row.get("brand_name")]),
                "metric_names": _stable_json_list([metric_name]),
                "as_of_date": _string_or_none(row.get("as_of_date")),
                "publish_status": _string_or_none(publish_status),
                "confidence_score": _float_or_none(row.get("confidence_score")),
                "source_name": _string_or_none(row.get("source_name")),
                "source_url_or_doc_id": _string_or_none(row.get("source_url_or_doc_id")),
                "metadata_json": json.dumps(
                    {
                        "blocking_reasons": blocking_reasons,
                        "warning_reasons": warning_reasons,
                        "validation_references": validation_references,
                        "method_reported_or_estimated": row.get("method_reported_or_estimated"),
                        "source_type": row.get("source_type"),
                        "reconciliation_grade": row.get("reconciliation_grade"),
                        "reference_source_count": row.get("reference_source_count"),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            }
        )
    return documents


def _load_reconciled_core_metrics(path: Path) -> list[dict[str, Any]]:
    frame = pd.read_parquet(path)
    documents: list[dict[str, Any]] = []
    for row in frame.to_dict(orient="records"):
        brand_name = _first_non_blank(row.get("canonical_brand_name"), row.get("brand_name"))
        metrics = [
            ("rank", row.get("rank")),
            ("store_count", row.get("us_store_count_2024")),
            ("system_sales", row.get("systemwide_revenue_usd_billions_2024")),
            ("auv", row.get("average_unit_volume_usd_thousands")),
            ("fte_mid", row.get("fte_mid")),
            ("margin_mid_pct", row.get("margin_mid_pct")),
        ]
        text = "\n".join(
            [
                f"Reconciled workbook claim for {brand_name or 'unknown brand'}.",
                f"Category: {row.get('category') or 'unknown'}",
                *[f"{metric_name}: {metric_value}" for metric_name, metric_value in metrics],
                f"Overall credibility grade: {row.get('overall_credibility_grade') or 'unknown'}",
                f"Reference source count: {row.get('reference_source_count') or 0}",
                f"Reconciliation warning: {row.get('reconciliation_warning') or 'none'}",
            ]
        )
        documents.append(
            {
                "doc_id": _make_doc_id("gold_reconciled_core_metrics", brand_name),
                "source_kind": "gold_reconciled_core_metrics",
                "title": f"Reconciled core metrics - {brand_name or 'unknown brand'}",
                "text": text,
                "artifact_path": str(path),
                "brand_names": _stable_json_list([brand_name, row.get("brand_name")]),
                "metric_names": _stable_json_list([name for name, _ in metrics]),
                "as_of_date": None,
                "publish_status": None,
                "confidence_score": _float_or_none(row.get("brand_match_confidence")),
                "source_name": "reconciled_core_metrics",
                "source_url_or_doc_id": None,
                "metadata_json": json.dumps(
                    {
                        "brand_match_confidence": row.get("brand_match_confidence"),
                        "brand_match_method": row.get("brand_match_method"),
                        "overall_credibility_grade": row.get("overall_credibility_grade"),
                        "reconciliation_warning": row.get("reconciliation_warning"),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            }
        )
    return documents


def _load_reference_coverage(path: Path) -> list[dict[str, Any]]:
    frame = pd.read_parquet(path)
    documents: list[dict[str, Any]] = []
    for row in frame.to_dict(orient="records"):
        brand_name = _first_non_blank(row.get("canonical_brand_name"), row.get("brand_name"))
        metric_name = row.get("metric_name")
        source_type = row.get("source_type")
        title = (
            f"Reference coverage - {row.get('coverage_kind')} - "
            f"{brand_name or metric_name or source_type or 'summary'}"
        )
        text = "\n".join(
            [
                f"Reference coverage row of kind {row.get('coverage_kind')}.",
                f"Brand: {brand_name or 'n/a'}",
                f"Metric: {metric_name or 'n/a'}",
                f"Source type: {source_type or 'n/a'}",
                f"Reference row count: {row.get('reference_row_count')}",
                f"Reference source count: {row.get('reference_source_count')}",
                f"Covered metrics count: {row.get('covered_metrics_count')}",
                f"Covered brand count: {row.get('covered_brand_count')}",
                f"Coverage rate: {row.get('coverage_rate')}",
                f"Missing metrics: {_join_or_none(_json_list(row.get('missing_metrics')))}",
                f"Warning: {row.get('warning') or 'none'}",
                f"Provenance summary: {row.get('provenance_completeness_summary') or 'none'}",
                f"Confidence summary: {row.get('provenance_confidence_summary') or 'none'}",
            ]
        )
        documents.append(
            {
                "doc_id": _make_doc_id(
                    "gold_reference_coverage",
                    row.get("coverage_kind"),
                    brand_name,
                    metric_name,
                    source_type,
                ),
                "source_kind": "gold_reference_coverage",
                "title": title,
                "text": text,
                "artifact_path": str(path),
                "brand_names": _stable_json_list([brand_name]),
                "metric_names": _stable_json_list([metric_name]),
                "as_of_date": None,
                "publish_status": None,
                "confidence_score": _float_or_none(row.get("provenance_completeness_score")),
                "source_name": _string_or_none(source_type),
                "source_url_or_doc_id": None,
                "metadata_json": json.dumps(
                    {
                        "coverage_kind": row.get("coverage_kind"),
                        "warning": row.get("warning"),
                        "details": _json_object(row.get("details")),
                        "source_type_names": _json_list(row.get("source_type_names")),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            }
        )
    return documents


def _load_validation_flags(path: Path) -> list[dict[str, Any]]:
    frame = pd.read_parquet(path)
    documents: list[dict[str, Any]] = []
    for index, row in enumerate(frame.to_dict(orient="records"), start=1):
        brand_name = row.get("brand_name")
        title = (
            f"Validation finding - {row.get('severity')} - "
            f"{brand_name or row.get('dataset') or row.get('check_name')}"
        )
        text = "\n".join(
            [
                f"Validation finding with severity {row.get('severity')}.",
                f"Dataset: {row.get('dataset') or 'unknown'}",
                f"Category: {row.get('category') or 'unknown'}",
                f"Check: {row.get('check_name') or 'unknown'}",
                f"Message: {row.get('message') or 'none'}",
                f"Brand: {brand_name or 'n/a'}",
                f"Field: {row.get('field_name') or 'n/a'}",
                f"Row number: {row.get('row_number') or 'n/a'}",
                f"Expected: {row.get('expected') or 'n/a'}",
                f"Observed: {row.get('observed') or 'n/a'}",
            ]
        )
        documents.append(
            {
                "doc_id": _make_doc_id(
                    "gold_validation_flag",
                    row.get("dataset"),
                    row.get("check_name"),
                    brand_name,
                    index,
                ),
                "source_kind": "gold_validation_flag",
                "title": title,
                "text": text,
                "artifact_path": str(path),
                "brand_names": _stable_json_list([brand_name]),
                "metric_names": _stable_json_list([row.get("field_name")]),
                "as_of_date": None,
                "publish_status": None,
                "confidence_score": None,
                "source_name": "validation_flags",
                "source_url_or_doc_id": None,
                "metadata_json": json.dumps(
                    {
                        "severity": row.get("severity"),
                        "category": row.get("category"),
                        "dataset": row.get("dataset"),
                        "field_name": row.get("field_name"),
                        "details": _json_object(row.get("details")),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            }
        )
    return documents


def _load_provenance_registry(path: Path) -> list[dict[str, Any]]:
    frame = pd.read_parquet(path)
    documents: list[dict[str, Any]] = []
    for index, row in enumerate(frame.to_dict(orient="records"), start=1):
        extra = _json_object(row.get("extra"))
        brand_name = _first_non_blank(
            extra.get("canonical_brand_name"),
            row.get("source_name"),
        )
        text = "\n".join(
            [
                f"Provenance record for {brand_name or 'unknown brand'}.",
                f"Source type: {row.get('source_type') or 'unknown'}",
                f"Source name: {row.get('source_name') or 'unknown'}",
                f"Source citation: {row.get('source_url_or_doc_id') or 'none'}",
                f"As of date: {row.get('as_of_date') or 'unknown'}",
                f"Method: {row.get('method_reported_or_estimated') or 'unknown'}",
                f"Confidence score: {row.get('confidence_score')}",
                f"Notes: {row.get('notes') or 'none'}",
            ]
        )
        documents.append(
            {
                "doc_id": _make_doc_id("gold_provenance_registry", brand_name, index),
                "source_kind": "gold_provenance_registry",
                "title": f"Provenance record - {brand_name or 'unknown brand'}",
                "text": text,
                "artifact_path": str(path),
                "brand_names": _stable_json_list([brand_name]),
                "metric_names": _stable_json_list([extra.get("metric_name")]),
                "as_of_date": _string_or_none(row.get("as_of_date")),
                "publish_status": _string_or_none(extra.get("publish_status")),
                "confidence_score": _float_or_none(row.get("confidence_score")),
                "source_name": _string_or_none(row.get("source_name")),
                "source_url_or_doc_id": _string_or_none(row.get("source_url_or_doc_id")),
                "metadata_json": json.dumps(
                    {
                        "method_reported_or_estimated": row.get("method_reported_or_estimated"),
                        "notes": row.get("notes"),
                        "extra": extra,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            }
        )
    return documents


def _load_validation_summary_markdown(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    return [
        {
            "doc_id": _make_doc_id("validation_summary_markdown", path.stem),
            "source_kind": "validation_summary_markdown",
            "title": "Validation summary markdown",
            "text": text,
            "artifact_path": str(path),
            "brand_names": _stable_json_list([]),
            "metric_names": _stable_json_list([]),
            "as_of_date": None,
            "publish_status": None,
            "confidence_score": None,
            "source_name": path.name,
            "source_url_or_doc_id": None,
            "metadata_json": json.dumps({"format": "markdown"}, ensure_ascii=False, sort_keys=True),
        }
    ]


def _load_manual_reference_notes(settings: Settings) -> tuple[list[dict[str, Any]], Path]:
    for candidate_name in MANUAL_REFERENCE_NOTE_CANDIDATES:
        candidate_path = settings.data_reference / candidate_name
        if not candidate_path.exists():
            continue
        frame = (
            pd.read_parquet(candidate_path)
            if candidate_path.suffix == ".parquet"
            else pd.read_csv(candidate_path)
        )
        documents: list[dict[str, Any]] = []
        for index, row in enumerate(frame.to_dict(orient="records"), start=1):
            note_text = _first_non_blank(row.get("note_text"), row.get("text"), row.get("note"))
            if note_text is None:
                continue
            brand_name = _first_non_blank(row.get("canonical_brand_name"), row.get("brand_name"))
            field_name = _first_non_blank(row.get("metric_name"), row.get("field_name"))
            title = f"Manual reference note - {brand_name or field_name or index}"
            documents.append(
                {
                    "doc_id": _make_doc_id("manual_reference_note", brand_name, field_name, index),
                    "source_kind": "manual_reference_note",
                    "title": title,
                    "text": str(note_text),
                    "artifact_path": str(candidate_path),
                    "brand_names": _stable_json_list([brand_name]),
                    "metric_names": _stable_json_list([field_name]),
                    "as_of_date": _string_or_none(row.get("as_of_date")),
                    "publish_status": _string_or_none(row.get("publish_status")),
                    "confidence_score": _float_or_none(row.get("confidence_score")),
                    "source_name": _string_or_none(row.get("source_name") or candidate_path.name),
                    "source_url_or_doc_id": _string_or_none(row.get("source_url_or_doc_id")),
                    "metadata_json": json.dumps(
                        row, ensure_ascii=False, sort_keys=True, default=str
                    ),
                }
            )
        return documents, candidate_path
    return [], settings.data_reference / MANUAL_REFERENCE_NOTE_CANDIDATES[0]


def _chunk_documents(
    *, documents: list[dict[str, Any]], max_chunk_chars: int, overlap_chars: int
) -> list[dict[str, Any]]:
    chunk_rows: list[dict[str, Any]] = []
    for document in documents:
        chunks = _deterministic_chunks(
            document["text"],
            max_chunk_chars=max_chunk_chars,
            overlap_chars=overlap_chars,
        )
        for index, chunk_text in enumerate(chunks, start=1):
            chunk_rows.append(
                {
                    **document,
                    "chunk_id": f"{document['doc_id']}::chunk-{index:03d}",
                    "text": chunk_text,
                }
            )
    return chunk_rows


def _deterministic_chunks(text: str, *, max_chunk_chars: int, overlap_chars: int) -> list[str]:
    normalized = text.strip()
    if not normalized:
        return [""]
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", normalized) if part.strip()]
    chunks: list[str] = []
    buffer = ""
    for paragraph in paragraphs:
        candidate = paragraph if not buffer else f"{buffer}\n\n{paragraph}"
        if len(candidate) <= max_chunk_chars:
            buffer = candidate
            continue
        if buffer:
            chunks.append(buffer)
            buffer = ""
        if len(paragraph) <= max_chunk_chars:
            buffer = paragraph
            continue
        chunks.extend(
            _split_long_text(
                paragraph, max_chunk_chars=max_chunk_chars, overlap_chars=overlap_chars
            )
        )
    if buffer:
        chunks.append(buffer)
    return chunks or [normalized]


def _split_long_text(text: str, *, max_chunk_chars: int, overlap_chars: int) -> list[str]:
    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = min(start + max_chunk_chars, len(text))
        chunks.append(text[start:end].strip())
        if end >= len(text):
            break
        start = max(end - overlap_chars, start + 1)
    return [chunk for chunk in chunks if chunk]


def _write_jsonl(frame: pd.DataFrame, path: Path) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in frame.to_dict(orient="records"):
            handle.write(json.dumps(row, ensure_ascii=False, default=str))
            handle.write("\n")


def _source_record(
    *,
    source_kind: str,
    artifact_path: Path,
    status: str,
    doc_count: int,
    reason: str | None = None,
) -> dict[str, Any]:
    return {
        "source_kind": source_kind,
        "artifact_path": str(artifact_path),
        "status": status,
        "doc_count": doc_count,
        "chunk_count": 0,
        "reason": reason,
    }


def _make_doc_id(*parts: object) -> str:
    slug = "-".join(_slugify(part) for part in parts if _slugify(part))
    return slug or "document"


def _slugify(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    slug = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return slug


def _stable_json_list(values: list[object]) -> str:
    cleaned = []
    for value in values:
        text = _string_or_none(value)
        if text and text not in cleaned:
            cleaned.append(text)
    return json.dumps(cleaned, ensure_ascii=False)


def _json_list(value: object) -> list[Any]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return [stripped]
        if isinstance(parsed, list):
            return parsed
        return [parsed]
    return [value]


def _json_object(value: object) -> dict[str, Any]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return {}
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return {"raw": stripped}
        return parsed if isinstance(parsed, dict) else {"value": parsed}
    return {"value": value}


def _join_or_none(values: list[Any]) -> str:
    if not values:
        return "none"
    return "; ".join(str(value) for value in values)


def _float_or_none(value: object) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _string_or_none(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    return text or None


def _first_non_blank(*values: object) -> str | None:
    for value in values:
        text = _string_or_none(value)
        if text:
            return text
    return None


def _corpus_columns() -> list[str]:
    return [
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
    ]


def _is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
    except ValueError:
        return False
    return True


__all__ = [
    "RagCorpusArtifacts",
    "RagCorpusRun",
    "build_rag_corpus",
    "load_rag_corpus",
]
