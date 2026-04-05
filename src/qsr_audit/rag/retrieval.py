"""Local retrieval backends for the RAG experiment scaffold."""

from __future__ import annotations

import json
import math
import os
import re
import time
from dataclasses import dataclass
from typing import Any

import pandas as pd

from qsr_audit.models.stubs import get_embedding_candidates

TOKEN_PATTERN = re.compile(r"[A-Za-z0-9']+")
_DENSE_RETRIEVER_MODELS = {
    "dense-minilm": "sentence-transformers/all-MiniLM-L6-v2",
    "dense-bge-small": "BAAI/bge-small-en-v1.5",
    "dense-e5-small": "intfloat/e5-small-v2",
}


@dataclass(frozen=True)
class RagSearchRun:
    """Result of a retrieval-only search over a local corpus."""

    retriever_name: str
    results: pd.DataFrame
    status: str
    reason: str | None
    latency_ms: float
    index_size_bytes: int


def prepare_retriever(
    *,
    corpus: pd.DataFrame,
    retriever_name: str,
    allow_model_download: bool = False,
) -> _BM25Retriever | _DenseRetriever | _SkippedRetriever:
    """Prepare a retriever once for repeated query evaluation."""

    return _resolve_retriever(
        retriever_name=retriever_name,
        corpus=corpus,
        allow_model_download=allow_model_download,
    )


def rag_search(
    *,
    corpus: pd.DataFrame,
    query: str,
    top_k: int = 5,
    retriever_name: str = "bm25",
    metadata_filters: dict[str, Any] | None = None,
    allow_model_download: bool = False,
    prepared_retriever: _BM25Retriever | _DenseRetriever | _SkippedRetriever | None = None,
) -> RagSearchRun:
    """Search a corpus and return ranked chunks plus metadata only."""

    started_at = time.perf_counter()
    retriever = prepared_retriever or prepare_retriever(
        corpus=corpus,
        retriever_name=retriever_name,
        allow_model_download=allow_model_download,
    )
    if isinstance(retriever, _SkippedRetriever):
        return RagSearchRun(
            retriever_name=retriever_name,
            results=pd.DataFrame(columns=_result_columns()),
            status="skipped",
            reason=retriever.reason,
            latency_ms=round((time.perf_counter() - started_at) * 1000, 3),
            index_size_bytes=0,
        )

    results = retriever.search(
        query=query,
        top_k=top_k,
        metadata_filters=metadata_filters or {},
    )
    latency_ms = round((time.perf_counter() - started_at) * 1000, 3)
    return RagSearchRun(
        retriever_name=retriever_name,
        results=results,
        status="ok",
        reason=None,
        latency_ms=latency_ms,
        index_size_bytes=retriever.index_size_bytes,
    )


def available_retriever_names() -> tuple[str, ...]:
    """Return supported retriever slugs."""

    return ("bm25", *tuple(_DENSE_RETRIEVER_MODELS.keys()))


class _SkippedRetriever:
    def __init__(self, reason: str) -> None:
        self.reason = reason


class _BM25Retriever:
    def __init__(self, corpus: pd.DataFrame) -> None:
        self.corpus = corpus.reset_index(drop=True).copy()
        self.doc_tokens = [_tokenize(text) for text in self.corpus["text"].fillna("")]
        self.avg_doc_length = (
            sum(len(tokens) for tokens in self.doc_tokens) / len(self.doc_tokens)
            if self.doc_tokens
            else 0.0
        )
        self.doc_frequencies = self._build_doc_frequencies(self.doc_tokens)
        self.index_size_bytes = sum(len(token) for tokens in self.doc_tokens for token in tokens)

    def search(
        self,
        *,
        query: str,
        top_k: int,
        metadata_filters: dict[str, Any],
    ) -> pd.DataFrame:
        filtered = _apply_metadata_filters(self.corpus, metadata_filters)
        if filtered.empty:
            return pd.DataFrame(columns=_result_columns())
        query_tokens = _tokenize(query)
        candidate_rows: list[dict[str, Any]] = []
        for _, row in filtered.iterrows():
            corpus_index = int(row.name)
            score = self._score(query_tokens, self.doc_tokens[corpus_index])
            candidate_rows.append(
                _result_row(
                    row=row,
                    score=score,
                    filter_match=_row_matches_filters(row.to_dict(), metadata_filters),
                )
            )
        return _rank_results(candidate_rows, top_k=top_k, retriever_name="bm25")

    def _score(self, query_tokens: list[str], document_tokens: list[str]) -> float:
        if not query_tokens or not document_tokens:
            return 0.0
        token_counts: dict[str, int] = {}
        for token in document_tokens:
            token_counts[token] = token_counts.get(token, 0) + 1

        score = 0.0
        k1 = 1.5
        b = 0.75
        document_length = len(document_tokens)
        for token in query_tokens:
            if token not in token_counts:
                continue
            df = self.doc_frequencies.get(token, 0)
            if df == 0:
                continue
            idf = math.log(1 + (len(self.doc_tokens) - df + 0.5) / (df + 0.5))
            tf = token_counts[token]
            denominator = tf + k1 * (
                1 - b + b * (document_length / self.avg_doc_length if self.avg_doc_length else 0)
            )
            score += idf * ((tf * (k1 + 1)) / denominator)
        return score

    @staticmethod
    def _build_doc_frequencies(doc_tokens: list[list[str]]) -> dict[str, int]:
        frequencies: dict[str, int] = {}
        for tokens in doc_tokens:
            for token in set(tokens):
                frequencies[token] = frequencies.get(token, 0) + 1
        return frequencies


class _DenseRetriever:
    def __init__(
        self,
        *,
        corpus: pd.DataFrame,
        retriever_name: str,
        model_name: str,
        allow_model_download: bool,
    ) -> None:
        from sentence_transformers import SentenceTransformer

        self.corpus = corpus.reset_index(drop=True).copy()
        self.retriever_name = retriever_name
        self.model_name = model_name
        self.query_prefix = _query_prefix_for_model(model_name)
        self.document_prefix = _document_prefix_for_model(model_name)
        self.model = SentenceTransformer(
            model_name,
            device="cpu",
            trust_remote_code=False,
            local_files_only=not allow_model_download,
        )
        self.document_embeddings = self.model.encode(
            [self.document_prefix + text for text in self.corpus["text"].fillna("")],
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        self.index_size_bytes = int(getattr(self.document_embeddings, "nbytes", 0))

    def search(
        self,
        *,
        query: str,
        top_k: int,
        metadata_filters: dict[str, Any],
    ) -> pd.DataFrame:
        filtered = _apply_metadata_filters(self.corpus, metadata_filters)
        if filtered.empty:
            return pd.DataFrame(columns=_result_columns())
        filtered_positions = filtered.index.tolist()
        query_embedding = self.model.encode(
            self.query_prefix + query,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        similarities = (self.document_embeddings[filtered_positions] @ query_embedding).tolist()
        candidate_rows: list[dict[str, Any]] = []
        for similarity, (_, row) in zip(similarities, filtered.iterrows(), strict=False):
            candidate_rows.append(
                _result_row(
                    row=row,
                    score=float(similarity),
                    filter_match=_row_matches_filters(row.to_dict(), metadata_filters),
                )
            )
        return _rank_results(candidate_rows, top_k=top_k, retriever_name=self.retriever_name)


def _resolve_retriever(
    *,
    retriever_name: str,
    corpus: pd.DataFrame,
    allow_model_download: bool,
) -> _BM25Retriever | _DenseRetriever | _SkippedRetriever:
    if retriever_name == "bm25":
        return _BM25Retriever(corpus)

    if retriever_name not in _DENSE_RETRIEVER_MODELS:
        return _SkippedRetriever(f"Unsupported retriever `{retriever_name}`.")

    if os.getenv("CI", "").lower() == "true":
        return _SkippedRetriever(
            f"`{retriever_name}` is disabled in CI to avoid model downloads during automated checks."
        )

    candidate_ids = {candidate.repo_id for candidate in get_embedding_candidates()}
    model_name = _DENSE_RETRIEVER_MODELS[retriever_name]
    if model_name not in candidate_ids:
        return _SkippedRetriever(f"`{model_name}` is not registered as a retrieval candidate.")

    try:
        return _DenseRetriever(
            corpus=corpus,
            retriever_name=retriever_name,
            model_name=model_name,
            allow_model_download=allow_model_download,
        )
    except ImportError:
        return _SkippedRetriever(
            "Dense retrieval requires the optional `sentence-transformers` dependency."
        )
    except Exception as exc:  # pragma: no cover - exact local cache errors vary by environment.
        return _SkippedRetriever(f"`{retriever_name}` could not be initialized locally: {exc}")


def _apply_metadata_filters(frame: pd.DataFrame, metadata_filters: dict[str, Any]) -> pd.DataFrame:
    if not metadata_filters:
        return frame.copy()
    mask = [_row_matches_filters(row, metadata_filters) for row in frame.to_dict(orient="records")]
    return frame.loc[mask].copy()


def _row_matches_filters(row: dict[str, Any], metadata_filters: dict[str, Any]) -> bool:
    for key, expected in metadata_filters.items():
        actual = row.get(key)
        if actual is None:
            actual = _metadata_json(row).get(key)
        if key in {"brand_names", "metric_names"}:
            if not _list_matches(_json_list(actual), expected):
                return False
            continue
        if isinstance(actual, str) and _looks_like_json(actual):
            parsed = _json_list(actual)
            if parsed and _list_matches(parsed, expected):
                continue
        if isinstance(expected, list | tuple | set):
            if str(actual) not in {str(value) for value in expected}:
                return False
        elif str(actual) != str(expected):
            return False
    return True


def _rank_results(
    candidate_rows: list[dict[str, Any]], *, top_k: int, retriever_name: str
) -> pd.DataFrame:
    ranked = sorted(
        candidate_rows,
        key=lambda row: (-float(row["score"]), str(row["chunk_id"])),
    )[:top_k]
    for rank, row in enumerate(ranked, start=1):
        row["rank"] = rank
        row["retriever_name"] = retriever_name
    return pd.DataFrame(ranked, columns=_result_columns())


def _result_row(*, row: Any, score: float, filter_match: bool) -> dict[str, Any]:
    row_dict = row.to_dict() if hasattr(row, "to_dict") else dict(row)
    return {
        "retriever_name": None,
        "rank": None,
        "score": round(float(score), 6),
        "doc_id": row_dict["doc_id"],
        "chunk_id": row_dict["chunk_id"],
        "source_kind": row_dict["source_kind"],
        "title": row_dict["title"],
        "text": row_dict["text"],
        "artifact_path": row_dict["artifact_path"],
        "brand_names": row_dict["brand_names"],
        "metric_names": row_dict["metric_names"],
        "as_of_date": row_dict["as_of_date"],
        "publish_status": row_dict["publish_status"],
        "confidence_score": row_dict["confidence_score"],
        "source_name": row_dict["source_name"],
        "source_url_or_doc_id": row_dict["source_url_or_doc_id"],
        "metadata_json": row_dict["metadata_json"],
        "filter_match": filter_match,
        "citation_present": bool(
            _string_or_none(row_dict.get("source_name"))
            or _string_or_none(row_dict.get("source_url_or_doc_id"))
        ),
    }


def _result_columns() -> list[str]:
    return [
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
    ]


def _tokenize(text: str) -> list[str]:
    return TOKEN_PATTERN.findall(text.lower())


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


def _list_matches(actual: list[str], expected: Any) -> bool:
    if isinstance(expected, list | tuple | set):
        expected_values = {str(value) for value in expected}
        return bool(expected_values.intersection({str(value) for value in actual}))
    return str(expected) in {str(value) for value in actual}


def _looks_like_json(value: str) -> bool:
    stripped = value.strip()
    return stripped.startswith("[") or stripped.startswith("{")


def _query_prefix_for_model(model_name: str) -> str:
    if model_name == "intfloat/e5-small-v2":
        return "query: "
    if model_name == "BAAI/bge-small-en-v1.5":
        return "Represent this sentence for searching relevant passages: "
    return ""


def _document_prefix_for_model(model_name: str) -> str:
    if model_name == "intfloat/e5-small-v2":
        return "passage: "
    return ""


def _string_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


__all__ = [
    "RagSearchRun",
    "available_retriever_names",
    "prepare_retriever",
    "rag_search",
]
