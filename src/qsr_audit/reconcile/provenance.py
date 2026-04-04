"""Provenance model for manual and workbook-derived sources.

Gold-layer outputs should carry provenance alongside the reconciled values so
downstream users can see where each claim came from, how it was derived, and how
much confidence the pipeline assigns to it.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class ProvenanceRecord:
    """Typed provenance metadata for a single source claim."""

    source_type: str
    source_name: str
    source_url_or_doc_id: str | None
    as_of_date: date | None
    method_reported_or_estimated: str
    confidence_score: float | None
    notes: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_type", _clean_text(self.source_type))
        object.__setattr__(self, "source_name", _clean_text(self.source_name))
        object.__setattr__(
            self, "source_url_or_doc_id", _clean_optional_text(self.source_url_or_doc_id)
        )
        object.__setattr__(
            self, "method_reported_or_estimated", _clean_text(self.method_reported_or_estimated)
        )
        object.__setattr__(self, "notes", _clean_optional_text(self.notes))
        object.__setattr__(
            self, "confidence_score", _normalize_confidence_score(self.confidence_score)
        )
        object.__setattr__(self, "as_of_date", _normalize_date(self.as_of_date))
        object.__setattr__(self, "extra", _json_safe_mapping(self.extra))

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dictionary representation."""

        payload = {
            "source_type": self.source_type,
            "source_name": self.source_name,
            "source_url_or_doc_id": self.source_url_or_doc_id,
            "as_of_date": self.as_of_date.isoformat() if self.as_of_date else None,
            "method_reported_or_estimated": self.method_reported_or_estimated,
            "confidence_score": self.confidence_score,
            "notes": self.notes,
            "extra": _json_safe(self.extra),
        }
        return payload

    def with_extra(self, **extra: Any) -> ProvenanceRecord:
        """Return a copy with additional metadata merged into ``extra``."""

        merged = dict(self.extra)
        merged.update(extra)
        return replace(self, extra=merged)

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> ProvenanceRecord:
        """Build a record from a mapping, accepting a few common aliases."""

        payload = dict(data)
        return cls(
            source_type=str(payload.get("source_type") or payload.get("type") or "unknown"),
            source_name=str(payload.get("source_name") or payload.get("name") or "unknown"),
            source_url_or_doc_id=_maybe_text(
                payload.get("source_url_or_doc_id")
                or payload.get("source_url")
                or payload.get("doc_id")
                or payload.get("document_id")
            ),
            as_of_date=_normalize_date(
                payload.get("as_of_date")
                or payload.get("date")
                or payload.get("as_of")
                or payload.get("effective_date")
            ),
            method_reported_or_estimated=str(
                payload.get("method_reported_or_estimated")
                or payload.get("method")
                or payload.get("basis")
                or "reported"
            ),
            confidence_score=payload.get("confidence_score"),
            notes=_maybe_text(payload.get("notes")),
            extra={
                key: value
                for key, value in payload.items()
                if key
                not in {
                    "source_type",
                    "type",
                    "source_name",
                    "name",
                    "source_url_or_doc_id",
                    "source_url",
                    "doc_id",
                    "document_id",
                    "as_of_date",
                    "date",
                    "as_of",
                    "effective_date",
                    "method_reported_or_estimated",
                    "method",
                    "basis",
                    "confidence_score",
                    "notes",
                }
            },
        )


@dataclass(frozen=True)
class ProvenanceRegistry:
    """Container for provenance rows written to Gold-layer artifacts."""

    records: tuple[ProvenanceRecord, ...] = field(default_factory=tuple)

    def to_frame(self) -> pd.DataFrame:
        """Convert the registry to a DataFrame suitable for parquet output."""

        columns = [
            "source_type",
            "source_name",
            "source_url_or_doc_id",
            "as_of_date",
            "method_reported_or_estimated",
            "confidence_score",
            "notes",
            "extra",
        ]
        frame = pd.DataFrame([record.to_dict() for record in self.records], columns=columns)
        if frame.empty:
            return pd.DataFrame(columns=columns)
        return frame

    def to_records(self) -> list[dict[str, Any]]:
        """Return a list of JSON-safe dictionaries."""

        return [record.to_dict() for record in self.records]

    def add(self, record: ProvenanceRecord | Mapping[str, Any]) -> ProvenanceRegistry:
        """Return a new registry with one more provenance record."""

        normalized = (
            record
            if isinstance(record, ProvenanceRecord)
            else ProvenanceRecord.from_mapping(record)
        )
        return ProvenanceRegistry(records=self.records + (normalized,))

    def extend(self, records: Iterable[ProvenanceRecord | Mapping[str, Any]]) -> ProvenanceRegistry:
        """Return a new registry with several additional provenance records."""

        normalized = tuple(
            record
            if isinstance(record, ProvenanceRecord)
            else ProvenanceRecord.from_mapping(record)
            for record in records
        )
        return ProvenanceRegistry(records=self.records + normalized)

    @classmethod
    def from_records(
        cls, records: Sequence[ProvenanceRecord | Mapping[str, Any]]
    ) -> ProvenanceRegistry:
        """Build a registry from typed records or mappings."""

        return cls(
            records=tuple(
                record
                if isinstance(record, ProvenanceRecord)
                else ProvenanceRecord.from_mapping(record)
                for record in records
            )
        )


def build_provenance_record(
    *,
    source_type: str,
    source_name: str,
    source_url_or_doc_id: str | None = None,
    as_of_date: date | datetime | str | None = None,
    method_reported_or_estimated: str,
    confidence_score: float | int | str | None = None,
    notes: str | None = None,
    **extra: Any,
) -> ProvenanceRecord:
    """Convenience constructor for provenance records."""

    return ProvenanceRecord(
        source_type=source_type,
        source_name=source_name,
        source_url_or_doc_id=source_url_or_doc_id,
        as_of_date=_normalize_date(as_of_date),
        method_reported_or_estimated=method_reported_or_estimated,
        confidence_score=confidence_score,
        notes=notes,
        extra=extra,
    )


def provenance_frame(records: Iterable[ProvenanceRecord | Mapping[str, Any]]) -> pd.DataFrame:
    """Convert provenance rows to a DataFrame."""

    return ProvenanceRegistry.from_records(tuple(records)).to_frame()


def provenance_records(
    records: Iterable[ProvenanceRecord | Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Convert provenance rows to JSON-safe dictionaries."""

    return ProvenanceRegistry.from_records(tuple(records)).to_records()


def _normalize_date(value: date | datetime | str | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value).strip()
    if not text:
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _normalize_confidence_score(value: float | int | str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    if not pd.notna(score):
        return None
    return max(0.0, min(1.0, score))


def _clean_text(value: Any) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError("Provenance text fields must not be blank.")
    return text


def _clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _maybe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _json_safe_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): _json_safe(item) for key, item in value.items()}


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _json_safe_mapping(value)
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, date | datetime):
        return value.isoformat()
    if hasattr(value, "item") and callable(value.item):
        try:
            return value.item()
        except (TypeError, ValueError):
            return str(value)
    return value


__all__ = [
    "ProvenanceRecord",
    "ProvenanceRegistry",
    "build_provenance_record",
    "provenance_frame",
    "provenance_records",
]
