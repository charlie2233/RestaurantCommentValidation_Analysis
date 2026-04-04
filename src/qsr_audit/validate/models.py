"""Shared validation result models."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

ValidationSeverity = Literal["error", "warning", "info"]
ValidationCategory = Literal[
    "schema_type",
    "null",
    "uniqueness",
    "allowed_range",
    "cross_sheet",
    "arithmetic",
]


@dataclass(frozen=True)
class ValidationFinding:
    """A single validation finding with report-friendly metadata."""

    severity: ValidationSeverity
    category: ValidationCategory
    check_name: str
    dataset: str
    message: str
    sheet_name: str | None = None
    field_name: str | None = None
    brand_name: str | None = None
    row_number: int | None = None
    expected: str | None = None
    observed: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def as_record(self) -> dict[str, Any]:
        return {
            "severity": self.severity,
            "category": self.category,
            "check_name": self.check_name,
            "dataset": self.dataset,
            "message": self.message,
            "sheet_name": self.sheet_name,
            "field_name": self.field_name,
            "brand_name": self.brand_name,
            "row_number": self.row_number,
            "expected": self.expected,
            "observed": self.observed,
            "details": _json_safe(self.details),
        }


@dataclass(frozen=True)
class ValidationArtifacts:
    """Paths produced by the validation workflow."""

    summary_markdown: Path
    results_json: Path
    flags_parquet: Path


@dataclass(frozen=True)
class ValidationRun:
    """Complete validation run result."""

    source_path: Path
    source_kind: str
    findings: tuple[ValidationFinding, ...]
    artifacts: ValidationArtifacts | None = None

    @property
    def passed(self) -> bool:
        return not any(finding.severity == "error" for finding in self.findings)

    @property
    def counts(self) -> dict[str, int]:
        counts = {"error": 0, "warning": 0, "info": 0}
        for finding in self.findings:
            counts[finding.severity] += 1
        return counts


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item") and callable(value.item):
        try:
            return value.item()
        except (TypeError, ValueError):
            return str(value)
    return value
