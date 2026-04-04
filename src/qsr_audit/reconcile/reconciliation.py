"""Reference comparison helpers for Gold-layer reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

GRADE_ORDER = {"A": 0, "B": 1, "C": 2, "D": 3, "F": 4, "MISSING": 5}


@dataclass(frozen=True)
class FieldComparison:
    """Comparison result between workbook and reference for one field."""

    field_name: str
    workbook_value: float | int | None
    reference_value: float | int | None
    absolute_error: float | int | None
    relative_error: float | None
    credibility_grade: str
    warning: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "field_name": self.field_name,
            "workbook_value": self.workbook_value,
            "reference_value": self.reference_value,
            "absolute_error": self.absolute_error,
            "relative_error": self.relative_error,
            "credibility_grade": self.credibility_grade,
            "warning": self.warning,
        }


def compare_numeric_field(
    *,
    field_name: str,
    workbook_value: object,
    reference_value: object,
) -> FieldComparison:
    """Compare numeric workbook and reference values."""

    workbook_number = _to_number(workbook_value)
    reference_number = _to_number(reference_value)
    if workbook_number is None or reference_number is None:
        return FieldComparison(
            field_name=field_name,
            workbook_value=workbook_number,
            reference_value=reference_number,
            absolute_error=None,
            relative_error=None,
            credibility_grade="MISSING",
            warning=f"No comparable reference value was available for `{field_name}`.",
        )

    absolute_error = abs(workbook_number - reference_number)
    relative_error = None if reference_number == 0 else absolute_error / abs(reference_number)
    return FieldComparison(
        field_name=field_name,
        workbook_value=workbook_number,
        reference_value=reference_number,
        absolute_error=absolute_error,
        relative_error=relative_error,
        credibility_grade=grade_numeric_credibility(relative_error=relative_error),
    )


def compare_rank_field(
    *,
    workbook_value: object,
    reference_value: object,
) -> FieldComparison:
    """Compare workbook and reference rank values."""

    workbook_rank = _to_int(workbook_value)
    reference_rank = _to_int(reference_value)
    if workbook_rank is None or reference_rank is None:
        return FieldComparison(
            field_name="rank",
            workbook_value=workbook_rank,
            reference_value=reference_rank,
            absolute_error=None,
            relative_error=None,
            credibility_grade="MISSING",
            warning="No comparable reference rank was available.",
        )

    absolute_error = abs(workbook_rank - reference_rank)
    if absolute_error == 0:
        grade = "A"
    elif absolute_error == 1:
        grade = "B"
    elif absolute_error <= 3:
        grade = "C"
    elif absolute_error <= 5:
        grade = "D"
    else:
        grade = "F"
    relative_error = absolute_error / reference_rank if reference_rank else None
    return FieldComparison(
        field_name="rank",
        workbook_value=workbook_rank,
        reference_value=reference_rank,
        absolute_error=absolute_error,
        relative_error=relative_error,
        credibility_grade=grade,
    )


def grade_numeric_credibility(*, relative_error: float | None) -> str:
    """Assign a credibility grade from relative error."""

    if relative_error is None:
        return "MISSING"
    if relative_error <= 0.02:
        return "A"
    if relative_error <= 0.05:
        return "B"
    if relative_error <= 0.10:
        return "C"
    if relative_error <= 0.20:
        return "D"
    return "F"


def select_best_reference_row(reference_rows: pd.DataFrame, *, field_name: str) -> pd.Series | None:
    """Pick the most credible reference row for a given field."""

    if reference_rows.empty:
        return None
    available = reference_rows[reference_rows[field_name].notna()].copy()
    if available.empty:
        return None

    available["sort_confidence"] = pd.to_numeric(
        available["confidence_score"], errors="coerce"
    ).fillna(0.0)
    available["sort_date"] = pd.to_datetime(available["as_of_date"], errors="coerce")
    available = available.sort_values(
        by=["sort_confidence", "sort_date", "source_name"],
        ascending=[False, False, True],
        na_position="last",
    )
    return available.iloc[0]


def overall_reconciliation_grade(grades: list[str]) -> str:
    """Return the worst non-missing grade across compared fields."""

    effective = [grade for grade in grades if grade != "MISSING"]
    if not effective:
        return "MISSING"
    return max(effective, key=lambda grade: GRADE_ORDER.get(grade, 99))


def _to_number(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: object) -> int | None:
    number = _to_number(value)
    if number is None:
        return None
    return int(round(number))


__all__ = [
    "FieldComparison",
    "compare_numeric_field",
    "compare_rank_field",
    "grade_numeric_credibility",
    "overall_reconciliation_grade",
    "select_best_reference_row",
]
