"""Pandera schemas for normalized QSR workbook tables."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import pandas as pd
from pandera.errors import SchemaErrors
from pandera.pandas import Check, Column, DataFrameSchema

from qsr_audit.contracts.workbook import (
    AI_STRATEGY_SHEET,
    CORE_BRAND_METRICS_SHEET,
    DATA_NOTES_SHEET,
)
from qsr_audit.validate.models import ValidationFinding

SCHEMA_TYPE_CATEGORY = "schema_type"


@dataclass(frozen=True)
class SchemaBundle:
    """Container for the normalized table schemas."""

    schemas: Mapping[str, DataFrameSchema]

    def for_table(self, table_name: str) -> DataFrameSchema:
        try:
            return self.schemas[table_name]
        except KeyError as exc:  # pragma: no cover - guarded by call sites
            msg = f"Unknown validation table: {table_name}"
            raise KeyError(msg) from exc


def build_schema_bundle() -> SchemaBundle:
    """Build the Pandera schemas for the normalized workbook tables."""

    return SchemaBundle(
        schemas={
            "core_brand_metrics": DataFrameSchema(
                {
                    "rank": Column(int, nullable=False, coerce=True, checks=Check.in_range(1, 30)),
                    "brand_name": Column(
                        str, nullable=False, coerce=True, checks=Check.str_length(min_value=1)
                    ),
                    "category": Column(
                        str, nullable=False, coerce=True, checks=Check.str_length(min_value=1)
                    ),
                    "us_store_count_2024": Column(
                        int, nullable=False, coerce=True, checks=Check.ge(1)
                    ),
                    "systemwide_revenue_usd_billions_2024": Column(
                        float,
                        nullable=False,
                        coerce=True,
                        checks=Check.gt(0),
                    ),
                    "average_unit_volume_usd_thousands": Column(
                        float,
                        nullable=False,
                        coerce=True,
                        checks=Check.gt(0),
                    ),
                    "store_daily_equivalent_fte_range": Column(
                        str,
                        nullable=False,
                        coerce=True,
                        checks=Check.str_length(min_value=1),
                    ),
                    "store_margin_range_pct": Column(
                        str,
                        nullable=False,
                        coerce=True,
                        checks=Check.str_length(min_value=1),
                    ),
                    "central_kitchen_supply_chain_model": Column(
                        str,
                        nullable=False,
                        coerce=True,
                        checks=Check.str_length(min_value=1),
                    ),
                    "ownership_model": Column(
                        str,
                        nullable=False,
                        coerce=True,
                        checks=Check.str_length(min_value=1),
                    ),
                    "source_sheet": Column(
                        str,
                        nullable=False,
                        coerce=True,
                        checks=Check.eq(CORE_BRAND_METRICS_SHEET),
                    ),
                    "row_number": Column(int, nullable=False, coerce=True, checks=Check.ge(1)),
                    "fte_min": Column(float, nullable=False, coerce=True, checks=Check.ge(0)),
                    "fte_max": Column(float, nullable=False, coerce=True, checks=Check.ge(0)),
                    "fte_mid": Column(float, nullable=False, coerce=True, checks=Check.ge(0)),
                    "margin_min_pct": Column(
                        float, nullable=False, coerce=True, checks=Check.in_range(0, 100)
                    ),
                    "margin_max_pct": Column(
                        float, nullable=False, coerce=True, checks=Check.in_range(0, 100)
                    ),
                    "margin_mid_pct": Column(
                        float, nullable=False, coerce=True, checks=Check.in_range(0, 100)
                    ),
                },
                strict=True,
                coerce=True,
                ordered=True,
            ),
            "ai_strategy_registry": DataFrameSchema(
                {
                    "brand_name": Column(
                        str, nullable=False, coerce=True, checks=Check.str_length(min_value=1)
                    ),
                    "ai_strategy_direction": Column(
                        str,
                        nullable=False,
                        coerce=True,
                        checks=Check.str_length(min_value=1),
                    ),
                    "key_initiatives": Column(
                        str, nullable=False, coerce=True, checks=Check.str_length(min_value=1)
                    ),
                    "deployment_scale": Column(
                        str, nullable=False, coerce=True, checks=Check.str_length(min_value=1)
                    ),
                    "impact_metrics": Column(
                        str, nullable=False, coerce=True, checks=Check.str_length(min_value=1)
                    ),
                    "current_status_2026_q1": Column(
                        str,
                        nullable=False,
                        coerce=True,
                        checks=Check.str_length(min_value=1),
                    ),
                    "source_sheet": Column(
                        str,
                        nullable=False,
                        coerce=True,
                        checks=Check.eq(AI_STRATEGY_SHEET),
                    ),
                    "row_number": Column(int, nullable=False, coerce=True, checks=Check.ge(1)),
                },
                strict=True,
                coerce=True,
                ordered=True,
            ),
            "data_notes": DataFrameSchema(
                {
                    "field_name": Column(
                        str, nullable=False, coerce=True, checks=Check.str_length(min_value=1)
                    ),
                    "note_text": Column(
                        str, nullable=False, coerce=True, checks=Check.str_length(min_value=1)
                    ),
                    "source_sheet": Column(
                        str,
                        nullable=False,
                        coerce=True,
                        checks=Check.eq(DATA_NOTES_SHEET),
                    ),
                    "row_number": Column(int, nullable=False, coerce=True, checks=Check.ge(1)),
                },
                strict=True,
                coerce=True,
                ordered=True,
            ),
            "key_findings": DataFrameSchema(
                {
                    "finding_number": Column(int, nullable=False, coerce=True, checks=Check.ge(1)),
                    "finding_text": Column(
                        str, nullable=False, coerce=True, checks=Check.str_length(min_value=1)
                    ),
                    "source_sheet": Column(
                        str,
                        nullable=False,
                        coerce=True,
                        checks=Check.eq(DATA_NOTES_SHEET),
                    ),
                    "row_number": Column(int, nullable=False, coerce=True, checks=Check.ge(1)),
                },
                strict=True,
                coerce=True,
                ordered=True,
            ),
        }
    )


SCHEMAS = build_schema_bundle()


def validate_schema(
    table_name: str,
    frame: pd.DataFrame,
    bundle: SchemaBundle | None = None,
) -> list[ValidationFinding]:
    """Validate a normalized table against its schema."""

    schema_bundle = bundle or build_schema_bundle()
    schema = schema_bundle.for_table(table_name)
    sheet_name = _sheet_name_for_table(table_name)

    try:
        schema.validate(frame, lazy=True)
    except SchemaErrors as exc:
        return _schema_error_findings(table_name, sheet_name, frame, exc)

    return [
        ValidationFinding(
            severity="info",
            category=SCHEMA_TYPE_CATEGORY,
            check_name=f"{table_name}.schema",
            dataset=table_name,
            message=f"{table_name} passed schema validation for {len(frame)} rows.",
            sheet_name=sheet_name,
            details={"row_count": len(frame)},
        )
    ]


def _schema_error_findings(
    table_name: str,
    sheet_name: str,
    frame: pd.DataFrame,
    exc: SchemaErrors,
) -> list[ValidationFinding]:
    failure_cases = getattr(exc, "failure_cases", None)
    rows: list[dict[str, object]] = []
    if isinstance(failure_cases, pd.DataFrame) and not failure_cases.empty:
        rows = failure_cases.head(10).to_dict(orient="records")

    findings = [
        ValidationFinding(
            severity="error",
            category=SCHEMA_TYPE_CATEGORY,
            check_name=f"{table_name}.schema",
            dataset=table_name,
            message=f"{table_name} failed schema validation.",
            sheet_name=sheet_name,
            details={
                "error_count": len(failure_cases)
                if isinstance(failure_cases, pd.DataFrame)
                else None,
                "examples": rows,
                "schema_error": str(exc),
            },
        )
    ]

    for row in rows[:5]:
        column_name = str(row.get("column")) if pd.notna(row.get("column")) else None
        index_value = row.get("index")
        record = _frame_record_at_index(frame, index_value)
        findings.append(
            ValidationFinding(
                severity="error",
                category=SCHEMA_TYPE_CATEGORY,
                check_name=f"{table_name}.schema.row",
                dataset=table_name,
                message=(
                    f"{table_name} row {index_value} failed schema validation"
                    + (f" on column `{column_name}`." if column_name else ".")
                ),
                sheet_name=sheet_name,
                field_name=column_name,
                brand_name=_record_brand_name(record),
                row_number=_record_row_number(record),
                observed=str(row.get("failure_case")),
                details=row,
            )
        )

    return findings


def _record_brand_name(record: pd.Series | None) -> str | None:
    if record is None or "brand_name" not in record:
        return None
    value = record["brand_name"]
    return None if pd.isna(value) else str(value)


def _record_row_number(record: pd.Series | None) -> int | None:
    if record is None or "row_number" not in record:
        return None
    value = record["row_number"]
    if pd.isna(value):
        return None
    return int(value)


def _frame_record_at_index(frame: pd.DataFrame, index_value: object) -> pd.Series | None:
    if index_value is None or pd.isna(index_value):
        return None
    try:
        index_int = int(index_value)
    except (TypeError, ValueError):
        return None
    if index_int < 0 or index_int >= len(frame):
        return None
    return frame.iloc[index_int]


def _sheet_name_for_table(table_name: str) -> str:
    return {
        "core_brand_metrics": CORE_BRAND_METRICS_SHEET,
        "ai_strategy_registry": AI_STRATEGY_SHEET,
        "data_notes": DATA_NOTES_SHEET,
        "key_findings": DATA_NOTES_SHEET,
    }[table_name]


__all__ = [
    "SCHEMA_TYPE_CATEGORY",
    "SCHEMAS",
    "SchemaBundle",
    "build_schema_bundle",
    "validate_schema",
]
