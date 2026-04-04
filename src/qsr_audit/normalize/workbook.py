"""Silver-layer normalization for the QSR workbook ingestion flow."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from qsr_audit.contracts.workbook import (
    AI_STRATEGY_REGISTRY_COLUMN_MAP,
    AI_STRATEGY_SHEET,
    CORE_BRAND_METRICS_COLUMN_MAP,
    CORE_BRAND_METRICS_SHEET,
    DATA_NOTES_COLUMN_MAP,
    DATA_NOTES_SHEET,
    KEY_FINDINGS_SECTION_MARKER,
    SILVER_OUTPUT_FILES,
)
from qsr_audit.ingest.parsers import canonicalize_brand_name, parse_fte_range, parse_margin_range


@dataclass(frozen=True)
class SilverArtifacts:
    core_brand_metrics_path: Path
    ai_strategy_registry_path: Path
    data_notes_path: Path
    key_findings_path: Path


def normalize_and_write_silver(
    raw_sheets: dict[str, pd.DataFrame],
    output_dir: Path,
) -> SilverArtifacts:
    """Normalize loaded workbook sheets into Silver parquet datasets."""

    output_dir.mkdir(parents=True, exist_ok=True)

    core_brand_metrics = normalize_core_brand_metrics(raw_sheets[CORE_BRAND_METRICS_SHEET])
    ai_strategy_registry = normalize_ai_strategy_registry(raw_sheets[AI_STRATEGY_SHEET])
    data_notes, key_findings = normalize_data_notes_and_key_findings(raw_sheets[DATA_NOTES_SHEET])

    core_brand_metrics_path = output_dir / SILVER_OUTPUT_FILES["core_brand_metrics"]
    ai_strategy_registry_path = output_dir / SILVER_OUTPUT_FILES["ai_strategy_registry"]
    data_notes_path = output_dir / SILVER_OUTPUT_FILES["data_notes"]
    key_findings_path = output_dir / SILVER_OUTPUT_FILES["key_findings"]

    core_brand_metrics.to_parquet(core_brand_metrics_path, index=False)
    ai_strategy_registry.to_parquet(ai_strategy_registry_path, index=False)
    data_notes.to_parquet(data_notes_path, index=False)
    key_findings.to_parquet(key_findings_path, index=False)

    return SilverArtifacts(
        core_brand_metrics_path=core_brand_metrics_path,
        ai_strategy_registry_path=ai_strategy_registry_path,
        data_notes_path=data_notes_path,
        key_findings_path=key_findings_path,
    )


def normalize_core_brand_metrics(raw_frame: pd.DataFrame) -> pd.DataFrame:
    normalized = _rename_with_lineage(
        raw_frame,
        CORE_BRAND_METRICS_COLUMN_MAP,
        source_sheet=CORE_BRAND_METRICS_SHEET,
    )
    normalized["brand_name"] = normalized["brand_name"].map(canonicalize_brand_name)
    normalized["rank"] = pd.to_numeric(normalized["rank"], errors="coerce").astype("Int64")
    normalized["us_store_count_2024"] = pd.to_numeric(
        normalized["us_store_count_2024"],
        errors="coerce",
    ).astype("Int64")
    normalized["systemwide_revenue_usd_billions_2024"] = pd.to_numeric(
        normalized["systemwide_revenue_usd_billions_2024"],
        errors="coerce",
    )
    normalized["average_unit_volume_usd_thousands"] = pd.to_numeric(
        normalized["average_unit_volume_usd_thousands"],
        errors="coerce",
    )

    fte_ranges = normalized["store_daily_equivalent_fte_range"].map(parse_fte_range)
    normalized[["fte_min", "fte_max", "fte_mid"]] = pd.DataFrame(
        list(fte_ranges),
        index=normalized.index,
    )

    margin_ranges = normalized["store_margin_range_pct"].map(parse_margin_range)
    normalized[["margin_min_pct", "margin_max_pct", "margin_mid_pct"]] = pd.DataFrame(
        list(margin_ranges),
        index=normalized.index,
    )

    return normalized


def normalize_ai_strategy_registry(raw_frame: pd.DataFrame) -> pd.DataFrame:
    normalized = _rename_with_lineage(
        raw_frame,
        AI_STRATEGY_REGISTRY_COLUMN_MAP,
        source_sheet=AI_STRATEGY_SHEET,
    )
    normalized["brand_name"] = normalized["brand_name"].map(canonicalize_brand_name)
    return normalized


def normalize_data_notes_and_key_findings(
    raw_frame: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    normalized = _rename_with_lineage(
        raw_frame,
        DATA_NOTES_COLUMN_MAP,
        source_sheet=DATA_NOTES_SHEET,
    )

    non_blank_mask = normalized[["field_name", "note_text"]].notna().any(axis=1)
    marker_indexes = normalized.index[
        normalized["field_name"] == KEY_FINDINGS_SECTION_MARKER
    ].tolist()
    marker_index = marker_indexes[0] if marker_indexes else None

    key_findings_mask = pd.Series(False, index=normalized.index)
    if marker_index is not None:
        following_blank = None
        for index in normalized.index[normalized.index > marker_index]:
            if not non_blank_mask.loc[index]:
                following_blank = index
                break

        upper_bound = following_blank if following_blank is not None else len(normalized)
        key_findings_mask = (
            (normalized.index > marker_index) & (normalized.index < upper_bound) & non_blank_mask
        )

    data_notes_mask = non_blank_mask & ~key_findings_mask
    if marker_index is not None:
        data_notes_mask &= normalized.index != marker_index

    data_notes = normalized.loc[
        data_notes_mask,
        ["field_name", "note_text", "source_sheet", "row_number"],
    ]
    key_findings = normalized.loc[
        key_findings_mask,
        ["field_name", "note_text", "source_sheet", "row_number"],
    ].rename(
        columns={
            "field_name": "finding_number",
            "note_text": "finding_text",
        }
    )
    key_findings["finding_number"] = pd.to_numeric(
        key_findings["finding_number"],
        errors="coerce",
    ).astype("Int64")

    return (data_notes.reset_index(drop=True), key_findings.reset_index(drop=True))


def _rename_with_lineage(
    raw_frame: pd.DataFrame,
    column_map: dict[str, str],
    *,
    source_sheet: str,
) -> pd.DataFrame:
    renamed = raw_frame.rename(columns=column_map).copy()
    renamed["source_sheet"] = source_sheet
    renamed["row_number"] = raw_frame.index + 2
    return renamed
