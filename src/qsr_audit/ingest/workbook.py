"""Workbook ingestion flow for Bronze and Silver outputs."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from qsr_audit.config import Settings
from qsr_audit.contracts.workbook import BRONZE_RAW_SHEET_STEMS, REQUIRED_WORKBOOK_SHEETS
from qsr_audit.normalize.workbook import SilverArtifacts, normalize_and_write_silver


@dataclass(frozen=True)
class BronzeSheetArtifacts:
    parquet_path: Path
    csv_path: Path


@dataclass(frozen=True)
class IngestWorkbookArtifacts:
    workbook_copy_path: Path
    bronze_sheet_artifacts: dict[str, BronzeSheetArtifacts]
    silver_artifacts: SilverArtifacts


def ingest_workbook(input_path: Path, settings: Settings) -> IngestWorkbookArtifacts:
    """Run the first workbook ingestion slice across Bronze and Silver."""

    workbook_path = input_path.expanduser().resolve()
    if not workbook_path.exists():
        raise FileNotFoundError(f"Workbook not found: {workbook_path}")

    raw_sheets = load_workbook_sheets(workbook_path)
    workbook_copy_path = preserve_raw_workbook_copy(workbook_path, settings.data_bronze)
    bronze_sheet_artifacts = write_bronze_sheet_dumps(raw_sheets, settings.data_bronze)
    silver_artifacts = normalize_and_write_silver(raw_sheets, settings.data_silver)

    return IngestWorkbookArtifacts(
        workbook_copy_path=workbook_copy_path,
        bronze_sheet_artifacts=bronze_sheet_artifacts,
        silver_artifacts=silver_artifacts,
    )


def load_workbook_sheets(workbook_path: Path) -> dict[str, pd.DataFrame]:
    """Load the expected workbook sheets as raw pandas frames."""

    excel_file = pd.ExcelFile(workbook_path, engine="openpyxl")
    missing_sheets = [
        sheet for sheet in REQUIRED_WORKBOOK_SHEETS if sheet not in excel_file.sheet_names
    ]
    if missing_sheets:
        joined = ", ".join(missing_sheets)
        raise ValueError(f"Workbook is missing required sheets: {joined}")

    return {
        sheet_name: excel_file.parse(sheet_name=sheet_name)
        for sheet_name in REQUIRED_WORKBOOK_SHEETS
    }


def preserve_raw_workbook_copy(workbook_path: Path, bronze_dir: Path) -> Path:
    """Copy the input workbook into Bronze for exact preservation."""

    target_dir = bronze_dir / "workbooks"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / workbook_path.name
    shutil.copy2(workbook_path, target_path)
    return target_path


def write_bronze_sheet_dumps(
    raw_sheets: dict[str, pd.DataFrame],
    bronze_dir: Path,
) -> dict[str, BronzeSheetArtifacts]:
    """Write raw sheet dumps to Bronze parquet and csv."""

    bronze_dir.mkdir(parents=True, exist_ok=True)
    artifacts: dict[str, BronzeSheetArtifacts] = {}
    for sheet_name, frame in raw_sheets.items():
        stem = BRONZE_RAW_SHEET_STEMS[sheet_name]
        parquet_path = bronze_dir / f"{stem}.parquet"
        csv_path = bronze_dir / f"{stem}.csv"
        frame.to_parquet(parquet_path, index=False)
        frame.to_csv(csv_path, index=False, encoding="utf-8-sig")
        artifacts[sheet_name] = BronzeSheetArtifacts(
            parquet_path=parquet_path,
            csv_path=csv_path,
        )

    return artifacts
