"""Publishable Gold snapshot history for downstream forecasting experiments."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.config import Settings

SNAPSHOT_INDEX_FILE = "snapshot_manifest.parquet"
SNAPSHOT_ROWS_FILE = "forecast_snapshot.parquet"
DECISIONS_ARCHIVE_FILE = "gold_publish_decisions.parquet"
PUBLISHABLE_ARCHIVE_FILE = "publishable_kpis.parquet"
SNAPSHOT_MANIFEST_FILE = "manifest.json"


@dataclass(frozen=True)
class GoldSnapshotArtifacts:
    """Paths written by a Gold snapshot operation."""

    snapshot_rows_path: Path
    archived_decisions_path: Path
    archived_publishable_path: Path
    manifest_path: Path
    index_path: Path


@dataclass(frozen=True)
class GoldSnapshotRun:
    """Result of snapshotting Gold publish outputs."""

    as_of_date: str
    included_statuses: tuple[str, ...]
    row_count: int
    brand_count: int
    metric_count: int
    artifacts: GoldSnapshotArtifacts


def snapshot_gold_history(
    *,
    as_of_date: str,
    settings: Settings | None = None,
    include_advisory: bool = False,
    gold_dir: Path | None = None,
    history_dir: Path | None = None,
) -> GoldSnapshotRun:
    """Snapshot current Gold publish decisions into a dated history location."""

    resolved_settings = settings or Settings()
    resolved_gold_dir = (gold_dir or resolved_settings.data_gold).expanduser().resolve()
    resolved_history_dir = (
        (history_dir or resolved_settings.gold_history_dir).expanduser().resolve()
    )
    snapshot_date = _parse_as_of_date(as_of_date)

    decisions_path = resolved_gold_dir / "gold_publish_decisions.parquet"
    publishable_path = resolved_gold_dir / "publishable_kpis.parquet"
    if not decisions_path.exists() or not publishable_path.exists():
        missing = [str(path) for path in (decisions_path, publishable_path) if not path.exists()]
        raise FileNotFoundError(
            "Gold snapshot history requires existing Gold gate artifacts. Missing: "
            + ", ".join(missing)
            + ". Run `qsr-audit gate-gold` first."
        )

    decisions = pd.read_parquet(decisions_path)
    publishable = pd.read_parquet(publishable_path)
    snapshot_rows = _build_snapshot_rows(
        decisions=decisions,
        publishable=publishable,
        gold_dir=resolved_gold_dir,
        as_of_date=snapshot_date.isoformat(),
        include_advisory=include_advisory,
    )

    snapshot_dir = resolved_history_dir / f"as_of_date={snapshot_date.isoformat()}"
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    snapshot_rows_path = snapshot_dir / SNAPSHOT_ROWS_FILE
    archived_decisions_path = snapshot_dir / DECISIONS_ARCHIVE_FILE
    archived_publishable_path = snapshot_dir / PUBLISHABLE_ARCHIVE_FILE
    manifest_path = snapshot_dir / SNAPSHOT_MANIFEST_FILE
    index_path = resolved_history_dir / SNAPSHOT_INDEX_FILE

    snapshot_rows.to_parquet(snapshot_rows_path, index=False)
    decisions.to_parquet(archived_decisions_path, index=False)
    publishable.to_parquet(archived_publishable_path, index=False)

    manifest_payload = {
        "as_of_date": snapshot_date.isoformat(),
        "snapshot_created_at_utc": datetime.now(UTC).isoformat(),
        "included_statuses": ["publishable", "advisory"] if include_advisory else ["publishable"],
        "row_count": int(len(snapshot_rows)),
        "brand_count": int(snapshot_rows["canonical_brand_name"].nunique())
        if not snapshot_rows.empty
        else 0,
        "metric_count": int(snapshot_rows["metric_name"].nunique())
        if not snapshot_rows.empty
        else 0,
        "snapshot_rows_path": SNAPSHOT_ROWS_FILE,
        "gold_publish_decisions_path": DECISIONS_ARCHIVE_FILE,
        "publishable_kpis_path": PUBLISHABLE_ARCHIVE_FILE,
    }
    manifest_path.write_text(
        json.dumps(manifest_payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    _write_snapshot_index(
        index_path=index_path, history_dir=resolved_history_dir, manifest=manifest_payload
    )

    return GoldSnapshotRun(
        as_of_date=snapshot_date.isoformat(),
        included_statuses=tuple(manifest_payload["included_statuses"]),
        row_count=int(manifest_payload["row_count"]),
        brand_count=int(manifest_payload["brand_count"]),
        metric_count=int(manifest_payload["metric_count"]),
        artifacts=GoldSnapshotArtifacts(
            snapshot_rows_path=snapshot_rows_path,
            archived_decisions_path=archived_decisions_path,
            archived_publishable_path=archived_publishable_path,
            manifest_path=manifest_path,
            index_path=index_path,
        ),
    )


def load_snapshot_index(history_dir: Path) -> pd.DataFrame:
    """Load the root snapshot manifest/index."""

    index_path = history_dir / SNAPSHOT_INDEX_FILE
    if not index_path.exists():
        raise FileNotFoundError(
            f"No Gold snapshot index exists at {index_path}. Run `qsr-audit snapshot-gold` first."
        )
    return pd.read_parquet(index_path)


def _build_snapshot_rows(
    *,
    decisions: pd.DataFrame,
    publishable: pd.DataFrame,
    gold_dir: Path,
    as_of_date: str,
    include_advisory: bool,
) -> pd.DataFrame:
    selected = (
        decisions.loc[decisions["publish_status"].isin(["publishable", "advisory"])].copy()
        if include_advisory
        else publishable.copy()
    )
    if selected.empty:
        selected = decisions.iloc[0:0].copy()

    selected = selected.loc[selected["publish_status"] != "blocked"].copy()
    selected["as_of_date"] = as_of_date
    selected["snapshot_scope"] = (
        "publishable_and_advisory" if include_advisory else "publishable_only"
    )
    selected["snapshot_created_at_utc"] = datetime.now(UTC).isoformat()

    coverage_path = gold_dir / "reference_coverage.parquet"
    if coverage_path.exists():
        coverage = pd.read_parquet(coverage_path)
        brand_summaries = coverage.loc[
            coverage["coverage_kind"] == "brand",
            [
                "canonical_brand_name",
                "provenance_completeness_summary",
                "provenance_confidence_summary",
            ],
        ].drop_duplicates(subset=["canonical_brand_name"], keep="last")
        selected = selected.merge(
            brand_summaries,
            on="canonical_brand_name",
            how="left",
        )
    else:
        selected["provenance_completeness_summary"] = None
        selected["provenance_confidence_summary"] = None

    return selected


def _write_snapshot_index(
    *,
    index_path: Path,
    history_dir: Path,
    manifest: dict[str, Any],
) -> None:
    index_path.parent.mkdir(parents=True, exist_ok=True)
    relative_snapshot_dir = f"as_of_date={manifest['as_of_date']}"
    row = pd.DataFrame(
        [
            {
                "as_of_date": manifest["as_of_date"],
                "snapshot_created_at_utc": manifest["snapshot_created_at_utc"],
                "included_statuses": ",".join(manifest["included_statuses"]),
                "row_count": manifest["row_count"],
                "brand_count": manifest["brand_count"],
                "metric_count": manifest["metric_count"],
                "snapshot_dir": relative_snapshot_dir,
                "snapshot_rows_path": f"{relative_snapshot_dir}/{SNAPSHOT_ROWS_FILE}",
                "gold_publish_decisions_path": f"{relative_snapshot_dir}/{DECISIONS_ARCHIVE_FILE}",
                "publishable_kpis_path": f"{relative_snapshot_dir}/{PUBLISHABLE_ARCHIVE_FILE}",
                "manifest_path": f"{relative_snapshot_dir}/{SNAPSHOT_MANIFEST_FILE}",
            }
        ]
    )

    if index_path.exists():
        existing = pd.read_parquet(index_path)
        existing = existing.loc[existing["as_of_date"] != manifest["as_of_date"]].copy()
        row = pd.concat([existing, row], ignore_index=True)

    row.sort_values(by=["as_of_date"], inplace=True, kind="stable", ignore_index=True)
    row.to_parquet(index_path, index=False)


def _parse_as_of_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"`{value}` is not a valid ISO date. Expected YYYY-MM-DD for `--as-of-date`."
        ) from exc


__all__ = [
    "DECISIONS_ARCHIVE_FILE",
    "GoldSnapshotArtifacts",
    "GoldSnapshotRun",
    "PUBLISHABLE_ARCHIVE_FILE",
    "SNAPSHOT_INDEX_FILE",
    "SNAPSHOT_MANIFEST_FILE",
    "SNAPSHOT_ROWS_FILE",
    "load_snapshot_index",
    "snapshot_gold_history",
]
