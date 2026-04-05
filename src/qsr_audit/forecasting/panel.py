"""Forecast-ready longitudinal panel assembly from dated Gold snapshots."""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.config import Settings
from qsr_audit.forecasting.history import load_snapshot_index


@dataclass(frozen=True)
class ForecastPanelArtifacts:
    """Written outputs for a forecast-ready panel build."""

    panel_parquet_path: Path
    metadata_json_path: Path
    summary_markdown_path: Path


@dataclass(frozen=True)
class ForecastPanelRun:
    """Result of assembling a forecast-ready panel."""

    metric_name: str
    panel: pd.DataFrame
    summary: dict[str, Any]
    artifacts: ForecastPanelArtifacts


def build_forecast_panel(
    *,
    metric_name: str,
    settings: Settings | None = None,
    history_dir: Path | None = None,
    output_root: Path | None = None,
    include_advisory: bool = False,
    allow_short_history: bool = False,
    min_periods: int = 3,
) -> ForecastPanelRun:
    """Build a longitudinal panel from Gold snapshot history."""

    resolved_settings = settings or Settings()
    resolved_history_dir = (
        (history_dir or resolved_settings.gold_history_dir).expanduser().resolve()
    )
    resolved_output_root = _resolve_experiment_output_root(
        output_root=output_root,
        settings=resolved_settings,
        metric_name=metric_name,
    )

    index_frame = load_snapshot_index(resolved_history_dir)
    if index_frame.empty:
        raise ValueError(
            "Gold snapshot index is empty. Run `qsr-audit snapshot-gold` for multiple periods first."
        )

    allowed_statuses = {"publishable", "advisory"} if include_advisory else {"publishable"}
    dropped_reasons: Counter[str] = Counter()
    frames: list[pd.DataFrame] = []

    for manifest_row in index_frame.sort_values("as_of_date", kind="stable").to_dict(
        orient="records"
    ):
        snapshot_path = resolved_history_dir / str(manifest_row["snapshot_rows_path"])
        if not snapshot_path.exists():
            dropped_reasons["missing_snapshot_file"] += 1
            continue
        frame = pd.read_parquet(snapshot_path)
        if frame.empty:
            dropped_reasons["empty_snapshot"] += 1
            continue

        metric_frame = frame.loc[frame["metric_name"] == metric_name].copy()
        if metric_frame.empty:
            dropped_reasons["metric_not_present"] += 1
            continue

        status_excluded = metric_frame.loc[~metric_frame["publish_status"].isin(allowed_statuses)]
        if not status_excluded.empty:
            dropped_reasons["status_excluded"] += int(len(status_excluded))
        metric_frame = metric_frame.loc[
            metric_frame["publish_status"].isin(allowed_statuses)
        ].copy()

        missing_values = metric_frame["metric_value"].isna()
        if missing_values.any():
            dropped_reasons["missing_metric_value"] += int(missing_values.sum())
            metric_frame = metric_frame.loc[~missing_values].copy()

        if metric_frame.empty:
            continue

        metric_frame["as_of_date"] = pd.to_datetime(metric_frame["as_of_date"]).dt.date
        frames.append(
            metric_frame[
                [
                    "as_of_date",
                    "brand_name",
                    "canonical_brand_name",
                    "metric_name",
                    "metric_value",
                    "publish_status",
                    "confidence_score",
                    "source_type",
                    "source_name",
                    "source_url_or_doc_id",
                    "method_reported_or_estimated",
                    "provenance_completeness_summary",
                    "provenance_confidence_summary",
                ]
            ].copy()
        )

    if not frames:
        raise ValueError(
            f"No snapshot rows for metric `{metric_name}` matched the current inclusion policy. "
            "Run `qsr-audit snapshot-gold` first or include advisory rows explicitly."
        )

    panel = pd.concat(frames, ignore_index=True)
    duplicates = panel.duplicated(
        subset=["as_of_date", "canonical_brand_name", "metric_name"], keep="last"
    )
    if duplicates.any():
        dropped_reasons["duplicate_brand_period"] += int(duplicates.sum())
        panel = panel.loc[~duplicates].copy()

    panel.sort_values(
        by=["canonical_brand_name", "as_of_date"],
        ascending=[True, True],
        inplace=True,
        kind="stable",
        ignore_index=True,
    )
    panel["confidence_tier"] = panel["confidence_score"].map(_confidence_tier)

    period_count = int(panel["as_of_date"].nunique())
    if period_count < min_periods and not allow_short_history:
        raise ValueError(
            f"Forecast panel for `{metric_name}` needs at least {min_periods} as-of dates; "
            f"found {period_count}. Capture more publishable Gold snapshots or pass "
            "`--allow-short-history` for tests and local scaffolding."
        )

    summary = {
        "metric_name": metric_name,
        "history_periods": sorted(str(value) for value in panel["as_of_date"].unique()),
        "period_count": period_count,
        "brand_count": int(panel["canonical_brand_name"].nunique()),
        "row_count": int(len(panel)),
        "include_advisory": include_advisory,
        "dropped_rows_by_reason": dict(sorted(dropped_reasons.items())),
        "publish_status_counts": {
            str(key): int(value)
            for key, value in panel["publish_status"]
            .value_counts(dropna=False)
            .sort_index()
            .items()
        },
        "confidence_coverage_by_tier": {
            str(key): int(value)
            for key, value in panel["confidence_tier"]
            .value_counts(dropna=False)
            .sort_index()
            .items()
        },
    }

    artifacts = _write_forecast_panel_outputs(
        panel=panel,
        summary=summary,
        output_root=resolved_output_root,
    )
    return ForecastPanelRun(
        metric_name=metric_name,
        panel=panel,
        summary=summary,
        artifacts=artifacts,
    )


def render_forecast_panel_summary(summary: dict[str, Any]) -> str:
    """Render a compact markdown summary for a forecast panel."""

    lines = [
        "# Forecast Panel Summary",
        "",
        f"- Metric: `{summary['metric_name']}`",
        f"- Periods: `{summary['period_count']}`",
        f"- Brands: `{summary['brand_count']}`",
        f"- Rows: `{summary['row_count']}`",
        f"- Advisory included: `{summary['include_advisory']}`",
        "",
        "## Dropped Rows",
        "",
    ]
    dropped = summary.get("dropped_rows_by_reason", {})
    if dropped:
        for reason, count in dropped.items():
            lines.append(f"- `{reason}`: {count}")
    else:
        lines.append("- None.")

    lines.extend(
        [
            "",
            "## Confidence Coverage",
            "",
        ]
    )
    confidence = summary.get("confidence_coverage_by_tier", {})
    if confidence:
        for tier, count in confidence.items():
            lines.append(f"- `{tier}`: {count} row(s)")
    else:
        lines.append("- None.")

    lines.extend(
        [
            "",
            "## History Periods",
            "",
            "- " + ", ".join(summary.get("history_periods", [])),
        ]
    )
    return "\n".join(lines) + "\n"


def _write_forecast_panel_outputs(
    *,
    panel: pd.DataFrame,
    summary: dict[str, Any],
    output_root: Path,
) -> ForecastPanelArtifacts:
    output_root.mkdir(parents=True, exist_ok=True)
    panel_path = output_root / "panel.parquet"
    metadata_path = output_root / "panel_metadata.json"
    summary_path = output_root / "panel_summary.md"

    panel.to_parquet(panel_path, index=False)
    metadata_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    summary_path.write_text(render_forecast_panel_summary(summary), encoding="utf-8")
    return ForecastPanelArtifacts(
        panel_parquet_path=panel_path,
        metadata_json_path=metadata_path,
        summary_markdown_path=summary_path,
    )


def _resolve_experiment_output_root(
    *,
    output_root: Path | None,
    settings: Settings,
    metric_name: str,
) -> Path:
    resolved = (
        output_root
        if output_root is not None
        else settings.artifacts_dir / "forecasting" / _slugify_metric_name(metric_name)
    )
    resolved = resolved.expanduser().resolve()
    for forbidden_root in (
        settings.reports_dir.expanduser().resolve(),
        settings.strategy_dir.expanduser().resolve(),
    ):
        if _is_relative_to(resolved, forbidden_root):
            raise ValueError(
                f"Forecast experiment artifacts must not be written under analyst-facing paths like {forbidden_root}."
            )
    return resolved


def _confidence_tier(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "missing"
    score = float(value)
    if score >= 0.9:
        return "high"
    if score >= 0.75:
        return "medium"
    return "low"


def _slugify_metric_name(metric_name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", metric_name.lower()).strip("-") or "metric"


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


__all__ = [
    "ForecastPanelArtifacts",
    "ForecastPanelRun",
    "build_forecast_panel",
    "render_forecast_panel_summary",
]
