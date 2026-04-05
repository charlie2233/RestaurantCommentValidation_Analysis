"""Offline baseline evaluation for forecast-ready Gold panels."""

from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.config import Settings
from qsr_audit.forecasting.panel import (
    ForecastPanelRun,
    build_forecast_panel,
)


@dataclass(frozen=True)
class ForecastSplit:
    """Leakage-safe time split for a longitudinal panel."""

    train: pd.DataFrame
    test: pd.DataFrame
    dropped_brands: dict[str, str]
    holdout_periods: int
    min_train_periods: int


@dataclass(frozen=True)
class ForecastBaselineArtifacts:
    """Written outputs for baseline evaluation."""

    panel_parquet_path: Path
    panel_summary_markdown_path: Path
    split_metadata_json_path: Path
    metrics_json_path: Path
    metrics_csv_path: Path
    forecasts_parquet_path: Path
    summary_markdown_path: Path


@dataclass(frozen=True)
class ForecastBaselineRun:
    """Result of running offline forecast baselines."""

    panel_run: ForecastPanelRun
    split: ForecastSplit
    forecasts: pd.DataFrame
    metrics: pd.DataFrame
    summary: dict[str, Any]
    artifacts: ForecastBaselineArtifacts


def forecast_baselines(
    *,
    metric_name: str,
    settings: Settings | None = None,
    output_root: Path | None = None,
    include_advisory: bool = False,
    allow_short_history: bool = False,
    holdout_periods: int = 1,
    min_train_periods: int = 2,
    season_length: int | None = None,
    rolling_window: int = 3,
    smoothing_alpha: float = 0.5,
) -> ForecastBaselineRun:
    """Build a panel and evaluate simple leakage-safe baselines."""

    panel_run = build_forecast_panel(
        metric_name=metric_name,
        settings=settings,
        output_root=output_root,
        include_advisory=include_advisory,
        allow_short_history=allow_short_history,
        min_periods=max(holdout_periods + min_train_periods, 3),
    )
    split = build_time_split(
        panel_run.panel,
        holdout_periods=holdout_periods,
        min_train_periods=min_train_periods,
    )
    forecasts, metrics = _evaluate_baselines(
        panel=panel_run.panel,
        split=split,
        season_length=season_length,
        rolling_window=rolling_window,
        smoothing_alpha=smoothing_alpha,
    )
    summary = _build_baseline_summary(
        panel_summary=panel_run.summary,
        split=split,
        forecasts=forecasts,
        metrics=metrics,
        season_length=season_length,
    )
    artifacts = _write_baseline_outputs(
        panel_run=panel_run,
        split=split,
        forecasts=forecasts,
        metrics=metrics,
        summary=summary,
    )
    return ForecastBaselineRun(
        panel_run=panel_run,
        split=split,
        forecasts=forecasts,
        metrics=metrics,
        summary=summary,
        artifacts=artifacts,
    )


def build_time_split(
    panel: pd.DataFrame,
    *,
    holdout_periods: int = 1,
    min_train_periods: int = 2,
) -> ForecastSplit:
    """Split a panel into past-only train rows and future holdout rows per brand."""

    if holdout_periods <= 0:
        raise ValueError("`holdout_periods` must be at least 1.")
    if min_train_periods <= 0:
        raise ValueError("`min_train_periods` must be at least 1.")

    train_frames: list[pd.DataFrame] = []
    test_frames: list[pd.DataFrame] = []
    dropped_brands: dict[str, str] = {}

    for brand_name, frame in panel.groupby("canonical_brand_name", sort=True):
        ordered = frame.sort_values("as_of_date", kind="stable").reset_index(drop=True)
        if len(ordered) < holdout_periods + min_train_periods:
            dropped_brands[str(brand_name)] = (
                f"needs at least {holdout_periods + min_train_periods} periods but only has {len(ordered)}"
            )
            continue
        train = ordered.iloc[:-holdout_periods].copy()
        test = ordered.iloc[-holdout_periods:].copy()
        if train["as_of_date"].max() >= test["as_of_date"].min():
            raise ValueError(
                f"Time split leakage detected for `{brand_name}`: train periods overlap test periods."
            )
        train_frames.append(train)
        test_frames.append(test)

    if not train_frames or not test_frames:
        raise ValueError(
            "No brands had enough history for a leakage-safe split. Add more snapshot periods first."
        )

    train_frame = pd.concat(train_frames, ignore_index=True)
    test_frame = pd.concat(test_frames, ignore_index=True)
    return ForecastSplit(
        train=train_frame,
        test=test_frame,
        dropped_brands=dropped_brands,
        holdout_periods=holdout_periods,
        min_train_periods=min_train_periods,
    )


def render_forecast_baseline_summary(summary: dict[str, Any]) -> str:
    """Render a compact markdown summary for baseline evaluation."""

    lines = [
        "# Forecast Baseline Summary",
        "",
        f"- Metric: `{summary['metric_name']}`",
        f"- Brands evaluated: `{summary['brand_count']}`",
        f"- Periods: `{summary['period_count']}`",
        f"- Holdout periods: `{summary['holdout_periods']}`",
        f"- Test rows: `{summary['test_row_count']}`",
        "",
        "## Dropped Brands",
        "",
    ]

    dropped = summary.get("dropped_brands", {})
    if dropped:
        for brand_name, reason in dropped.items():
            lines.append(f"- `{brand_name}`: {reason}")
    else:
        lines.append("- None.")

    lines.extend(["", "## Confidence Coverage", ""])
    for tier, count in summary.get("confidence_coverage_by_tier", {}).items():
        lines.append(f"- `{tier}`: {count} row(s)")

    lines.extend(
        [
            "",
            "## Baseline Results",
            "",
            "| Baseline | Status | MASE | WAPE | sMAPE | RMSE | Bias |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in summary.get("baseline_results", []):
        mase = _format_metric(row.get("mase"))
        wape = _format_metric(row.get("wape"))
        smape = _format_metric(row.get("smape"))
        rmse = _format_metric(row.get("rmse"))
        bias = _format_metric(row.get("bias"))
        lines.append(
            f"| {row['baseline_name']} | {row['status']} | {mase} | {wape} | {smape} | {rmse} | {bias} |"
        )

    skipped_notes = [
        row for row in summary.get("baseline_results", []) if row.get("status") != "ok"
    ]
    if skipped_notes:
        lines.extend(["", "## Skipped Or Partial Baselines", ""])
        for row in skipped_notes:
            lines.append(
                f"- `{row['baseline_name']}`: {row.get('status_reason') or 'not available'}"
            )

    return "\n".join(lines) + "\n"


def _evaluate_baselines(
    *,
    panel: pd.DataFrame,
    split: ForecastSplit,
    season_length: int | None,
    rolling_window: int,
    smoothing_alpha: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Evaluate baselines against a fixed-origin holdout window.

    The holdout horizon is scored without ever appending holdout actuals back into
    the model history. Every prediction for a given brand is generated from the
    pre-holdout training window only, so multi-step evaluation stays leakage-safe.
    """

    by_brand = {
        brand_name: frame.sort_values("as_of_date", kind="stable").reset_index(drop=True)
        for brand_name, frame in panel.groupby("canonical_brand_name", sort=True)
    }
    train_by_brand = {
        brand_name: frame.sort_values("as_of_date", kind="stable").reset_index(drop=True)
        for brand_name, frame in split.train.groupby("canonical_brand_name", sort=True)
    }
    baseline_rows: list[dict[str, Any]] = []
    baseline_skip_reasons: defaultdict[str, list[str]] = defaultdict(list)
    scale_by_brand = {
        brand_name: _naive_scale(frame["metric_value"].tolist())
        for brand_name, frame in split.train.groupby("canonical_brand_name", sort=True)
    }

    for brand_name, full_series in by_brand.items():
        if brand_name in split.dropped_brands:
            continue
        ordered = full_series.reset_index(drop=True)
        train_history = train_by_brand[str(brand_name)]
        history_values = train_history["metric_value"].tolist()
        regular_cadence = _has_regular_cadence(ordered["as_of_date"])
        holdout_rows = split.test.loc[split.test["canonical_brand_name"] == brand_name].sort_values(
            "as_of_date", kind="stable"
        )
        for horizon_step, (_, row) in enumerate(holdout_rows.iterrows()):
            actual = float(row["metric_value"])
            common = {
                "baseline_date": str(row["as_of_date"]),
                "brand_name": row["brand_name"],
                "canonical_brand_name": row["canonical_brand_name"],
                "actual_value": actual,
                "confidence_tier": row["confidence_tier"],
                "scale": scale_by_brand.get(str(brand_name)),
            }
            baseline_rows.append(
                {
                    **common,
                    "baseline_name": "naive_last_value",
                    "prediction": float(history_values[-1]),
                }
            )
            rolling_slice = history_values[-min(len(history_values), rolling_window) :]
            baseline_rows.append(
                {
                    **common,
                    "baseline_name": f"rolling_average_{rolling_window}",
                    "prediction": float(sum(rolling_slice) / len(rolling_slice)),
                }
            )
            baseline_rows.append(
                {
                    **common,
                    "baseline_name": "exp_smoothing_alpha_0_5",
                    "prediction": float(
                        _exp_smoothing_forecast(history_values, alpha=smoothing_alpha)
                    ),
                }
            )

            if season_length is None:
                baseline_skip_reasons["seasonal_naive"].append("no season_length provided")
            elif season_length <= 1:
                baseline_skip_reasons["seasonal_naive"].append(
                    "season_length must be greater than 1"
                )
            elif not regular_cadence:
                baseline_skip_reasons["seasonal_naive"].append(
                    f"`{brand_name}` does not have regular snapshot cadence"
                )
            elif len(history_values) < season_length:
                baseline_skip_reasons["seasonal_naive"].append(
                    f"`{brand_name}` needs at least {season_length} prior periods for seasonal naive"
                )
            else:
                baseline_rows.append(
                    {
                        **common,
                        "baseline_name": "seasonal_naive",
                        "prediction": _seasonal_naive_forecast(
                            history_values,
                            horizon_step=horizon_step,
                            season_length=season_length,
                        ),
                    }
                )

    forecasts = pd.DataFrame(baseline_rows)
    if not forecasts.empty:
        forecasts["error"] = forecasts["prediction"] - forecasts["actual_value"]
        forecasts["abs_error"] = forecasts["error"].abs()
        forecasts["squared_error"] = forecasts["error"] ** 2
        forecasts["scaled_abs_error"] = forecasts.apply(
            lambda row: _scaled_abs_error(row["abs_error"], row.get("scale")),
            axis=1,
        )

    metrics_rows: list[dict[str, Any]] = []
    for baseline_name in [
        "naive_last_value",
        "seasonal_naive",
        f"rolling_average_{rolling_window}",
        "exp_smoothing_alpha_0_5",
    ]:
        baseline_frame = forecasts.loc[forecasts["baseline_name"] == baseline_name].copy()
        if baseline_frame.empty:
            reason = _dedupe_reason_messages(baseline_skip_reasons.get(baseline_name, []))
            metrics_rows.append(
                {
                    "baseline_name": baseline_name,
                    "status": "skipped",
                    "status_reason": reason
                    or "baseline could not be evaluated on the current panel",
                    "observation_count": 0,
                    "mase": None,
                    "wape": None,
                    "smape": None,
                    "rmse": None,
                    "bias": None,
                }
            )
            continue
        metrics_rows.append(
            {
                "baseline_name": baseline_name,
                "status": "ok",
                "status_reason": None,
                "observation_count": int(len(baseline_frame)),
                "mase": _round_metric(_mean_ignore_na(baseline_frame["scaled_abs_error"])),
                "wape": _round_metric(
                    _safe_ratio(
                        float(baseline_frame["abs_error"].sum()),
                        float(baseline_frame["actual_value"].abs().sum()),
                    )
                ),
                "smape": _round_metric(
                    _mean_ignore_na(
                        baseline_frame.apply(
                            lambda row: _smape_row(row["prediction"], row["actual_value"]),
                            axis=1,
                        )
                    )
                ),
                "rmse": _round_metric(math.sqrt(float(baseline_frame["squared_error"].mean()))),
                "bias": _round_metric(float(baseline_frame["error"].mean())),
            }
        )

    metrics = pd.DataFrame(metrics_rows)
    return forecasts, metrics


def _build_baseline_summary(
    *,
    panel_summary: dict[str, Any],
    split: ForecastSplit,
    forecasts: pd.DataFrame,
    metrics: pd.DataFrame,
    season_length: int | None,
) -> dict[str, Any]:
    return {
        "metric_name": panel_summary["metric_name"],
        "period_count": panel_summary["period_count"],
        "brand_count": int(forecasts["canonical_brand_name"].nunique())
        if not forecasts.empty
        else 0,
        "test_row_count": int(len(split.test)),
        "holdout_periods": split.holdout_periods,
        "min_train_periods": split.min_train_periods,
        "season_length": season_length,
        "dropped_brands": dict(sorted(split.dropped_brands.items())),
        "confidence_coverage_by_tier": panel_summary["confidence_coverage_by_tier"],
        "baseline_results": metrics.to_dict(orient="records"),
    }


def _write_baseline_outputs(
    *,
    panel_run: ForecastPanelRun,
    split: ForecastSplit,
    forecasts: pd.DataFrame,
    metrics: pd.DataFrame,
    summary: dict[str, Any],
) -> ForecastBaselineArtifacts:
    output_root = panel_run.artifacts.panel_parquet_path.parent
    split_metadata_path = output_root / "split_metadata.json"
    metrics_json_path = output_root / "baseline_metrics.json"
    metrics_csv_path = output_root / "baseline_metrics.csv"
    forecasts_parquet_path = output_root / "baseline_forecasts.parquet"
    summary_markdown_path = output_root / "baseline_summary.md"

    split_metadata_path.write_text(
        json.dumps(
            {
                "holdout_periods": split.holdout_periods,
                "min_train_periods": split.min_train_periods,
                "dropped_brands": split.dropped_brands,
                "train_row_count": int(len(split.train)),
                "test_row_count": int(len(split.test)),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    metrics_json_path.write_text(
        json.dumps(summary["baseline_results"], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    metrics.to_csv(metrics_csv_path, index=False)
    forecasts.to_parquet(forecasts_parquet_path, index=False)
    summary_markdown_path.write_text(render_forecast_baseline_summary(summary), encoding="utf-8")

    return ForecastBaselineArtifacts(
        panel_parquet_path=panel_run.artifacts.panel_parquet_path,
        panel_summary_markdown_path=panel_run.artifacts.summary_markdown_path,
        split_metadata_json_path=split_metadata_path,
        metrics_json_path=metrics_json_path,
        metrics_csv_path=metrics_csv_path,
        forecasts_parquet_path=forecasts_parquet_path,
        summary_markdown_path=summary_markdown_path,
    )


def _has_regular_cadence(dates: pd.Series) -> bool:
    date_index = pd.DatetimeIndex(pd.to_datetime(dates).sort_values(kind="stable").unique())
    if len(date_index) < 2:
        return False
    if len(date_index) >= 3 and pd.infer_freq(date_index) is not None:
        return True
    date_series = pd.Series(date_index).sort_values(kind="stable")
    diffs = date_series.diff().dropna()
    if diffs.empty:
        return False
    return diffs.nunique() == 1


def _naive_scale(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    diffs = [abs(float(curr) - float(prev)) for prev, curr in zip(values, values[1:], strict=False)]
    mean_diff = sum(diffs) / len(diffs)
    return mean_diff if mean_diff > 0 else None


def _exp_smoothing_forecast(values: list[float], *, alpha: float) -> float:
    level = float(values[0])
    for value in values[1:]:
        level = alpha * float(value) + (1 - alpha) * level
    return level


def _seasonal_naive_forecast(
    values: list[float], *, horizon_step: int, season_length: int
) -> float:
    seasonal_index = len(values) - season_length + (horizon_step % season_length)
    return float(values[seasonal_index])


def _scaled_abs_error(abs_error: float, scale: object) -> float | None:
    if scale is None or scale == 0:
        return None
    return float(abs_error) / float(scale)


def _mean_ignore_na(values: pd.Series) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return None
    return float(numeric.mean())


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def _smape_row(prediction: float, actual: float) -> float | None:
    denominator = abs(prediction) + abs(actual)
    if denominator == 0:
        return None
    return 2 * abs(prediction - actual) / denominator


def _round_metric(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 6)


def _format_metric(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "-"
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def _dedupe_reason_messages(messages: list[str]) -> str | None:
    deduped = []
    seen = set()
    for message in messages:
        text = str(message).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        deduped.append(text)
    if not deduped:
        return None
    return "; ".join(deduped)


__all__ = [
    "ForecastBaselineArtifacts",
    "ForecastBaselineRun",
    "ForecastSplit",
    "build_time_split",
    "forecast_baselines",
    "render_forecast_baseline_summary",
]
