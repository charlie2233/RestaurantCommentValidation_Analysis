"""Tests for forecast-readiness scaffolding and offline baseline evaluation."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from qsr_audit.forecasting import (
    build_forecast_panel,
    build_time_split,
    forecast_baselines,
    prepare_chronos_experiment,
    snapshot_gold_history,
)
from qsr_audit.forecasting.baselines import _has_regular_cadence

from tests.helpers import build_settings


def _write_current_gold_gate_outputs(
    settings,
    *,
    mcd_system_sales: float,
    taco_system_sales: float,
) -> None:
    decisions = pd.DataFrame(
        [
            {
                "brand_name": "McDonald's",
                "canonical_brand_name": "McDonald's",
                "metric_name": "system_sales",
                "metric_value": mcd_system_sales,
                "publish_status": "publishable",
                "blocking_reasons": "[]",
                "warning_reasons": "[]",
                "source_type": "qsr50",
                "source_name": "QSR 50",
                "source_url_or_doc_id": "doc-mcd",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.95,
                "validation_references": "[]",
            },
            {
                "brand_name": "Taco Bell",
                "canonical_brand_name": "Taco Bell",
                "metric_name": "system_sales",
                "metric_value": taco_system_sales,
                "publish_status": "publishable",
                "blocking_reasons": "[]",
                "warning_reasons": "[]",
                "source_type": "qsr50",
                "source_name": "QSR 50",
                "source_url_or_doc_id": "doc-taco",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.90,
                "validation_references": "[]",
            },
            {
                "brand_name": "McDonald's",
                "canonical_brand_name": "McDonald's",
                "metric_name": "margin_mid_pct",
                "metric_value": 19.0,
                "publish_status": "advisory",
                "blocking_reasons": "[]",
                "warning_reasons": json.dumps(
                    [
                        "Restaurant-level operating margin estimates remain advisory-only until stronger analyst-supplied evidence is attached."
                    ]
                ),
                "source_type": "workbook",
                "source_name": "fixture.xlsx",
                "source_url_or_doc_id": "local-workbook",
                "as_of_date": None,
                "method_reported_or_estimated": "reported_in_workbook",
                "confidence_score": 0.35,
                "validation_references": "[]",
            },
            {
                "brand_name": "Taco Bell",
                "canonical_brand_name": "Taco Bell",
                "metric_name": "auv",
                "metric_value": 2100.0,
                "publish_status": "blocked",
                "blocking_reasons": json.dumps(["Reference confidence missing"]),
                "warning_reasons": "[]",
                "source_type": "qsr50",
                "source_name": "QSR 50",
                "source_url_or_doc_id": "doc-taco",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.40,
                "validation_references": "[]",
            },
        ]
    )
    decisions.to_parquet(settings.data_gold / "gold_publish_decisions.parquet", index=False)
    decisions.loc[decisions["publish_status"] == "publishable"].to_parquet(
        settings.data_gold / "publishable_kpis.parquet",
        index=False,
    )
    pd.DataFrame(
        [
            {
                "coverage_kind": "brand",
                "canonical_brand_name": "McDonald's",
                "provenance_completeness_summary": "All provenance fields populated.",
                "provenance_confidence_summary": "Average confidence 0.95 across 1 row(s).",
            },
            {
                "coverage_kind": "brand",
                "canonical_brand_name": "Taco Bell",
                "provenance_completeness_summary": "All provenance fields populated.",
                "provenance_confidence_summary": "Average confidence 0.90 across 1 row(s).",
            },
        ]
    ).to_parquet(settings.data_gold / "reference_coverage.parquet", index=False)


def _snapshot_fixture_series(settings, values_by_date: list[tuple[str, float, float]]) -> None:
    for as_of_date, mcd_value, taco_value in values_by_date:
        _write_current_gold_gate_outputs(
            settings,
            mcd_system_sales=mcd_value,
            taco_system_sales=taco_value,
        )
        snapshot_gold_history(as_of_date=as_of_date, settings=settings)


def test_snapshot_gold_writes_dated_snapshot_and_manifest(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    _write_current_gold_gate_outputs(settings, mcd_system_sales=53.5, taco_system_sales=15.0)

    run = snapshot_gold_history(as_of_date="2025-01-31", settings=settings)

    assert run.artifacts.snapshot_rows_path.exists()
    assert run.artifacts.manifest_path.exists()
    assert run.artifacts.index_path.exists()
    assert run.artifacts.archived_decisions_path.exists()
    assert run.artifacts.archived_publishable_path.exists()

    snapshot_frame = pd.read_parquet(run.artifacts.snapshot_rows_path)
    assert set(snapshot_frame["publish_status"]) == {"publishable"}
    assert set(snapshot_frame["metric_name"]) == {"system_sales"}
    manifest = json.loads(run.artifacts.manifest_path.read_text(encoding="utf-8"))
    assert manifest["as_of_date"] == "2025-01-31"
    index_frame = pd.read_parquet(run.artifacts.index_path)
    assert index_frame["as_of_date"].tolist() == ["2025-01-31"]


def test_snapshot_gold_includes_advisory_only_when_requested(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    _write_current_gold_gate_outputs(settings, mcd_system_sales=53.5, taco_system_sales=15.0)

    default_run = snapshot_gold_history(as_of_date="2025-01-31", settings=settings)
    advisory_run = snapshot_gold_history(
        as_of_date="2025-02-28",
        settings=settings,
        include_advisory=True,
    )

    default_snapshot = pd.read_parquet(default_run.artifacts.snapshot_rows_path)
    advisory_snapshot = pd.read_parquet(advisory_run.artifacts.snapshot_rows_path)
    assert set(default_snapshot["publish_status"]) == {"publishable"}
    assert set(advisory_snapshot["publish_status"]) == {"publishable", "advisory"}
    assert "blocked" not in set(advisory_snapshot["publish_status"])


def test_build_forecast_panel_fails_clearly_on_short_history(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    _snapshot_fixture_series(
        settings,
        [
            ("2025-01-31", 10.0, 20.0),
            ("2025-02-28", 12.0, 21.0),
        ],
    )

    with pytest.raises(ValueError, match="needs at least 3 as-of dates"):
        build_forecast_panel(metric_name="system_sales", settings=settings)

    run = build_forecast_panel(
        metric_name="system_sales",
        settings=settings,
        allow_short_history=True,
    )
    assert run.summary["period_count"] == 2
    assert run.artifacts.panel_parquet_path.exists()


def test_time_split_helper_prevents_future_leakage() -> None:
    panel = pd.DataFrame(
        [
            {
                "as_of_date": value,
                "brand_name": brand,
                "canonical_brand_name": brand,
                "metric_name": "system_sales",
                "metric_value": metric_value,
                "publish_status": "publishable",
                "confidence_score": 0.95,
                "source_type": "qsr50",
                "source_name": "QSR 50",
                "source_url_or_doc_id": "doc",
                "method_reported_or_estimated": "reported",
                "provenance_completeness_summary": "ok",
                "provenance_confidence_summary": "ok",
                "confidence_tier": "high",
            }
            for brand, series in {
                "McDonald's": [10.0, 12.0, 14.0, 16.0],
                "Taco Bell": [20.0, 21.0, 22.0, 23.0],
            }.items()
            for value, metric_value in zip(
                [
                    pd.Timestamp("2025-01-31").date(),
                    pd.Timestamp("2025-02-28").date(),
                    pd.Timestamp("2025-03-31").date(),
                    pd.Timestamp("2025-04-30").date(),
                ],
                series,
                strict=False,
            )
        ]
    )

    split = build_time_split(panel, holdout_periods=1, min_train_periods=2)

    for brand_name in ["McDonald's", "Taco Bell"]:
        train_brand = split.train.loc[split.train["canonical_brand_name"] == brand_name]
        test_brand = split.test.loc[split.test["canonical_brand_name"] == brand_name]
        assert train_brand["as_of_date"].max() < test_brand["as_of_date"].min()
        assert set(train_brand["as_of_date"]).isdisjoint(set(test_brand["as_of_date"]))


def test_forecast_baseline_metrics_are_deterministic(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    _snapshot_fixture_series(
        settings,
        [
            ("2025-01-31", 10.0, 20.0),
            ("2025-02-28", 12.0, 21.0),
            ("2025-03-31", 14.0, 22.0),
            ("2025-04-30", 16.0, 23.0),
        ],
    )

    run = forecast_baselines(metric_name="system_sales", settings=settings)

    naive = run.metrics.loc[run.metrics["baseline_name"] == "naive_last_value"].iloc[0]
    assert naive["status"] == "ok"
    assert naive["mase"] == pytest.approx(1.0)
    assert naive["wape"] == pytest.approx(3 / 39, rel=1e-6)
    assert naive["rmse"] == pytest.approx((2.5) ** 0.5, rel=1e-6)
    assert naive["bias"] == pytest.approx(-1.5)

    seasonal = run.metrics.loc[run.metrics["baseline_name"] == "seasonal_naive"].iloc[0]
    assert seasonal["status"] == "skipped"
    assert "season_length" in seasonal["status_reason"]
    summary_text = run.artifacts.summary_markdown_path.read_text(encoding="utf-8")
    assert "Forecast Baseline Summary" in summary_text
    assert "naive_last_value" in summary_text


def test_forecast_baselines_holdout_two_uses_fixed_origin_history(
    tmp_path: Path,
) -> None:
    settings = build_settings(tmp_path)
    _snapshot_fixture_series(
        settings,
        [
            ("2025-01-31", 10.0, 20.0),
            ("2025-02-28", 20.0, 21.0),
            ("2025-03-31", 30.0, 22.0),
            ("2025-04-30", 100.0, 23.0),
            ("2025-05-31", 200.0, 24.0),
        ],
    )

    run = forecast_baselines(
        metric_name="system_sales",
        settings=settings,
        holdout_periods=2,
        rolling_window=2,
        season_length=2,
    )
    mcd_forecasts = run.forecasts.loc[
        run.forecasts["canonical_brand_name"] == "McDonald's"
    ].sort_values(["baseline_name", "baseline_date"], kind="stable")

    naive_predictions = mcd_forecasts.loc[
        mcd_forecasts["baseline_name"] == "naive_last_value", "prediction"
    ].tolist()
    assert naive_predictions == [30.0, 30.0]

    rolling_predictions = mcd_forecasts.loc[
        mcd_forecasts["baseline_name"] == "rolling_average_2", "prediction"
    ].tolist()
    assert rolling_predictions == [25.0, 25.0]

    smoothing_predictions = mcd_forecasts.loc[
        mcd_forecasts["baseline_name"] == "exp_smoothing_alpha_0_5", "prediction"
    ].tolist()
    assert smoothing_predictions == [22.5, 22.5]

    seasonal_predictions = mcd_forecasts.loc[
        mcd_forecasts["baseline_name"] == "seasonal_naive", "prediction"
    ].tolist()
    assert seasonal_predictions == [20.0, 30.0]


def test_has_regular_cadence_accepts_month_end_dates() -> None:
    assert _has_regular_cadence(
        pd.Series(
            [
                "2025-01-31",
                "2025-02-28",
                "2025-03-31",
                "2025-04-30",
            ]
        )
    )


def test_has_regular_cadence_accepts_quarter_end_dates() -> None:
    assert _has_regular_cadence(
        pd.Series(
            [
                "2025-03-31",
                "2025-06-30",
                "2025-09-30",
                "2025-12-31",
            ]
        )
    )


def test_has_regular_cadence_rejects_ragged_dates() -> None:
    assert not _has_regular_cadence(
        pd.Series(
            [
                "2025-01-31",
                "2025-03-15",
                "2025-04-30",
                "2025-07-20",
            ]
        )
    )


def test_seasonal_naive_runs_on_regular_month_end_panel(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    _snapshot_fixture_series(
        settings,
        [
            ("2025-01-31", 10.0, 20.0),
            ("2025-02-28", 12.0, 21.0),
            ("2025-03-31", 14.0, 22.0),
            ("2025-04-30", 16.0, 23.0),
        ],
    )

    run = forecast_baselines(
        metric_name="system_sales",
        settings=settings,
        season_length=2,
    )

    seasonal = run.metrics.loc[run.metrics["baseline_name"] == "seasonal_naive"].iloc[0]
    assert seasonal["status"] == "ok"
    assert seasonal["observation_count"] == 2


def test_prepare_chronos_experiment_is_opt_in_and_ci_guarded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    panel_path = tmp_path / "panel.parquet"
    pd.DataFrame(
        [
            {
                "as_of_date": "2025-01-31",
                "canonical_brand_name": "McDonald's",
                "metric_value": 10.0,
            },
            {
                "as_of_date": "2025-02-28",
                "canonical_brand_name": "McDonald's",
                "metric_value": 12.0,
            },
            {
                "as_of_date": "2025-03-31",
                "canonical_brand_name": "McDonald's",
                "metric_value": 14.0,
            },
        ]
    ).to_parquet(panel_path, index=False)

    disabled = prepare_chronos_experiment(panel_path, enabled=False)
    assert disabled.runnable is False
    assert "opt-in" in disabled.reason

    monkeypatch.setenv("CI", "true")
    guarded = prepare_chronos_experiment(panel_path, enabled=True)
    assert guarded.runnable is False
    assert "disabled in CI" in guarded.reason


def test_forecast_artifacts_cannot_write_under_reports_or_strategy(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    _snapshot_fixture_series(
        settings,
        [
            ("2025-01-31", 10.0, 20.0),
            ("2025-02-28", 12.0, 21.0),
            ("2025-03-31", 14.0, 22.0),
        ],
    )

    with pytest.raises(ValueError, match="must not be written under analyst-facing paths"):
        build_forecast_panel(
            metric_name="system_sales",
            settings=settings,
            output_root=settings.reports_dir,
        )

    with pytest.raises(ValueError, match="must not be written under analyst-facing paths"):
        forecast_baselines(
            metric_name="system_sales",
            settings=settings,
            output_root=settings.strategy_dir,
        )
