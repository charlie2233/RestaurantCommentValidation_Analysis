"""Tests for syntheticness signal utilities and orchestration."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from qsr_audit.cli import app
from qsr_audit.config import Settings
from qsr_audit.validate import run_syntheticness
from qsr_audit.validate.syntheticness_stats import (
    analyze_end_digit_heaping,
    analyze_first_digit_benford,
    analyze_nice_number_spikes,
    count_digits,
    extract_end_digits,
    extract_first_digits,
    extract_first_two_digits,
)
from typer.testing import CliRunner


def _build_settings(tmp_path: Path) -> Settings:
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    reference_dir = tmp_path / "reference"
    reports_dir = tmp_path / "reports"
    for directory in [raw_dir, bronze_dir, silver_dir, gold_dir, reference_dir, reports_dir]:
        directory.mkdir(parents=True, exist_ok=True)

    return Settings(
        data_raw=raw_dir,
        data_bronze=bronze_dir,
        data_silver=silver_dir,
        data_gold=gold_dir,
        data_reference=reference_dir,
        reports_dir=reports_dir,
    )


def _write_core_metrics_parquet(path: Path) -> None:
    rows = []
    store_counts = [
        480,
        620,
        790,
        910,
        1100,
        1300,
        1550,
        1800,
        2100,
        2500,
        2900,
        3400,
        3900,
        4600,
        5400,
        6300,
        7300,
        8600,
        10100,
        11800,
    ]
    revenue_per_store_k = [
        680,
        720,
        760,
        810,
        870,
        920,
        980,
        1040,
        1110,
        1190,
        1280,
        1380,
        1490,
        1610,
        1740,
        1880,
        2030,
        2190,
        2360,
        2540,
    ]
    for index, (store_count, auv_k) in enumerate(
        zip(store_counts, revenue_per_store_k, strict=True),
        start=1,
    ):
        rows.append(
            {
                "brand_name": f"Brand {index}",
                "row_number": index + 1,
                "us_store_count_2024": store_count,
                "systemwide_revenue_usd_billions_2024": store_count * auv_k / 1_000_000,
                "average_unit_volume_usd_thousands": auv_k,
                "fte_mid": 9 + index * 0.8,
                "margin_mid_pct": 12 + index * 0.35,
            }
        )
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_digit_extraction_helpers() -> None:
    assert extract_first_digits([123, 0.045, -98, None, "bad"]) == [1, 4]
    assert extract_first_two_digits([123, 0.045, 98, -76, None, "bad"]) == [12, 45, 98]
    assert extract_end_digits([10, 25, 38, None, "bad"]) == [0, 5, 8]
    assert count_digits([1, 1, 3, 9], domain=range(1, 10)) == {
        1: 2,
        2: 0,
        3: 1,
        4: 0,
        5: 0,
        6: 0,
        7: 0,
        8: 0,
        9: 1,
    }


def test_heaping_and_nice_number_signals_are_deterministic() -> None:
    values = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 125, 150, 175]

    heaping = analyze_end_digit_heaping(values, field_name="test_metric")
    nice_numbers = analyze_nice_number_spikes(values, field_name="test_metric")

    assert heaping.sample_size == len(values)
    assert heaping.score is not None and heaping.score > 0.8
    assert nice_numbers.sample_size == len(values)
    assert nice_numbers.score is not None and nice_numbers.score > 0.8


def test_log_uniform_sample_looks_more_benford_like_than_tidy_sequence() -> None:
    rng = np.random.default_rng(42)
    log_uniform = np.power(10.0, rng.uniform(0, 6, size=5000))
    tidy_sequence = np.tile(np.arange(100, 1000, 100), 600)

    benchmark_signal = analyze_first_digit_benford(log_uniform, field_name="log_uniform")
    synthetic_signal = analyze_first_digit_benford(tidy_sequence, field_name="tidy_sequence")

    assert benchmark_signal.score is not None
    assert synthetic_signal.score is not None
    assert benchmark_signal.score < synthetic_signal.score


def test_run_syntheticness_writes_expected_outputs(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    core_path = settings.data_silver / "core_brand_metrics.parquet"
    _write_core_metrics_parquet(core_path)

    run = run_syntheticness(core_path, settings=settings)

    assert run.artifacts.report_markdown.exists()
    assert run.artifacts.signals_parquet.exists()
    assert (
        run.counts["weak"] + run.counts["moderate"] + run.counts["strong"] + run.counts["unknown"]
        > 0
    )

    signals = pd.read_parquet(run.artifacts.signals_parquet)
    assert "signal_type" in signals.columns
    assert "plain_english" in signals.columns


def test_cli_run_syntheticness_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _build_settings(tmp_path)
    core_path = settings.data_silver / "core_brand_metrics.parquet"
    _write_core_metrics_parquet(core_path)

    monkeypatch.setenv("QSR_DATA_RAW", str(settings.data_raw))
    monkeypatch.setenv("QSR_DATA_BRONZE", str(settings.data_bronze))
    monkeypatch.setenv("QSR_DATA_SILVER", str(settings.data_silver))
    monkeypatch.setenv("QSR_DATA_GOLD", str(settings.data_gold))
    monkeypatch.setenv("QSR_DATA_REFERENCE", str(settings.data_reference))
    monkeypatch.setenv("QSR_REPORTS_DIR", str(settings.reports_dir))

    runner = CliRunner()
    result = runner.invoke(app, ["run-syntheticness", "--input", str(core_path)])

    assert result.exit_code == 0
    assert "Syntheticness analysis complete" in result.stdout
    assert (settings.reports_dir / "validation" / "syntheticness_report.md").exists()
    assert (settings.data_gold / "syntheticness_signals.parquet").exists()
