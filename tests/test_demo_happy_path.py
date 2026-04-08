"""Five-brand happy-path demo tests."""

from __future__ import annotations

import pandas as pd
import pytest
from qsr_audit.cli import app
from qsr_audit.demo import DEMO_BRANDS
from typer.testing import CliRunner

from tests.helpers import build_settings


def _write_demo_workbook(path) -> None:
    core_brand_metrics = pd.DataFrame(
        [
            {
                "排名": 2,
                "品牌": "Starbucks",
                "品类": "咖啡/饮品",
                "美国门店数\n(2024)": 16935,
                "全系统营收\n($B, 2024)": 26.2,
                "店均AUV\n($K)": 945,
                "店均日等效FTE\n(估算)": "20-26",
                "门店利润率\n(估算)": "16-18%",
                "央厨/供应链模式": "饮品标准化 + 后台备料",
                "所有制模式": "混合",
            },
            {
                "排名": 5,
                "品牌": "Taco Bell",
                "品类": "墨西哥快餐",
                "美国门店数\n(2024)": 7604,
                "全系统营收\n($B, 2024)": 15.0,
                "店均AUV\n($K)": 2100,
                "店均日等效FTE\n(估算)": "16-19",
                "门店利润率\n(估算)": "18-22%",
                "央厨/供应链模式": "区域备餐 + 门店组装",
                "所有制模式": "97%加盟",
            },
            {
                "排名": 26,
                "品牌": "Raising Cane's",
                "品类": "鸡肉",
                "美国门店数\n(2024)": 870,
                "全系统营收\n($B, 2024)": 5.0,
                "店均AUV\n($K)": 6200,
                "店均日等效FTE\n(估算)": "24-28",
                "门店利润率\n(估算)": "18-21%",
                "央厨/供应链模式": "高吞吐单品模式",
                "所有制模式": "公司直营为主",
            },
            {
                "排名": 28,
                "品牌": "Dutch Bros",
                "品类": "咖啡/饮品",
                "美国门店数\n(2024)": 831,
                "全系统营收\n($B, 2024)": 1.4,
                "店均AUV\n($K)": 1973,
                "店均日等效FTE\n(估算)": "11-14",
                "门店利润率\n(估算)": "14-18%",
                "央厨/供应链模式": "轻餐饮+饮品快取",
                "所有制模式": "混合",
            },
            {
                "排名": 29,
                "品牌": "Shake Shack",
                "品类": "汉堡",
                "美国门店数\n(2024)": 659,
                "全系统营收\n($B, 2024)": 1.45,
                "店均AUV\n($K)": 4000,
                "店均日等效FTE\n(估算)": "19-23",
                "门店利润率\n(估算)": "15-18%",
                "央厨/供应链模式": "高端快餐现制",
                "所有制模式": "公司直营为主",
            },
        ]
    )
    ai_strategy_registry = pd.DataFrame(
        [
            {
                "品牌": "Starbucks",
                "AI/技术策略方向": "数字化个性化",
                "关键举措": "移动端排序和推荐",
                "部署规模": "Enterprise",
                "落地效果/数据": "复购提升",
                "当前状态(2026Q1)": "推进中",
            },
            {
                "品牌": "Taco Bell",
                "AI/技术策略方向": "队列与备料",
                "关键举措": "高峰编排",
                "部署规模": "Pilot",
                "落地效果/数据": "拥堵下降",
                "当前状态(2026Q1)": "推进中",
            },
            {
                "品牌": "Raising Cane's",
                "AI/技术策略方向": "厨房节拍",
                "关键举措": "订单编排",
                "部署规模": "Pilot",
                "落地效果/数据": "吞吐提升",
                "当前状态(2026Q1)": "推进中",
            },
            {
                "品牌": "Dutch Bros",
                "AI/技术策略方向": "饮品运营",
                "关键举措": "快取和备料优化",
                "部署规模": "Pilot",
                "落地效果/数据": "等待时间下降",
                "当前状态(2026Q1)": "推进中",
            },
            {
                "品牌": "Shake Shack",
                "AI/技术策略方向": "厨房与队列",
                "关键举措": "峰值拥堵调度",
                "部署规模": "Pilot",
                "落地效果/数据": "履约稳定",
                "当前状态(2026Q1)": "推进中",
            },
        ]
    )
    data_notes = pd.DataFrame(
        [
            {"字段": "美国门店数", "说明": "2024年底美国门店数量"},
            {"字段": "店均AUV", "说明": "单位为千美元"},
            {"字段": "关键发现", "说明": None},
            {"字段": "1", "说明": "本工作簿仍是待验证假设工件"},
            {"字段": "2", "说明": "QSR50作为首个外部对照源"},
        ]
    )

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        core_brand_metrics.to_excel(writer, sheet_name="QSR Top30 核心数据", index=False)
        ai_strategy_registry.to_excel(writer, sheet_name="AI策略与落地效果", index=False)
        data_notes.to_excel(writer, sheet_name="数据说明与来源", index=False)


def _write_qsr50_reference_csv(path, *, include_shake_shack: bool = True) -> None:
    rows = [
        {
            "brand_name": "Starbucks",
            "canonical_brand_name": "Starbucks",
            "source_type": "qsr50",
            "source_name": "QSR 50 2025",
            "source_url_or_doc_id": "https://www.qsrmagazine.com/story/the-2025-qsr-50-fast-foods-leading-annual-reportqsr-50-2025/",
            "as_of_date": "2024-12-31",
            "method_reported_or_estimated": "estimated",
            "confidence_score": 0.85,
            "notes": "Estimated in QSR 50.",
            "qsr50_rank": 2,
            "us_store_count_2024": 16935,
            "systemwide_revenue_usd_billions_2024": 30.4,
            "average_unit_volume_usd_thousands": 1800,
            "currency": "USD",
            "geography": "US",
            "source_page": "Top 50 chart",
            "source_excerpt": "Starbucks rank 2; 2024 U.S. sales 30400 million; 2024 AUV 1800 thousand; 2024 total units 16935.",
        },
        {
            "brand_name": "Taco Bell",
            "canonical_brand_name": "Taco Bell",
            "source_type": "qsr50",
            "source_name": "QSR 50 2025",
            "source_url_or_doc_id": "https://www.qsrmagazine.com/story/the-2025-qsr-50-fast-foods-leading-annual-reportqsr-50-2025/",
            "as_of_date": "2024-12-31",
            "method_reported_or_estimated": "reported",
            "confidence_score": 0.95,
            "notes": "Reported in QSR 50.",
            "qsr50_rank": 4,
            "us_store_count_2024": 7604,
            "systemwide_revenue_usd_billions_2024": 16.2,
            "average_unit_volume_usd_thousands": 2130,
            "currency": "USD",
            "geography": "US",
            "source_page": "Top 50 chart",
            "source_excerpt": "Taco Bell rank 4; 2024 U.S. sales 16200 million; 2024 AUV 2130 thousand; 2024 total units 7604.",
        },
        {
            "brand_name": "Raising Cane's",
            "canonical_brand_name": "Raising Cane's",
            "source_type": "qsr50",
            "source_name": "QSR 50 2025",
            "source_url_or_doc_id": "https://www.qsrmagazine.com/story/the-2025-qsr-50-fast-foods-leading-annual-reportqsr-50-2025/",
            "as_of_date": "2024-12-31",
            "method_reported_or_estimated": "reported",
            "confidence_score": 0.95,
            "notes": "Reported in QSR 50.",
            "qsr50_rank": 16,
            "us_store_count_2024": 828,
            "systemwide_revenue_usd_billions_2024": 4.96,
            "average_unit_volume_usd_thousands": 6560,
            "currency": "USD",
            "geography": "US",
            "source_page": "Top 50 chart",
            "source_excerpt": "Raising Cane's rank 16; 2024 U.S. sales 4960 million; 2024 AUV 6560 thousand; 2024 total units 828.",
        },
        {
            "brand_name": "Dutch Bros",
            "canonical_brand_name": "Dutch Bros",
            "source_type": "qsr50",
            "source_name": "QSR 50 2025",
            "source_url_or_doc_id": "https://www.qsrmagazine.com/story/the-2025-qsr-50-fast-foods-leading-annual-reportqsr-50-2025/",
            "as_of_date": "2024-12-31",
            "method_reported_or_estimated": "reported",
            "confidence_score": 0.95,
            "notes": "Reported in QSR 50.",
            "qsr50_rank": 33,
            "us_store_count_2024": 982,
            "systemwide_revenue_usd_billions_2024": 1.819,
            "average_unit_volume_usd_thousands": 2018,
            "currency": "USD",
            "geography": "US",
            "source_page": "Top 50 chart",
            "source_excerpt": "Dutch Bros rank 33; 2024 U.S. sales 1819 million; 2024 AUV 2018 thousand; 2024 total units 982.",
        },
    ]
    if include_shake_shack:
        rows.append(
            {
                "brand_name": "Shake Shack",
                "canonical_brand_name": "Shake Shack",
                "source_type": "qsr50",
                "source_name": "QSR 50 2025",
                "source_url_or_doc_id": "https://www.qsrmagazine.com/story/the-2025-qsr-50-fast-foods-leading-annual-reportqsr-50-2025/",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "estimated",
                "confidence_score": 0.85,
                "notes": "Estimated in QSR 50.",
                "qsr50_rank": 37,
                "us_store_count_2024": 373,
                "systemwide_revenue_usd_billions_2024": 1.351,
                "average_unit_volume_usd_thousands": 3900,
                "currency": "USD",
                "geography": "US",
                "source_page": "Top 50 chart",
                "source_excerpt": "Shake Shack rank 37; 2024 U.S. sales 1351 million; 2024 AUV 3900 thousand; 2024 total units 373.",
            }
        )
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_sec_reference_csv(path) -> None:
    pd.DataFrame(
        [
            {
                "brand_name": "Taco Bell",
                "canonical_brand_name": "Taco Bell",
                "source_type": "sec_filings",
                "source_name": "Example 10-K",
                "source_url_or_doc_id": "https://example.com/taco-bell-10k",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.99,
                "notes": "Conflicting fixture row used to confirm the demo stays QSR50-only.",
                "filing_type": "10-K",
                "filing_date": "2025-02-20",
                "us_store_count": 9999,
                "systemwide_revenue_usd_billions": 99.9,
                "revenue_segment_notes": "Conflicting test fixture.",
                "currency": "USD",
                "geography": "US",
                "source_page": "Item 7",
                "source_excerpt": "Intentionally conflicting fixture row.",
            }
        ]
    ).to_csv(path, index=False)


def _set_settings_env(monkeypatch: pytest.MonkeyPatch, settings) -> None:
    monkeypatch.setenv("QSR_DATA_RAW", str(settings.data_raw))
    monkeypatch.setenv("QSR_DATA_BRONZE", str(settings.data_bronze))
    monkeypatch.setenv("QSR_DATA_SILVER", str(settings.data_silver))
    monkeypatch.setenv("QSR_DATA_GOLD", str(settings.data_gold))
    monkeypatch.setenv("QSR_DATA_REFERENCE", str(settings.data_reference))
    monkeypatch.setenv("QSR_REPORTS_DIR", str(settings.reports_dir))
    monkeypatch.setenv("QSR_STRATEGY_DIR", str(settings.strategy_dir))
    monkeypatch.setenv("QSR_ARTIFACTS_DIR", str(settings.artifacts_dir))


def test_demo_happy_path_command_end_to_end_without_arguments(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = build_settings(tmp_path)
    workbook_path = settings.data_raw / "demo_fixture.xlsx"
    _write_demo_workbook(workbook_path)
    _write_qsr50_reference_csv(settings.data_reference / "qsr50_reference.csv")
    _set_settings_env(monkeypatch, settings)

    runner = CliRunner()
    result = runner.invoke(app, ["demo-happy-path"])

    assert result.exit_code == 0, result.stdout
    assert "Five-brand happy-path demo complete" in result.stdout
    assert (settings.reports_dir / "validation" / "core_scorecard.html").exists()
    assert (settings.reports_dir / "reconciliation" / "brand_deltas.csv").exists()
    assert (settings.reports_dir / "summary" / "top_risks.md").exists()
    assert (settings.data_gold / "demo_gold.parquet").exists()
    assert (settings.data_gold / "demo_syntheticness.parquet").exists()


def test_demo_happy_path_command_end_to_end(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = build_settings(tmp_path)
    workbook_path = settings.data_raw / "demo_fixture.xlsx"
    _write_demo_workbook(workbook_path)
    _write_qsr50_reference_csv(settings.data_reference / "qsr50_reference.csv")
    _set_settings_env(monkeypatch, settings)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "demo-happy-path",
            "--input",
            str(workbook_path),
            "--reference-dir",
            str(settings.data_reference),
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert "Five-brand happy-path demo complete" in result.stdout

    core_scorecard = settings.reports_dir / "validation" / "core_scorecard.html"
    brand_deltas = settings.reports_dir / "reconciliation" / "brand_deltas.csv"
    top_risks = settings.reports_dir / "summary" / "top_risks.md"
    demo_gold = settings.data_gold / "demo_gold.parquet"
    demo_syntheticness = settings.data_gold / "demo_syntheticness.parquet"

    for path in (core_scorecard, brand_deltas, top_risks, demo_gold, demo_syntheticness):
        assert path.exists()

    scorecard_html = core_scorecard.read_text(encoding="utf-8")
    assert "Five-Brand Happy-Path Demo" in scorecard_html
    assert "Publish recommendation" in scorecard_html
    assert "Syntheticness score" in scorecard_html

    deltas_frame = pd.read_csv(brand_deltas)
    assert len(deltas_frame.index) == len(DEMO_BRANDS) * 4
    assert {
        "brand_name",
        "metric_name",
        "workbook_value",
        "reference_value",
        "relative_error",
        "publish_status",
    }.issubset(deltas_frame.columns)

    demo_gold_frame = pd.read_parquet(demo_gold)
    assert set(demo_gold_frame["canonical_brand_name"]) == set(DEMO_BRANDS)
    assert "brand_publish_status_recommendation" in demo_gold_frame.columns

    syntheticness_frame = pd.read_parquet(demo_syntheticness)
    assert list(syntheticness_frame.columns) == [
        "brand_name",
        "canonical_brand_name",
        "syntheticness_score",
        "supporting_signals",
        "review_required",
        "caveats",
    ]
    assert set(syntheticness_frame["canonical_brand_name"]) == set(DEMO_BRANDS)

    top_risks_text = top_risks.read_text(encoding="utf-8")
    assert "# Top Risks" in top_risks_text
    assert "Largest Reconciliation Deltas" in top_risks_text
    assert "Publishability Risks" in top_risks_text


def test_demo_happy_path_zero_arg_mode_fails_cleanly_when_workbook_discovery_is_ambiguous(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = build_settings(tmp_path)
    workbook_path = settings.data_raw / "demo_fixture.xlsx"
    second_workbook_path = settings.data_raw / "demo_fixture_copy.xlsx"
    _write_demo_workbook(workbook_path)
    _write_demo_workbook(second_workbook_path)
    _write_qsr50_reference_csv(settings.data_reference / "qsr50_reference.csv")
    _set_settings_env(monkeypatch, settings)

    runner = CliRunner()
    result = runner.invoke(app, ["demo-happy-path"])
    normalized_stdout = " ".join(result.stdout.split())

    assert result.exit_code != 0
    assert "Happy-path demo failed" in normalized_stdout
    assert "requires exactly one workbook under `data/raw/`" in normalized_stdout
    assert "demo_fixture.xlsx" in normalized_stdout
    assert "demo_fixture_copy.xlsx" in normalized_stdout
    assert "Pass `--input <workbook>` explicitly" in normalized_stdout


def test_demo_happy_path_fails_cleanly_when_qsr50_rows_are_missing(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = build_settings(tmp_path)
    workbook_path = settings.data_raw / "demo_fixture.xlsx"
    _write_demo_workbook(workbook_path)
    _write_qsr50_reference_csv(
        settings.data_reference / "qsr50_reference.csv",
        include_shake_shack=False,
    )
    _set_settings_env(monkeypatch, settings)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "demo-happy-path",
            "--input",
            str(workbook_path),
            "--reference-dir",
            str(settings.data_reference),
        ],
    )

    assert result.exit_code != 0
    assert "Happy-path demo failed" in result.stdout
    assert "requires QSR50 coverage for all" in result.stdout
    assert "five demo brands" in result.stdout
    assert "Shake Shack" in result.stdout


def test_demo_happy_path_ignores_non_qsr50_reference_rows_in_same_directory(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = build_settings(tmp_path)
    workbook_path = settings.data_raw / "demo_fixture.xlsx"
    _write_demo_workbook(workbook_path)
    _write_qsr50_reference_csv(settings.data_reference / "qsr50_reference.csv")
    _write_sec_reference_csv(settings.data_reference / "sec_filings_reference.csv")
    _set_settings_env(monkeypatch, settings)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "demo-happy-path",
            "--input",
            str(workbook_path),
            "--reference-dir",
            str(settings.data_reference),
        ],
    )

    assert result.exit_code == 0, result.stdout

    deltas_frame = pd.read_csv(settings.reports_dir / "reconciliation" / "brand_deltas.csv")
    assert set(deltas_frame["brand_name"]) == set(DEMO_BRANDS)
    assert set(deltas_frame["source_type"]) == {"qsr50"}
    assert set(deltas_frame["source_name"]) == {"QSR 50 2025"}

    taco_bell_store_count = deltas_frame.loc[
        deltas_frame["brand_name"].eq("Taco Bell") & deltas_frame["metric_name"].eq("store_count"),
        "reference_value",
    ]
    assert taco_bell_store_count.tolist() == [7604.0]
