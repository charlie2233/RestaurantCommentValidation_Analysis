"""Tests for the Gold-only strategy recommendation layer."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from qsr_audit.config import Settings
from qsr_audit.strategy import (
    generate_strategy_outputs,
    match_brand_archetypes,
    parse_franchise_share,
)


def _build_settings(tmp_path: Path) -> Settings:
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    reference_dir = tmp_path / "reference"
    reports_dir = tmp_path / "reports"
    strategy_dir = tmp_path / "strategy"
    for directory in [
        raw_dir,
        bronze_dir,
        silver_dir,
        gold_dir,
        reference_dir,
        reports_dir,
        strategy_dir,
    ]:
        directory.mkdir(parents=True, exist_ok=True)

    return Settings(
        data_raw=raw_dir,
        data_bronze=bronze_dir,
        data_silver=silver_dir,
        data_gold=gold_dir,
        data_reference=reference_dir,
        reports_dir=reports_dir,
        strategy_dir=strategy_dir,
    )


def _write_gold_strategy_inputs(settings: Settings) -> None:
    pd.DataFrame(
        [
            {
                "rank": 1,
                "brand_name": "Starbucks",
                "canonical_brand_name": "Starbucks",
                "category": "咖啡",
                "ownership_model": "57%直营/43%授权",
                "central_kitchen_supply_chain_model": "集中烘焙工厂和门店咖啡机网络",
                "us_store_count_2024": 16935,
                "average_unit_volume_usd_thousands": 945,
                "fte_mid": 22.5,
                "margin_mid_pct": 20.0,
                "reference_source_count": 2,
                "overall_credibility_grade": "B",
            },
            {
                "rank": 2,
                "brand_name": "Subway",
                "canonical_brand_name": "Subway",
                "category": "三明治",
                "ownership_model": "99%加盟",
                "central_kitchen_supply_chain_model": "中央配送+门店现场组装，无烹饪设备",
                "us_store_count_2024": 19502,
                "average_unit_volume_usd_thousands": 490,
                "fte_mid": 7.0,
                "margin_mid_pct": 10.0,
                "reference_source_count": 1,
                "overall_credibility_grade": "B",
            },
            {
                "rank": 3,
                "brand_name": "Domino's",
                "canonical_brand_name": "Domino's",
                "category": "披萨",
                "ownership_model": "98%加盟",
                "central_kitchen_supply_chain_model": "集中供应链和门店烤制",
                "us_store_count_2024": 6800,
                "average_unit_volume_usd_thousands": 1400,
                "fte_mid": 13.5,
                "margin_mid_pct": 17.5,
                "reference_source_count": 1,
                "overall_credibility_grade": "B",
            },
            {
                "rank": 4,
                "brand_name": "Taco Bell",
                "canonical_brand_name": "Taco Bell",
                "category": "墨西哥",
                "ownership_model": "93%加盟",
                "central_kitchen_supply_chain_model": "门店加热+组装",
                "us_store_count_2024": 7604,
                "average_unit_volume_usd_thousands": 2100,
                "fte_mid": 17.5,
                "margin_mid_pct": 20.0,
                "reference_source_count": 0,
                "overall_credibility_grade": "D",
            },
        ]
    ).to_parquet(settings.data_gold / "reconciled_core_metrics.parquet", index=False)

    pd.DataFrame(
        [
            {
                "severity": "error",
                "category": "arithmetic",
                "check_name": "implied_auv_k",
                "brand_name": "Taco Bell",
                "message": "Taco Bell implied AUV differs materially from the recorded AUV.",
            }
        ]
    ).to_parquet(settings.data_gold / "validation_flags.parquet", index=False)

    pd.DataFrame(
        [
            {
                "signal_type": "heaping",
                "title": "Subway store count ends on a rounded digit",
                "plain_english": "Subway uses a rounded store count, which is only a weak anomaly signal.",
                "strength": "weak",
                "field_name": "us_store_count_2024",
                "details": json.dumps({"brand_name": "Subway"}),
            }
        ]
    ).to_parquet(settings.data_gold / "syntheticness_signals.parquet", index=False)


def test_archetype_helpers_match_expected_patterns() -> None:
    assert parse_franchise_share("99%加盟") == 0.99
    assert parse_franchise_share("57%直营/43%授权") == 0.43

    matches = match_brand_archetypes(
        {
            "brand_name": "Subway",
            "canonical_brand_name": "Subway",
            "category": "三明治",
            "ownership_model": "99%加盟",
            "central_kitchen_supply_chain_model": "中央配送+门店现场组装，无烹饪设备",
            "us_store_count_2024": 19502,
            "average_unit_volume_usd_thousands": 490,
            "fte_mid": 7.0,
        }
    )
    codes = {match.archetype_code for match in matches}
    assert "throughput_model" in codes
    assert "franchise_standardized" in codes
    assert "assembly_automation" in codes


def test_archetype_matching_is_field_driven_not_brand_name_driven() -> None:
    first = match_brand_archetypes(
        {
            "brand_name": "Brand A",
            "canonical_brand_name": "Brand A",
            "category": "咖啡",
            "ownership_model": "70%直营/30%授权",
            "central_kitchen_supply_chain_model": "门店仅制作饮品，依赖咖啡机和糖浆",
            "us_store_count_2024": 3000,
            "average_unit_volume_usd_thousands": 1100,
            "fte_mid": 14.0,
            "margin_mid_pct": 18.0,
        }
    )
    second = match_brand_archetypes(
        {
            "brand_name": "Completely Different Name",
            "canonical_brand_name": "Completely Different Name",
            "category": "咖啡",
            "ownership_model": "70%直营/30%授权",
            "central_kitchen_supply_chain_model": "门店仅制作饮品，依赖咖啡机和糖浆",
            "us_store_count_2024": 3000,
            "average_unit_volume_usd_thousands": 1100,
            "fte_mid": 14.0,
            "margin_mid_pct": 18.0,
        }
    )

    assert [match.archetype_code for match in first] == [match.archetype_code for match in second]


def test_heavy_cook_models_do_not_default_to_assembly_automation() -> None:
    matches = match_brand_archetypes(
        {
            "brand_name": "Chicken House",
            "canonical_brand_name": "Chicken House",
            "category": "鸡肉",
            "ownership_model": "95%加盟",
            "central_kitchen_supply_chain_model": "中央配送+门店现场压力油炸",
            "us_store_count_2024": 4500,
            "average_unit_volume_usd_thousands": 1600,
            "fte_mid": 14.0,
            "margin_mid_pct": 15.0,
        }
    )

    assert "assembly_automation" not in {match.archetype_code for match in matches}


def test_generate_strategy_outputs_builds_strategy_and_report_artifacts(
    tmp_path: Path,
) -> None:
    settings = _build_settings(tmp_path)
    _write_gold_strategy_inputs(settings)

    run = generate_strategy_outputs(settings=settings, report_dir=settings.reports_dir / "strategy")

    assert run.artifacts.recommendations_parquet_path.exists()
    assert run.artifacts.recommendations_json_path.exists()
    assert run.artifacts.playbook_markdown_path.exists()
    assert (settings.reports_dir / "strategy" / "recommendations.json").exists()

    recommendations = pd.read_parquet(run.artifacts.recommendations_parquet_path)
    assert set(recommendations["canonical_brand_name"]) == {
        "Starbucks",
        "Subway",
        "Domino's",
        "Taco Bell",
    }

    starbucks = recommendations[recommendations["canonical_brand_name"] == "Starbucks"]
    assert starbucks.iloc[0]["initiative_code"] == "ops_backoffice_ai"
    assert "beverage_equipment_queueing" in set(starbucks["initiative_code"])
    assert "digital_queueing_personalization" in set(starbucks["initiative_code"])

    dominos = recommendations[recommendations["canonical_brand_name"] == "Domino's"]
    assert "digital_queueing_personalization" in set(dominos["initiative_code"])

    taco_bell = recommendations[recommendations["canonical_brand_name"] == "Taco Bell"]
    assert set(taco_bell["strategy_readiness"]) == {"hold"}
    assert "data_foundation" in set(taco_bell["initiative_code"])
    assert "drive_thru_voice_ai" not in set(taco_bell["initiative_code"])
    assert set(taco_bell["no_roi_claim"]) == {True}

    playbook = run.artifacts.playbook_markdown_path.read_text(encoding="utf-8")
    assert "interpretation layer, not a metric-definition layer" in playbook
    assert "Executive Summary" in playbook
    assert "Portfolio Priorities" in playbook
    assert "Brand Watchlist" in playbook
