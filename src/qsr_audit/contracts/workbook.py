"""Workbook contracts and dataset naming for the first ingestion slice."""

from __future__ import annotations

from typing import Final

CORE_BRAND_METRICS_SHEET: Final = "QSR Top30 核心数据"
AI_STRATEGY_SHEET: Final = "AI策略与落地效果"
DATA_NOTES_SHEET: Final = "数据说明与来源"

REQUIRED_WORKBOOK_SHEETS: Final[tuple[str, ...]] = (
    CORE_BRAND_METRICS_SHEET,
    AI_STRATEGY_SHEET,
    DATA_NOTES_SHEET,
)

BRONZE_RAW_SHEET_STEMS: Final[dict[str, str]] = {
    CORE_BRAND_METRICS_SHEET: "qsr_top30_core_data_raw",
    AI_STRATEGY_SHEET: "ai_strategy_implementation_raw",
    DATA_NOTES_SHEET: "data_notes_and_sources_raw",
}

SILVER_OUTPUT_FILES: Final[dict[str, str]] = {
    "core_brand_metrics": "core_brand_metrics.parquet",
    "ai_strategy_registry": "ai_strategy_registry.parquet",
    "data_notes": "data_notes.parquet",
    "key_findings": "key_findings.parquet",
}

CORE_BRAND_METRICS_COLUMN_MAP: Final[dict[str, str]] = {
    "排名": "rank",
    "品牌": "brand_name",
    "品类": "category",
    "美国门店数\n(2024)": "us_store_count_2024",
    "全系统营收\n($B, 2024)": "systemwide_revenue_usd_billions_2024",
    "店均AUV\n($K)": "average_unit_volume_usd_thousands",
    "店均日等效FTE\n(估算)": "store_daily_equivalent_fte_range",
    "门店利润率\n(估算)": "store_margin_range_pct",
    "央厨/供应链模式": "central_kitchen_supply_chain_model",
    "所有制模式": "ownership_model",
}

AI_STRATEGY_REGISTRY_COLUMN_MAP: Final[dict[str, str]] = {
    "品牌": "brand_name",
    "AI/技术策略方向": "ai_strategy_direction",
    "关键举措": "key_initiatives",
    "部署规模": "deployment_scale",
    "落地效果/数据": "impact_metrics",
    "当前状态(2026Q1)": "current_status_2026_q1",
}

DATA_NOTES_COLUMN_MAP: Final[dict[str, str]] = {
    "字段": "field_name",
    "说明": "note_text",
}

KEY_FINDINGS_SECTION_MARKER: Final = "关键发现"

ORIGINAL_CHINESE_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    "core_brand_metrics": tuple(CORE_BRAND_METRICS_COLUMN_MAP.keys()),
    "ai_strategy_registry": tuple(AI_STRATEGY_REGISTRY_COLUMN_MAP.keys()),
    "data_notes": tuple(DATA_NOTES_COLUMN_MAP.keys()),
    "key_findings": tuple(DATA_NOTES_COLUMN_MAP.keys()),
}

CANONICAL_BRAND_NAME_ALIASES: Final[dict[str, str]] = {
    "arbys": "Arby's",
    "burgerking": "Burger King",
    "chickfila": "Chick-fil-A",
    "chipotle": "Chipotle",
    "culvers": "Culver's",
    "dominos": "Domino's",
    "dunkin": "Dunkin'",
    "dutchbros": "Dutch Bros",
    "fiveguys": "Five Guys",
    "innout": "In-N-Out",
    "jackinthebox": "Jack in the Box",
    "jerseymikes": "Jersey Mike's",
    "jimmyjohns": "Jimmy John's",
    "kfc": "KFC",
    "littlecaesars": "Little Caesars",
    "mcdonalds": "McDonald's",
    "panerabread": "Panera Bread",
    "pandaexpress": "Panda Express",
    "papajohns": "Papa Johns",
    "pizzahut": "Pizza Hut",
    "popeyes": "Popeyes",
    "raisingcanes": "Raising Cane's",
    "shakeshack": "Shake Shack",
    "sonicdrivein": "Sonic Drive-In",
    "starbucks": "Starbucks",
    "subway": "Subway",
    "sweetgreen": "Sweetgreen",
    "tacobell": "Taco Bell",
    "whataburger": "Whataburger",
    "wendys": "Wendy's",
    "wingstop": "Wingstop",
}
