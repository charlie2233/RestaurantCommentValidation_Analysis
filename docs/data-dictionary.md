# Data Dictionary

This dictionary describes the primary datasets produced by the pipeline. It is intentionally scoped to analyst and developer use, not raw workbook documentation. Raw workbooks remain hypothesis artifacts.

## Silver datasets

### `data/silver/core_brand_metrics.parquet`

Canonical normalized brand metrics from the core workbook sheet.

| Field | Meaning |
|---|---|
| `rank` | Claimed Top 30 rank from the workbook |
| `brand_name` | Canonical brand display name after normalization |
| `category` | Claimed operating category from the workbook |
| `us_store_count_2024` | Claimed US store count for 2024 |
| `systemwide_revenue_usd_billions_2024` | Claimed system sales in USD billions |
| `average_unit_volume_usd_thousands` | Claimed AUV in USD thousands |
| `store_daily_equivalent_fte_range` | Original FTE range text from the workbook |
| `store_margin_range_pct` | Original margin range text from the workbook |
| `fte_min`, `fte_max`, `fte_mid` | Parsed FTE range values |
| `margin_min_pct`, `margin_max_pct`, `margin_mid_pct` | Parsed margin range values |
| `central_kitchen_supply_chain_model` | Free-text operating model note |
| `ownership_model` | Free-text ownership or franchise model |
| `source_sheet` | Workbook sheet lineage |
| `row_number` | Original row number in the source sheet |

### `data/silver/ai_strategy_registry.parquet`

Normalized AI strategy claims from the workbook AI sheet.

| Field | Meaning |
|---|---|
| `brand_name` | Canonical brand name |
| `ai_strategy_direction` | Claimed AI strategy direction |
| `key_initiatives` | Claimed initiatives or pilots |
| `deployment_scale` | Claimed rollout scale |
| `impact_metrics` | Claimed impact or outcome text |
| `current_status_2026_q1` | Claimed current status snapshot |
| `source_sheet`, `row_number` | Sheet and row lineage |

### `data/silver/data_notes.parquet`

Normalized notes and metadata rows from the workbook notes sheet.

| Field | Meaning |
|---|---|
| `field_name` | Field or concept being described |
| `note_text` | Analyst-facing note text |
| `source_sheet`, `row_number` | Sheet and row lineage |

### `data/silver/key_findings.parquet`

Extracted free-text findings from the notes sheet.

| Field | Meaning |
|---|---|
| `finding_number` | Ordinal finding number when detectable |
| `finding_text` | Finding text preserved from the workbook |
| `source_sheet`, `row_number` | Sheet and row lineage |

## Gold datasets

### `data/gold/validation_flags.parquet`

Structured validation findings across schema checks, invariants, and cross-sheet consistency.

| Field | Meaning |
|---|---|
| `severity` | `error`, `warning`, or `info` |
| `category` | Finding category such as `null`, `allowed_range`, or `arithmetic_invariant` |
| `check_name` | Stable machine-readable check identifier |
| `dataset` | Dataset under validation |
| `message` | Plain-English analyst message |
| `sheet_name`, `field_name`, `brand_name`, `row_number` | Finding lineage and localization |
| `expected`, `observed`, `details` | Optional comparison context |

### `data/gold/syntheticness_signals.parquet`

Weak-to-moderate anomaly signals for numeric pattern review.

| Field | Meaning |
|---|---|
| `signal_type` | Signal family such as Benford, heaping, or outlier |
| `title` | Short analyst-friendly label |
| `plain_english` | Human-readable explanation |
| `strength` | `strong`, `moderate`, `weak`, or `unknown` |
| `dataset`, `field_name`, `method` | Signal source |
| `sample_size`, `score`, `p_value`, `z_score`, `threshold` | Statistical context when relevant |
| `observed`, `expected`, `details` | Observed values and structured metadata |
| `interpretation`, `caveat` | Usage guidance and limitations |

### `data/gold/reconciled_core_metrics.parquet`

Core brand metrics enriched with entity resolution, reconciliation, and credibility fields.

| Field | Meaning |
|---|---|
| `canonical_brand_name` | Resolved canonical brand name after entity matching |
| `brand_match_confidence`, `brand_match_method` | Entity resolution result |
| `reference_source_count`, `reference_source_names` | Count and names of matched reference sources |
| `*_reference_value` | Matched reference value for rank, stores, sales, or AUV |
| `*_absolute_error`, `*_relative_error` | Reconciliation deltas against reference values |
| `*_credibility_grade` | Field-level credibility grade |
| `*_reference_source_name`, `*_reference_source_type`, `*_reference_confidence_score` | Field-level provenance |
| `overall_credibility_grade` | Rollup credibility assessment |
| `reconciliation_warning` | Explicit missing coverage or conflict warning text |

### `data/gold/provenance_registry.parquet`

Source-level provenance registry for reconciled core rows.

| Field | Meaning |
|---|---|
| `source_type` | Source family such as workbook, qsr50, technomic, sec filing |
| `source_name` | Human-readable source label |
| `source_url_or_doc_id` | Manual URL, document ID, or local identifier |
| `as_of_date` | Effective source date |
| `method_reported_or_estimated` | How the figure was produced or described |
| `confidence_score` | Analyst-entered or computed confidence score |
| `notes` | Free-text notes |
| `extra` | JSON metadata such as canonical brand name and lineage |

## Report and strategy outputs

### `reports/index.json`

Machine-readable global and brand scorecard bundle for reporting and the dashboard stub.

### `strategy/recommendations.parquet`

Rules-based strategy recommendations derived from Gold artifacts only.

| Field | Meaning |
|---|---|
| `brand_name`, `canonical_brand_name` | Display and canonical brand identifiers |
| `source_layer`, `source_sheet`, `source_row_number` | Upstream Gold lineage |
| `primary_archetype_code`, `primary_archetype_name` | Deterministic strategy archetype assignment |
| `matched_archetype_codes`, `matched_archetypes` | Additional archetype matches |
| `strategy_readiness` | `hold`, `caution`, or `ready` based on Gold quality gates |
| `priority_rank`, `priority_bucket` | Ordered recommendation position and timing bucket |
| `initiative_code`, `initiative_name` | Stable recommendation identifiers |
| `recommendation_summary`, `rationale`, `guardrail` | Analyst-facing recommendation text |
| `evidence_fields`, `evidence_snapshot` | Gold-derived evidence referenced by the rule |
| `validation_error_count`, `validation_warning_count` | Brand-level validation rollup |
| `brand_synthetic_signal_count` | Count of moderate/strong brand-level syntheticness signals |
| `reference_source_count`, `overall_credibility_grade` | Reconciliation quality context |
| `weakest_provenance_fields`, `largest_reconciliation_errors`, `open_issues` | Analyst debugging fields |
| `plain_english_caveat`, `no_roi_claim` | Interpretation guardrails |
