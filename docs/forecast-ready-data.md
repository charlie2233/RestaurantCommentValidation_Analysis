# Forecast-Ready Data

This document defines the minimum data contract for forecasting experiments in
this repo. It is intentionally downstream of Gold gating.

## Core rule

Forecast experiments may only use repeated-period Gold snapshots. A single
cross-sectional workbook snapshot is not enough.

## Required inputs

You need multiple `snapshot-gold` runs across different `as_of_date` values.
Each snapshot should come from a Gold gate state that already exists for that
period.

Minimum practical ingredients:

- repeated `as_of_date` values
- stable `canonical_brand_name`
- stable `metric_name`
- Gold `publish_status`
- provenance and confidence fields

## Snapshot contract

`qsr-audit snapshot-gold --as-of-date YYYY-MM-DD` writes:

- `data/gold/history/as_of_date=YYYY-MM-DD/forecast_snapshot.parquet`
- `data/gold/history/as_of_date=YYYY-MM-DD/gold_publish_decisions.parquet`
- `data/gold/history/as_of_date=YYYY-MM-DD/publishable_kpis.parquet`
- `data/gold/history/as_of_date=YYYY-MM-DD/manifest.json`
- `data/gold/history/snapshot_manifest.parquet`

Default behavior:

- include only `publishable` rows
- exclude `advisory` unless `--include-advisory` is passed
- never include `blocked` rows

## Forecast panel contract

`qsr-audit build-forecast-panel --metric <metric_name>` produces a panel with at
least these fields:

- `as_of_date`
- `brand_name`
- `canonical_brand_name`
- `metric_name`
- `metric_value`
- `publish_status`
- `confidence_score`
- `source_type`
- `source_name`
- `source_url_or_doc_id`
- `method_reported_or_estimated`
- `provenance_completeness_summary`

The panel builder fails clearly on too-short history unless the explicit
test/scaffolding override is used.

## What counts as a forecast-safe target

Safer first targets:

- `store_count`
- `system_sales`
- `auv` only when the Gold gate marks the row publishable and contradictions are
  resolved

Use caution:

- `fte_mid`
- `margin_mid_pct`

Those operational metrics are often estimated and remain advisory-only unless a
future policy explicitly promotes them.

## Why forecasts are not audited facts

Forecasts are model outputs, not reconciled evidence. Even when the target data
comes from Gold snapshots:

- the future is unknown
- baseline skill can be weak on short history
- covariates may be missing
- model quality can drift across brands or periods

Forecast outputs must stay under `artifacts/forecasting/` in this scaffold.
They must not be published as if they were audited KPI facts.
