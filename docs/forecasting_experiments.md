# Forecasting Experiments

This document defines a cautious evaluation plan for forecasting models. It is
deliberately downstream of the current pipeline: forecasting is not part of the
supported workbook ingestion, validation, reconciliation, or reporting flow.

## Scope

The first forecasting target should be a repeated-period Gold dataset. A single
cross-sectional workbook snapshot is not enough. Until we have longitudinal Gold
series, forecasting work should stop at offline scaffolding and baseline
evaluation.

## Current scaffold

The repo now supports the non-production bridge work needed before any Chronos
benchmark:

1. `qsr-audit snapshot-gold --as-of-date YYYY-MM-DD`
2. `qsr-audit build-forecast-panel --metric <metric_name>`
3. `qsr-audit forecast-baseline --metric <metric_name>`

These commands write only to `data/gold/history/` and `artifacts/forecasting/`.
They do not write analyst-facing forecast outputs under `reports/` or
`strategy/`.

## Candidate ordering

1. [`amazon/chronos-bolt-small`](https://hf.co/amazon/chronos-bolt-small)
2. [`amazon/chronos-bolt-base`](https://hf.co/amazon/chronos-bolt-base)
3. [`amazon/chronos-t5-small`](https://hf.co/amazon/chronos-t5-small)

Rationale:

- `chronos-bolt-small` is the best first fit for lightweight experimentation.
- `chronos-bolt-base` is the second pass only if `bolt-small` looks promising.
- `chronos-t5-small` gives a stable historical control from the original
  Chronos family.

## Required data before starting

- Gold snapshots across multiple periods for the same brands.
- A stable target definition, such as weekly sales, store growth, or labor
  cost, that already exists in validated and reconciled form.
- Clear train/validation/test time splits with no leakage from future periods.
- Provenance retained for every series so model output can be interpreted in
  context.
- Forecast targets should default to rows that were `publishable` at snapshot
  time. `advisory` rows may be included only as an explicit experimental choice.
- `blocked` rows are not forecast-safe targets.

## What counts as a forecast-safe target

- Repeated-period Gold rows for the same `metric_name` and `canonical_brand_name`
  across multiple `as_of_date` values.
- Metrics that remain publishable after Gold gating, with provenance and
  confidence retained in the panel.
- Targets that are externally reported or strongly evidenced, such as
  `store_count`, `system_sales`, and sometimes `auv` when the Gold gate marks
  them publishable.

Not forecast-safe by default:

- Single one-off workbook snapshots with no prior history.
- `blocked` Gold rows.
- `advisory` rows unless the experiment explicitly opts in and documents why.
- Forecast outputs themselves. They are experimental estimates, not audited
  facts.

## Baselines

Before evaluating any foundation model, compare against:

1. Naive last-value forecast.
2. Seasonal naive forecast when a seasonal cadence exists.
3. Simple rolling average or exponential smoothing baseline.

In this scaffold, multi-step holdouts use a fixed-origin evaluation window. The
full holdout horizon is predicted from the pre-holdout training window only, so
later holdout actuals never leak into subsequent baseline predictions.

If Chronos cannot beat these baselines on meaningful held-out periods, it
should not advance.

If cadence or sample depth does not support a given baseline, the experiment
summary should say so explicitly instead of inventing a score.
Regular cadence is calendar-aware: common month-end, quarter-end, and weekly
series count as regular even when day gaps vary across calendar boundaries.

## Evaluation plan

Measure:

- WQL when quantile forecasts are available.
- MASE and WAPE or sMAPE for business readability.
- RMSE for sensitivity to large misses.
- Interval coverage at P50 and P90 when supported.
- Forecast bias by brand and by archetype.
- Runtime and memory on a normal developer laptop.
- Error concentration in brands with weaker provenance or unresolved
  reconciliation warnings.

Slice results by:

- Brand
- Segment or archetype
- High-confidence vs. low-confidence provenance
- Stable vs. high-volatility series

The offline scaffold writes machine-readable experiment outputs such as:

- `artifacts/forecasting/<metric>/panel.parquet`
- `artifacts/forecasting/<metric>/panel_metadata.json`
- `artifacts/forecasting/<metric>/panel_summary.md`
- `artifacts/forecasting/<metric>/split_metadata.json`
- `artifacts/forecasting/<metric>/baseline_metrics.json`
- `artifacts/forecasting/<metric>/baseline_metrics.csv`
- `artifacts/forecasting/<metric>/baseline_summary.md`

## Failure modes to watch

- Forecasting on too little history.
- Treating workbook-implied metrics as stable targets when they still have
  unresolved Gold warnings.
- Apparent model skill that comes from leakage in time splits.
- Expecting a zero-shot univariate model to absorb promotions, holidays, or
  operational covariates it never saw.
- Strong average performance hiding concentrated misses on operationally
  important brands.
- Confusing forecast outputs with audited facts in analyst-facing materials.

## Chronos posture in this scaffold

- Chronos remains opt-in and offline-only here.
- The scaffold includes a lightweight readiness guard, not a supported model
  serving path.
- CI must not auto-download Chronos weights.
- A real Chronos benchmark still requires repeated-period Gold history on
  actual QSR data, not synthetic fixtures alone.

## Exit criteria for a future production proposal

- The best Chronos variant beats the simple baselines on a held-out time split.
- Results are stable across multiple brands, not one narrow subset.
- Runtime is practical for local or CI-scale evaluation.
- Forecast inputs are fully Gold-sourced and provenance-aware.
- A separate hardening change defines how forecasts would be surfaced without
  confusing them with audited facts.
