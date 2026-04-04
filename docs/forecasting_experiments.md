# Forecasting Experiments

This document defines a cautious evaluation plan for forecasting models. It is
deliberately downstream of the current pipeline: forecasting is not part of the
supported workbook ingestion, validation, reconciliation, or reporting flow.

## Scope

The first forecasting target should be a repeated-period Gold dataset. A single
cross-sectional workbook snapshot is not enough. Until we have longitudinal Gold
series, forecasting work should stop at planning and stub code.

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

## Baselines

Before evaluating any foundation model, compare against:

1. Naive last-value forecast.
2. Seasonal naive forecast when a seasonal cadence exists.
3. Simple rolling average or exponential smoothing baseline.

If Chronos cannot beat these baselines on meaningful held-out periods, it
should not advance.

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

## Failure modes to watch

- Forecasting on too little history.
- Treating workbook-implied metrics as stable targets when they still have
  unresolved Gold warnings.
- Apparent model skill that comes from leakage in time splits.
- Expecting a zero-shot univariate model to absorb promotions, holidays, or
  operational covariates it never saw.
- Strong average performance hiding concentrated misses on operationally
  important brands.

## Exit criteria for a future production proposal

- The best Chronos variant beats the simple baselines on a held-out time split.
- Results are stable across multiple brands, not one narrow subset.
- Runtime is practical for local or CI-scale evaluation.
- Forecast inputs are fully Gold-sourced and provenance-aware.
- A separate hardening change defines how forecasts would be surfaced without
  confusing them with audited facts.
