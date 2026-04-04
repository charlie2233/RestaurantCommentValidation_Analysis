# Analyst Runbook

This runbook is the operational guide for using the repository as an analyst. It assumes the workbook is a hypothesis artifact and that only Gold outputs are safe for downstream interpretation.

## Core rule

- Do not use raw workbook values directly in memos, strategy decks, or recommendations.
- Do not use Bronze or Silver artifacts as reporting inputs.
- Use Gold outputs, validation summaries, reconciliation summaries, and strategy outputs together.

## Standard analyst workflow

```bash
qsr-audit ingest-workbook --input data/raw/source_workbook.xlsx
qsr-audit validate-workbook --input data/silver --tolerance-auv 0.05
qsr-audit run-syntheticness --input data/silver/core_brand_metrics.parquet
qsr-audit reconcile --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/
qsr-audit report --output reports/
```

## What each stage is for

### 1. Ingest

- Creates an untouched workbook copy in the raw/Bronze path.
- Produces raw sheet dumps in Bronze for inspection.
- Produces normalized Silver datasets with lineage columns.

### 2. Validate

- Catches schema issues, nulls, uniqueness failures, range problems, and arithmetic contradictions.
- Writes findings to `reports/validation/validation_summary.md`, `reports/validation/validation_results.json`, and `data/gold/validation_flags.parquet`.
- Any `error` means the corresponding brand should not be treated as execution-ready.

### 3. Syntheticness review

- Produces anomaly signals that are useful for triage, not proof of fabrication.
- Use this only as supporting evidence for analyst review.
- Strong or moderate signals should prompt manual scrutiny, not accusations.

### 4. Reconcile

- Compares workbook claims to manual reference templates.
- Missing coverage should be treated as missing evidence, not confirmation.
- Field-level credibility grades matter more than a single rollup grade.

### 5. Report

- Generates executive-facing scorecards and brand-level debugging outputs in Markdown, HTML, and JSON.
- Also produces strategy recommendations as a downstream Gold consumer.

## How to read the outputs

### Validation outputs

- `error`: hard contradiction or schema failure that blocks confident downstream use.
- `warning`: issue worth review, but not necessarily blocking.
- `info`: non-blocking context.

### Reconciliation outputs

- Start with `overall_credibility_grade`, then inspect field-level grades.
- Review `*_relative_error` and `reconciliation_warning` for the largest conflicts.
- Check `provenance_registry.parquet` when source coverage or method matters.

### Syntheticness outputs

- Benford can be skipped or caveated for small or bounded samples.
- Heaping and nice-number checks are useful for spotting suspiciously neat values.
- Outliers can reflect real business differences, not just data problems.

### Strategy outputs

- `strategy/recommendations.parquet` and `reports/strategy/strategy_playbook.md` are interpretation layers.
- They never redefine source-of-truth metrics.
- `strategy_readiness = hold` means validation contradictions are unresolved.
- `strategy_readiness = caution` means reference coverage or reconciliation credibility is weak.

## Recommended analyst review order

1. Read `reports/validation/validation_summary.md`.
2. Read `reports/reconciliation/reconciliation_summary.md`.
3. Check `reports/index.md` for portfolio scorecards.
4. Open specific `reports/brands/*.md` files for brand-level debugging.
5. Read `reports/strategy/strategy_playbook.md` only after the earlier steps.

## Escalation guidance

- Escalate validation `error` findings before using a brand in a strategic recommendation.
- Escalate reconciliation gaps when `reference_source_count == 0` or credibility is `MISSING`, `D`, or `F`.
- Escalate strategy recommendations only as hypotheses when the underlying Gold credibility is weak.
