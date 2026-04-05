# CLI Reference

## Core commands

### `qsr-audit ingest-workbook --input <workbook.xlsx>`

- Purpose: copy the workbook into Bronze, dump raw sheets, and write normalized Silver outputs.
- Primary outputs:
  - `data/bronze/workbooks/<workbook>.xlsx`
  - `data/bronze/*.parquet`
  - `data/bronze/*.csv`
  - `data/silver/*.parquet`

### `qsr-audit validate-workbook --input <raw workbook|silver path> --tolerance-auv 0.05`

- Purpose: run schema checks, null/uniqueness/range checks, cross-sheet checks, and arithmetic invariants.
- Exit behavior:
  - `0` when no validation `error` findings are present
  - `1` when blocking validation errors are present
- Primary outputs:
  - `reports/validation/validation_summary.md`
  - `reports/validation/validation_results.json`
  - `data/gold/validation_flags.parquet`

### `qsr-audit run-syntheticness --input <silver core path>`

- Purpose: generate weak-to-moderate anomaly signals such as heaping, nice-number spikes, outliers, and optional Isolation Forest scores.
- Primary outputs:
  - `reports/validation/syntheticness_report.md`
  - `data/gold/syntheticness_signals.parquet`

### `qsr-audit reconcile --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/`

- Purpose: join normalized Silver core metrics to manual reference inputs and emit Gold reconciliation outputs with provenance and coverage audit artifacts.
- Primary outputs:
  - `data/gold/reconciled_core_metrics.parquet`
  - `data/gold/provenance_registry.parquet`
  - `reports/reconciliation/reconciliation_summary.md`
  - `data/gold/reference_coverage.parquet`
  - `reports/reference/reference_coverage.md`

### `qsr-audit audit-reference --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/`

- Purpose: validate manual reference CSVs, audit coverage against the core brands, and emit the coverage artifacts without writing reconciliation outputs.
- Primary outputs:
  - `data/gold/reference_coverage.parquet`
  - `reports/reference/reference_coverage.md`

### `qsr-audit report --output reports/`

- Purpose: build analyst-facing scorecards and downstream Gold-only strategy outputs.
- Primary outputs:
  - `reports/index.md`
  - `reports/index.html`
  - `reports/index.json`
  - `reports/brands/*.md|html|json`
  - `reports/strategy/strategy_playbook.md`
  - `reports/strategy/recommendations.json`
  - `strategy/recommendations.parquet`
  - `strategy/recommendations.json`

## Local end-to-end example

```bash
qsr-audit ingest-workbook --input data/raw/source_workbook.xlsx
qsr-audit validate-workbook --input data/silver --tolerance-auv 0.05
qsr-audit run-syntheticness --input data/silver/core_brand_metrics.parquet
qsr-audit reconcile --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/
qsr-audit report --output reports/
```

## Notes

- The legacy `ingest` and `validate` commands are placeholders. Use `ingest-workbook` and `validate-workbook`.
- Reference CSVs are manual-first templates. Leave unknowns blank, preserve provenance fields, mark values as `reported` or `estimated`, and do not infer missing data.
- Strategy is downstream-only. It must consume Gold outputs and must not redefine business truth on its own.
