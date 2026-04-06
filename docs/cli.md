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

### `qsr-audit gate-gold`

- Purpose: apply the explicit Gold publishing policy to reconciled Gold artifacts and decide which KPI rows are publishable, advisory, or blocked.
- Primary outputs:
  - `data/gold/gold_publish_decisions.parquet`
  - `data/gold/publishable_kpis.parquet`
  - `data/gold/blocked_kpis.parquet`
  - `reports/audit/gold_publish_scorecard.md`
  - `reports/audit/gold_publish_scorecard.json`

### `qsr-audit snapshot-gold --as-of-date YYYY-MM-DD`

- Purpose: retain a dated, forecast-ready snapshot of current Gold publish decisions and the safe KPI subset.
- Default behavior:
  - snapshots `publishable` rows only
  - keeps `advisory` rows out unless `--include-advisory` is passed
  - never includes `blocked` rows
- Primary outputs:
  - `data/gold/history/as_of_date=YYYY-MM-DD/forecast_snapshot.parquet`
  - `data/gold/history/as_of_date=YYYY-MM-DD/gold_publish_decisions.parquet`
  - `data/gold/history/as_of_date=YYYY-MM-DD/publishable_kpis.parquet`
  - `data/gold/history/as_of_date=YYYY-MM-DD/manifest.json`
  - `data/gold/history/snapshot_manifest.parquet`

### `qsr-audit build-forecast-panel --metric <metric_name>`

- Purpose: assemble a longitudinal panel from dated Gold snapshots for one target metric.
- Guardrails:
  - fails clearly when history is too short unless `--allow-short-history` is passed
  - excludes `blocked` rows by default
  - keeps outputs out of `reports/` and `strategy/`
- Primary outputs:
  - `artifacts/forecasting/<metric>/panel.parquet`
  - `artifacts/forecasting/<metric>/panel_metadata.json`
  - `artifacts/forecasting/<metric>/panel_summary.md`

### `qsr-audit forecast-baseline --metric <metric_name>`

- Purpose: run leakage-safe offline baselines on a forecast panel.
- Evaluation semantics:
  - multi-step holdouts use a fixed-origin training window, so later holdout actuals never feed earlier predictions back into baseline history
  - seasonal naive runs only when the snapshot dates form a regular calendar cadence such as weekly, month-end, or quarter-end
- Baselines:
  - naive last-value
  - seasonal naive when cadence and `--season-length` support it
  - rolling average
  - exponential smoothing
- Primary outputs:
  - `artifacts/forecasting/<metric>/panel.parquet`
  - `artifacts/forecasting/<metric>/split_metadata.json`
  - `artifacts/forecasting/<metric>/baseline_metrics.json`
  - `artifacts/forecasting/<metric>/baseline_metrics.csv`
  - `artifacts/forecasting/<metric>/baseline_summary.md`

### `qsr-audit build-rag-corpus`

- Purpose: build a retrieval-only corpus from vetted Gold and provenance-aware reviewed artifacts.
- Guardrails:
  - excludes raw workbook, Bronze, and Silver sources by default
  - preserves `publish_status` and provenance metadata on each chunk
  - writes only under `artifacts/rag/corpus/`
- Primary outputs:
  - `artifacts/rag/corpus/corpus.parquet`
  - `artifacts/rag/corpus/corpus.jsonl`
  - `artifacts/rag/corpus/manifest.json`

### `qsr-audit validate-rag-benchmark --benchmark-dir <path>`

- Purpose: validate an analyst-authored benchmark pack against the current
  vetted retrieval corpus.
- Validation catches:
  - duplicate query IDs
  - duplicate or contradictory judgments
  - dangling doc/chunk references
  - invalid relevance labels
  - malformed filters
  - empty packs
- Validation semantics:
  - malformed row values become structured validation issues when the pack can still be loaded
  - `chunk_id` judgments require the exact chunk
  - `doc_id` judgments are satisfied by any retrieved chunk from that document
- Primary outputs:
  - `artifacts/rag/benchmarks/validation/validation_results.json`
  - `artifacts/rag/benchmarks/validation/validation_summary.md`
  - `artifacts/rag/benchmarks/validation/query_specs.json`

### `qsr-audit eval-rag-retrieval`

- Purpose: benchmark retrieval-only baselines over the default smoke fixture or
  an analyst-authored benchmark pack.
- Baselines:
  - BM25 lexical retrieval
  - optional `dense-minilm`
  - optional `dense-bge-small`
  - optional `dense-e5-small` only when explicitly requested
- Optional reranker:
  - `rerank-cross-minilm`
- Guardrails:
  - dense retrieval remains opt-in
  - reranking remains opt-in
  - dense retrieval is skipped in CI
  - reranking is skipped in CI when weights are unavailable
  - writes only under `artifacts/rag/benchmarks/`
- Primary outputs:
  - `artifacts/rag/benchmarks/metrics.json`
  - `artifacts/rag/benchmarks/metrics.csv`
  - `artifacts/rag/benchmarks/per_query_results.parquet`
  - `artifacts/rag/benchmarks/failure_cases.md`
  - `artifacts/rag/benchmarks/summary.md`
  - `artifacts/rag/benchmarks/query_bucket_metrics.csv`
  - `artifacts/rag/benchmarks/rerank_delta.csv` when reranking is enabled

### `qsr-audit inspect-rag-benchmark --query-id <id> --benchmark-dir <path>`

- Purpose: inspect one benchmark query, its active filters, expected relevant
  chunks, retrieved chunks, and a failure diagnosis.
- Guardrails:
  - prints chunks plus metadata only
  - does not synthesize answers
  - does not write under `reports/` or `strategy/`

### `qsr-audit rag-search --query "..."`

- Purpose: return retrieved chunks plus metadata only.
- Guardrails:
  - no answer synthesis
  - no chat layer
  - no analyst-facing output files

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
qsr-audit gate-gold
qsr-audit report --output reports/
```

## Forecast experiment example

```bash
qsr-audit snapshot-gold --as-of-date 2025-01-31
qsr-audit snapshot-gold --as-of-date 2025-02-28
qsr-audit snapshot-gold --as-of-date 2025-03-31
qsr-audit build-forecast-panel --metric system_sales
qsr-audit forecast-baseline --metric system_sales
```

## Retrieval experiment example

```bash
qsr-audit build-rag-corpus
qsr-audit validate-rag-benchmark --benchmark-dir data/rag_benchmarks/my-pack
qsr-audit eval-rag-retrieval --benchmark-dir data/rag_benchmarks/my-pack --retriever bm25
qsr-audit inspect-rag-benchmark --benchmark-dir data/rag_benchmarks/my-pack --query-id blocked-kpi
qsr-audit rag-search --query "Which KPI rows are blocked?" --top-k 5
```

## Notes

- The legacy `ingest` and `validate` commands are placeholders. Use `ingest-workbook` and `validate-workbook`.
- Reference CSVs are manual-first templates. Leave unknowns blank, preserve provenance fields, mark values as `reported` or `estimated`, and do not infer missing data.
- `gate-gold` is the export decision layer. Advisory rows are not publishable, and blocked rows should be treated as unsafe for external use.
- Strategy is downstream-only. It must consume Gold outputs and must not redefine business truth on its own.
- Forecasting commands are experimental and offline-only. Their outputs are not audited facts and must not be surfaced as analyst-facing reports in this scaffold.
- Retrieval commands are experimental and retrieval-only. Retrieved chunks are navigation aids, not audited answers.
- Analyst-authored benchmark packs live under `data/rag_benchmarks/`, but benchmark outputs always belong under `artifacts/rag/benchmarks/`.
