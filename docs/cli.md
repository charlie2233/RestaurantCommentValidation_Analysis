# CLI Reference

## Control artifacts

- Release-relevant commands emit machine-readable manifests under `artifacts/manifests/<command>/`.
- The same commands emit structured audit logs under `artifacts/audit_logs/<command>/`.
- These control artifacts are for lineage, release gating, and handoff debugging. They are not analyst-facing reports.

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

### `qsr-audit preflight-release`

- Purpose: verify that Gold publish artifacts, upstream manifests, and release runbooks are ready for external-facing handoff.
- Gate behavior:
  - fails when required Gold decision artifacts are missing
  - fails when required upstream manifests are missing
  - fails when `publishable_kpis.parquet` or `blocked_kpis.parquet` drift from `gold_publish_decisions.parquet`
  - fails when experimental forecasting or RAG artifacts leak into analyst-facing paths
- Primary outputs:
  - `artifacts/release/preflight_summary.json`
  - `artifacts/release/preflight_summary.md`

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

### `qsr-audit init-rag-benchmark --name <pack-name>`

- Purpose: initialize a local analyst benchmark pack under `data/rag_benchmarks/<pack-name>/`.
- Outputs:
  - `metadata.json`
  - `README.md`
  - `checklist.md`
  - copied CSV templates for `queries.csv`, `judgments.csv`, `filters.csv`, and `query_groups.csv`

### `qsr-audit bootstrap-rag-judgments --benchmark-dir <path>`

- Purpose: run first-pass retrieval and write suggestion-only reviewer work files.
- Guardrails:
  - suggestions are not ground truth
  - does not overwrite the pack-level `judgments.csv`
  - writes working files under `data/rag_benchmarks/<pack>/working/`
- Primary outputs:
  - `working/query_specs.json`
  - `working/candidate_results.parquet`
  - `working/candidate_results.csv`
  - `working/judgment_workspace.csv`
  - `working/bootstrap_manifest.json`

### `qsr-audit seed-rag-queries --benchmark-dir <path>`

- Purpose: generate deterministic analyst-style query suggestions from the current vetted corpus metadata.
- Guardrails:
  - writes only under `data/rag_benchmarks/<pack>/working/`
  - never overwrites `queries.csv`
  - suggestions are review candidates, not ground truth
- Primary outputs:
  - `working/suggested_queries.csv`
  - `working/suggested_queries.md`

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

### `qsr-audit validate-rag-reviewer-file --benchmark-dir <path> --reviewer <name>`

- Purpose: validate one reviewer-specific `reviewers/<name>/judgments.csv` file against the current vetted corpus.
- Validation semantics:
  - reviewer rows must still use valid `query_id`, `doc_id`, and `chunk_id` references
  - missing rationale and malformed `must_appear_in_top_k` values remain validation errors
- Primary outputs:
  - reviewer-scoped validation artifacts under `artifacts/rag/benchmarks/validation/<pack>/reviewer-<name>/`

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
  - prefers `adjudicated_judgments.csv` when present
  - warns when the pack is still `draft` or `in_review`
  - writes only under `artifacts/rag/benchmarks/`
- Primary outputs:
  - `artifacts/rag/benchmarks/metrics.json`
  - `artifacts/rag/benchmarks/metrics.csv`
  - `artifacts/rag/benchmarks/per_query_results.parquet`
  - `artifacts/rag/benchmarks/failure_cases.md`
  - `artifacts/rag/benchmarks/summary.md`
  - `artifacts/rag/benchmarks/query_bucket_metrics.csv`
  - `artifacts/rag/benchmarks/rerank_delta.csv` when reranking is enabled

### `qsr-audit mine-rag-hard-negatives --benchmark-dir <path> --run-dir <path>`

- Purpose: inspect one retrieval benchmark run and propose hard-negative review candidates such as wrong-brand, wrong-metric, and metadata-filter near misses.
- Guardrails:
  - suggestions stay under `data/rag_benchmarks/<pack>/working/`
  - does not change `judgments.csv` or `adjudicated_judgments.csv`
  - suggestions remain human-review inputs, not final labels
- Primary outputs:
  - `data/rag_benchmarks/<pack>/working/hard_negative_suggestions.csv`
  - `artifacts/rag/benchmarks/<run_id>/hard_negative_summary.md`

### `qsr-audit adjudicate-rag-benchmark --benchmark-dir <path>`

- Purpose: compare reviewer judgments, write conflict reports, and emit `adjudicated_judgments.csv` when safe.
- Guardrails:
  - reviewer files are preserved
  - unresolved conflicts block `adjudicated` status unless `--force` is used
  - doc-level judgments remain doc-level; chunk-level judgments remain exact
- Primary outputs:
  - `artifacts/rag/benchmarks/adjudication/<run_id>/conflicts.csv`
  - `artifacts/rag/benchmarks/adjudication/<run_id>/agreement_summary.json`
  - `artifacts/rag/benchmarks/adjudication/<run_id>/agreement_summary.md`
  - `data/rag_benchmarks/<pack>/adjudicated_judgments.csv` when written

### `qsr-audit summarize-rag-failures --benchmark-dir <path> --run-dir <path>`

- Purpose: bucket retrieval benchmark failures into triage categories such as retrieval miss, metadata filter miss, citation/provenance miss, or ambiguity/query-design issue.
- Primary outputs:
  - `artifacts/rag/benchmarks/<run_id>/failure_triage.csv`
  - `artifacts/rag/benchmarks/<run_id>/failure_triage.json`
  - `artifacts/rag/benchmarks/<run_id>/failure_triage.md`

### `qsr-audit summarize-rag-benchmark-authoring --benchmark-dir <path>`

- Purpose: summarize benchmark authoring coverage, unjudged queries, and missing slice coverage.
- Optional run context:
  - pass `--run-dir` to surface dominant failure buckets and outstanding hard-negative review gaps from an existing retrieval run
- Primary outputs:
  - `artifacts/rag/benchmarks/authoring/<pack>/summary.json`
  - `artifacts/rag/benchmarks/authoring/<pack>/summary.md`
  - `artifacts/rag/benchmarks/authoring/<pack>/coverage_rows.csv`

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

## Settings and redaction

- `.env.example` documents the supported path settings without shipping real values.
- `qsr_audit.config.Settings.safe_debug_summary()` is safe to print in CI and local diagnostics because it redacts secret-like environment values.
- `artifacts/` must stay separate from `reports/` and `strategy/`. The settings layer rejects unsafe overlaps early.

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
qsr-audit init-rag-benchmark --name my-pack --author alice
qsr-audit build-rag-corpus
qsr-audit bootstrap-rag-judgments --benchmark-dir data/rag_benchmarks/my-pack
qsr-audit validate-rag-reviewer-file --benchmark-dir data/rag_benchmarks/my-pack --reviewer alice
qsr-audit adjudicate-rag-benchmark --benchmark-dir data/rag_benchmarks/my-pack
qsr-audit eval-rag-retrieval --benchmark-dir data/rag_benchmarks/my-pack --retriever bm25
qsr-audit summarize-rag-benchmark-authoring --benchmark-dir data/rag_benchmarks/my-pack
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
