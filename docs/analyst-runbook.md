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
qsr-audit gate-gold
qsr-audit preflight-release
qsr-audit report --output reports/
```

## First Real Analyst Cycle

The main bottleneck is now human evidence collection, not missing framework
code.

Start here:

1. Work the top evidence queue in [reference-evidence-backlog.md](/Users/hanfei/RestaurantAnalysis/docs/reference-evidence-backlog.md).
2. Review the committed starter benchmark pack in [data/rag_benchmarks/2026q2_pack/README.md](/Users/hanfei/RestaurantAnalysis/data/rag_benchmarks/2026q2_pack/README.md).
3. Keep `queries.csv` and `judgments.csv` blank until humans approve real
   benchmark tasks and labels.
4. Use two reviewers from the start for benchmark judgments.
5. Snapshot Gold monthly only after the checklist in
   [monthly-gold-snapshot-checklist.md](/Users/hanfei/RestaurantAnalysis/docs/monthly-gold-snapshot-checklist.md)
   is satisfied.

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
  - `qsr-audit audit-reference --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/` is available when you only want to inspect manual reference coverage.
  - Fill reference CSVs manually: leave unknown values blank, preserve the original source in the provenance columns, mark `method_reported_or_estimated` as `reported` or `estimated`, and do not infer missing values.
  - If a field is unknown, leave it blank instead of inventing a placeholder value.
  - Partial CSV rows are acceptable only when the missing cells are genuinely unknown. Empty coverage is explicit and should not be read as confirmation.

### 5. Gate Gold

- Applies the explicit Gold publishing policy to decide which KPI rows are publishable, advisory, or blocked.
- Writes `data/gold/gold_publish_decisions.parquet`, `data/gold/publishable_kpis.parquet`, `data/gold/blocked_kpis.parquet`, and `reports/audit/gold_publish_scorecard.md`.
- Treat `publishable` as the only status safe for external KPI export.
- Treat `advisory` as analyst context only. It is not safe for external export and should never be promoted silently.
- Treat `blocked` as unsafe until the specific blocking evidence is resolved.

### 6. Report

- Generates executive-facing scorecards and brand-level debugging outputs in Markdown, HTML, and JSON.
- Also produces strategy recommendations as a downstream Gold consumer.

### 7. Release preflight

- Verifies the release package before external handoff.
- Fails when required Gold decision artifacts or upstream manifests are missing.
- Fails when publishable and blocked subsets drift away from `gold_publish_decisions.parquet`.
- Fails when experimental forecasting or retrieval artifacts leak into analyst-facing paths.
- Writes `artifacts/release/preflight_summary.json` and `artifacts/release/preflight_summary.md`.

## Lineage and control artifacts

- `artifacts/manifests/<command>/latest.json` is the latest lineage manifest for a release-relevant command.
- `artifacts/audit_logs/<command>/` stores structured start/end status logs for CLI runs.
- These files are internal controls. They help engineering and release owners trace inputs, outputs, counts, hashes, and upstream dependencies.
- Secret-like environment values are intentionally redacted from safe debug output. Do not paste raw tokens into notes, runbooks, or issue comments.

## Forecast-readiness workflow

Forecasting remains experimental in this repo. Use it only after Gold gating and
only as an offline research path.

```bash
qsr-audit snapshot-gold --as-of-date 2025-01-31
qsr-audit snapshot-gold --as-of-date 2025-02-28
qsr-audit snapshot-gold --as-of-date 2025-03-31
qsr-audit build-forecast-panel --metric system_sales
qsr-audit forecast-baseline --metric system_sales
```

Rules:

- Snapshot after Gold gating, not before.
- Default to `publishable` rows only.
- Include `advisory` rows only as an explicit experimental choice.
- Never treat forecast outputs as audited facts.
- Forecast artifacts belong under `artifacts/forecasting/`, not `reports/` or
  `strategy/`.
- Follow the operating checklist in
  [monthly-gold-snapshot-checklist.md](/Users/hanfei/RestaurantAnalysis/docs/monthly-gold-snapshot-checklist.md)
  before treating a month as forecast-ready history.

## Retrieval experiment workflow

Retrieval experiments are also offline-only. Use them to benchmark lookup quality
over vetted local artifacts, not to generate analyst-facing answers.

```bash
qsr-audit init-rag-benchmark --name my-pack --author alice
qsr-audit build-rag-corpus
qsr-audit seed-rag-queries --benchmark-dir data/rag_benchmarks/my-pack
qsr-audit bootstrap-rag-judgments --benchmark-dir data/rag_benchmarks/my-pack
qsr-audit validate-rag-reviewer-file --benchmark-dir data/rag_benchmarks/my-pack --reviewer alice
qsr-audit adjudicate-rag-benchmark --benchmark-dir data/rag_benchmarks/my-pack
qsr-audit eval-rag-retrieval --benchmark-dir data/rag_benchmarks/my-pack --retriever bm25
qsr-audit mine-rag-hard-negatives --benchmark-dir data/rag_benchmarks/my-pack --run-dir artifacts/rag/benchmarks/<run_id>
qsr-audit summarize-rag-failures --benchmark-dir data/rag_benchmarks/my-pack --run-dir artifacts/rag/benchmarks/<run_id>
qsr-audit summarize-rag-benchmark-authoring --benchmark-dir data/rag_benchmarks/my-pack --run-dir artifacts/rag/benchmarks/<run_id>
qsr-audit inspect-rag-benchmark --benchmark-dir data/rag_benchmarks/my-pack --query-id blocked-kpi
qsr-audit rag-search --query "Which KPI rows are blocked?" --top-k 5
```

Rules:

- The corpus must stay restricted to Gold outputs, provenance-aware reviewed artifacts, and optional manual reference notes under `data/reference/`.
- Raw workbook files, Bronze, and Silver are excluded by default.
- Retrieved chunks are not audited facts on their own.
- Retrieval artifacts belong under `artifacts/rag/`, not `reports/` or `strategy/`.
- Experimental forecasting and retrieval artifacts are not release-safe facts, even when they are useful internally.
- Dense retrieval is opt-in and may be skipped when weights are unavailable or CI is running.
- Initialize benchmark packs under `data/rag_benchmarks/` and keep analyst source files there.
- `seed-rag-queries` writes deterministic metadata-driven suggestions to `working/`; review them manually before copying anything into `queries.csv`.
- Use `working/judgment_workspace.csv` as a suggestion workspace only. It is not a final judgment file.
- `working/hard_negative_suggestions.csv` is also suggestion-only. It should never be treated as final `not_relevant` evidence without human review.
- Keep reviewer files under `reviewers/<name>/judgments.csv`.
- Leave unknowns blank, label ambiguity explicitly, and do not invent judgments for evidence that is not in the vetted corpus.
- Adjudicate reviewer conflicts before treating benchmark metrics as stable evidence.
- Use reranking only after the benchmark pack is valid and the first-pass retriever quality is measurable.
- Use failure triage after each real run to decide whether the next benchmark fix belongs in retrieval, reranking, filters, provenance coverage, or the benchmark labels themselves.
- The committed starter pack under [data/rag_benchmarks/2026q2_pack](/Users/hanfei/RestaurantAnalysis/data/rag_benchmarks/2026q2_pack)
  includes draft working suggestions only. It does not include approved final
  queries or judgments.

## What Humans Need To Do Next

- fill manual reference rows for the priority KPI gaps instead of waiting for
  more framework work
- review `working/suggested_queries.csv` and copy only approved tasks into
  `queries.csv`
- create reviewer-specific judgment files and resolve disagreements before
  treating retrieval metrics as evidence
- decide whether a monthly Gold state is clean enough to snapshot for
  forecasting research

## What Codex Can Help With

- run the ingest, validate, reconcile, gate, report, and preflight commands
- summarize which brands or metrics remain blocked by missing evidence
- prepare or refresh benchmark working files, reviewer validation runs, and
  adjudication reports
- draft CSV rows, checklists, and backlog updates without claiming those drafts
  are final evidence

## What Still Cannot Be Automated Safely

- inventing missing reference values or source excerpts
- deciding that a private-brand metric is authoritative without human source
  review
- resolving ambiguous benchmark query intent or final relevance labels without
  reviewers
- promoting `advisory` or `blocked` KPI rows into external-facing facts
- presenting forecast or retrieval experiment outputs as audited truth

## How to read the outputs

### Validation outputs

- `error`: hard contradiction or schema failure that blocks confident downstream use.
- `warning`: issue worth review, but not necessarily blocking.
- `info`: non-blocking context.

### Reconciliation outputs

- Start with `overall_credibility_grade`, then inspect field-level grades.
- Review `*_relative_error` and `reconciliation_warning` for the largest conflicts.
- Check `provenance_registry.parquet` when source coverage or method matters.

### Gold publishing outputs

- `publishable_kpis.parquet` is the safe subset for external-facing KPI export under the current policy.
- `gold_publish_decisions.parquet` is the full audit log. Use it when you need to see why a row was downgraded or blocked.
- `blocked_kpis.parquet` is the immediate queue for analyst follow-up.
- If a metric is `advisory`, it stays out of published exports even when the value looks plausible.
- Missing provenance, unresolved AUV contradictions, and weak or contradictory reconciliation evidence are the main reasons external metrics get blocked.

### Forecast-readiness outputs

- `data/gold/history/` stores repeated-period Gold snapshots for future model evaluation.
- `artifacts/forecasting/<metric>/panel.parquet` is a research-only longitudinal panel assembled from those snapshots.
- `artifacts/forecasting/<metric>/baseline_summary.md` is an offline baseline evaluation summary, not an analyst-facing report.
- Forecast panels should default to rows that were `publishable` at snapshot time.
- `advisory` rows remain excluded by default and should not be mixed into facts silently.

### Retrieval experiment outputs

- `artifacts/rag/corpus/` holds the retrieval corpus parquet, JSONL, and manifest.
- `artifacts/rag/benchmarks/validation/` holds benchmark-pack validation output.
- `artifacts/rag/benchmarks/adjudication/` holds reviewer agreement and conflict reports.
- `artifacts/rag/benchmarks/authoring/` holds benchmark coverage summaries.
- `artifacts/rag/benchmarks/` holds benchmark metrics, per-query retrieval results, failure cases, bucket metrics, failure triage, hard-negative summaries, rerank deltas, and summary markdown.
- `rag-search` returns chunks plus metadata only. It does not synthesize answers.
- If a retrieved chunk is `blocked` or `advisory`, that status is context, not clearance to use it externally.
- `eval-rag-retrieval` prefers `adjudicated_judgments.csv` when present and warns when the pack is still `draft` or `in_review`.
- `summarize-rag-benchmark-authoring --run-dir ...` is the quickest way to see under-covered slices, dominant failure buckets, and whether hard-negative review is still missing.

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
4. Read `reports/audit/gold_publish_scorecard.md`.
5. Open specific `reports/brands/*.md` files for brand-level debugging.
6. Read `reports/strategy/strategy_playbook.md` only after the earlier steps.
7. Use forecast experiment artifacts only for offline method comparison, never as proof of future business performance.
8. Use retrieval experiment artifacts only for lookup benchmarking, never as analyst-facing evidence by themselves.

## Escalation guidance

- Escalate validation `error` findings before using a brand in a strategic recommendation.
- Escalate reconciliation gaps when `reference_source_count == 0` or credibility is `MISSING`, `D`, or `F`.
- Escalate any KPI row that stays `advisory` or `blocked` in the Gold publishing gate before using it externally.
- Escalate strategy recommendations only as hypotheses when the underlying Gold credibility is weak.
