# RAG Benchmark Authoring

This document explains how analysts should author retrieval benchmark packs for
`qsr-audit`.

## Purpose

The benchmark pack exists to measure retrieval quality over vetted local
artifacts. It does not create a new audited fact layer and it does not justify
answer generation by itself.

## Files

Author benchmark packs with these CSV files:

- `queries.csv`
- `judgments.csv`
- optional `filters.csv`
- optional `query_groups.csv`

Templates live under `data/rag_benchmarks/templates/`.

For a real local pack, start with:

```bash
qsr-audit init-rag-benchmark --name my-pack --author alice
```

That creates:

- `data/rag_benchmarks/my-pack/queries.csv`
- `data/rag_benchmarks/my-pack/judgments.csv`
- `data/rag_benchmarks/my-pack/filters.csv`
- `data/rag_benchmarks/my-pack/query_groups.csv`
- `data/rag_benchmarks/my-pack/metadata.json`
- `data/rag_benchmarks/my-pack/README.md`
- `data/rag_benchmarks/my-pack/checklist.md`

## Query authoring

Each query row should capture one realistic analyst lookup task.

Required fields:

- `query_id`
- `query_text`
- `language`
- `notes`
- `brand_filter`
- `metric_filter`
- `publish_status_scope`
- `expected_source_kinds`
- `ambiguity_flag`
- `requires_citation`

Guidance:

- Use `brand_filter` and `metric_filter` only when the analyst intent is really
  scoped that way.
- Use pipe-separated values for multi-brand or multi-metric queries.
- Mark `ambiguity_flag` when more than one interpretation is plausible.
- Mark `requires_citation` when the analyst would reject an uncited result.

## Judgment authoring

Judgments define which vetted chunks or documents are relevant.

Required fields:

- `query_id`
- `doc_id` or `chunk_id`
- `relevance_label`
- `rationale`
- optional `must_appear_in_top_k`

Relevance labels:

- `not_relevant`
- `relevant`
- `highly_relevant`

Guidance:

- Prefer `chunk_id` when the exact passage matters.
- Use `doc_id` when any chunk from the document is acceptable.
- `chunk_id` judgments are exact. A same-document sibling chunk does not satisfy them.
- `doc_id` judgments are satisfied when any retrieved chunk from that document appears.
- Use `must_appear_in_top_k` sparingly for critical direct-hit expectations.
- For `doc_id` judgments, `must_appear_in_top_k` is satisfied by any chunk from that document
  appearing within the threshold.
- Do not author judgments for raw workbook tabs, Bronze dumps, or Silver tables.

## Filters and query groups

`filters.csv` is for extra metadata filters not already covered by the main query
columns.

`query_groups.csv` is for analyst buckets such as:

- `cross_brand_comparison`
- `provenance_citation`
- `metadata_filter_heavy`

These groups support grouped evaluation summaries. They do not change corpus
policy.

## Reviewer workflow

Use reviewer-specific files instead of clobbering the pack-level `judgments.csv`.

Recommended layout:

- `data/rag_benchmarks/my-pack/reviewers/alice/judgments.csv`
- `data/rag_benchmarks/my-pack/reviewers/bob/judgments.csv`

Bootstrap suggestions first:

```bash
qsr-audit build-rag-corpus
qsr-audit bootstrap-rag-judgments --benchmark-dir data/rag_benchmarks/my-pack --retriever bm25 --top-k 10
```

This writes reviewer-facing working files under `data/rag_benchmarks/my-pack/working/`:

- `query_specs.json`
- `candidate_results.parquet`
- `candidate_results.csv`
- `judgment_workspace.csv`
- `bootstrap_manifest.json`

These are suggestions only. They are not ground truth and they never overwrite
`judgments.csv`.

Reviewer submission workflow:

1. Copy or curate candidate rows into `reviewers/<name>/judgments.csv`.
2. Leave unknowns blank rather than guessing.
3. Keep `chunk_id` vs `doc_id` semantics explicit.
4. Provide rationale for every judgment row.

Validate reviewer files before adjudication:

```bash
qsr-audit validate-rag-reviewer-file --benchmark-dir data/rag_benchmarks/my-pack --reviewer alice
```

## Adjudication

Adjudication compares reviewer files and writes reports under
`artifacts/rag/benchmarks/adjudication/<run_id>/`.

```bash
qsr-audit adjudicate-rag-benchmark --benchmark-dir data/rag_benchmarks/my-pack
```

Outputs:

- `conflicts.csv`
- `agreement_summary.json`
- `agreement_summary.md`
- `data/rag_benchmarks/my-pack/adjudicated_judgments.csv` when conflicts are resolved

Rules:

- reviewer files are never overwritten
- unresolved conflicts keep the pack out of `adjudicated` status unless `--force` is used
- `chunk_id` judgments remain exact
- `doc_id` judgments are satisfied by any chunk from the judged document
- doc-level and chunk-level judgments are not flattened into each other during adjudication

## Validation and evaluation workflow

```bash
qsr-audit init-rag-benchmark --name my-pack --author alice
qsr-audit build-rag-corpus
qsr-audit bootstrap-rag-judgments --benchmark-dir data/rag_benchmarks/my-pack
qsr-audit validate-rag-reviewer-file --benchmark-dir data/rag_benchmarks/my-pack --reviewer alice
qsr-audit adjudicate-rag-benchmark --benchmark-dir data/rag_benchmarks/my-pack
qsr-audit eval-rag-retrieval --benchmark-dir data/rag_benchmarks/my-pack --retriever bm25
qsr-audit inspect-rag-benchmark --benchmark-dir data/rag_benchmarks/my-pack --query-id blocked-kpi
qsr-audit summarize-rag-benchmark-authoring --benchmark-dir data/rag_benchmarks/my-pack
```

Validation failures that come from malformed field values, contradictory rows, or
dangling references are reported in structured validation artifacts under
`artifacts/rag/benchmarks/validation/`. Missing required files or badly shaped CSVs
still fail immediately as hard load errors.

Evaluation prefers `adjudicated_judgments.csv` when present. If only draft or
single-reviewer judgments exist, the benchmark summary stays provisional and
warns that the pack is not yet fully adjudicated.

## When reranking is worth testing

Try reranking only after:

- the benchmark pack validates cleanly
- BM25 or dense retrieval has enough judged queries to measure
- failure cases suggest ordering, not corpus policy or query labeling, is the bottleneck

Reranking remains opt-in, offline-only, and non-analyst-facing in this repo.

## Guardrails

- Retrieval quality must be proven before answer generation is considered.
- Retrieval output is not the same thing as an audited fact.
- Benchmark outputs belong under `artifacts/rag/benchmarks/`, not `reports/` or `strategy/`.
