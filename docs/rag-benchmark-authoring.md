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

## Validation and evaluation workflow

```bash
qsr-audit build-rag-corpus
qsr-audit validate-rag-benchmark --benchmark-dir data/rag_benchmarks/my-pack
qsr-audit eval-rag-retrieval --benchmark-dir data/rag_benchmarks/my-pack --retriever bm25
qsr-audit inspect-rag-benchmark --benchmark-dir data/rag_benchmarks/my-pack --query-id blocked-kpi
```

Validation failures that come from malformed field values, contradictory rows, or
dangling references are reported in structured validation artifacts under
`artifacts/rag/benchmarks/validation/`. Missing required files or badly shaped CSVs
still fail immediately as hard load errors.

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
