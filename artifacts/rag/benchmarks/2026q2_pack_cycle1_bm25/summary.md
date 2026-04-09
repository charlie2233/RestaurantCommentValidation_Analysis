# RAG Retrieval Benchmark Summary

- Corpus chunks: `303`
- Corpus documents: `300`
- Queries: `16`
- Judged queries: `0`
- Top-k: `5`

## Benchmark Status

- Run ID: `2026q2_pack_cycle1_bm25`
- Run status: `provisional`
- Pack status: `in_review`
- Judgments source: `judgments.csv`
- Benchmark is provisional until the pack is adjudicated.
- Warning: This benchmark run is using draft or single-reviewer judgments instead of an adjudicated pack.
- Warning: Benchmark pack status is `in_review`; treat retrieval metrics as provisional.

## Retriever Results

| Run | Status | Recall@k | MRR | nDCG@k | Citation precision | Metadata filter correctness | Latency ms | Index size bytes |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| bm25 | skipped | - | - | - | - | - | - | 114845 |
- `bm25` skipped: No relevance judgments matched the current corpus.

## Query Bucket Results

| Bucket | Run | Query count | Recall@k | MRR | nDCG@k | Citation precision | Metadata filter correctness |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| ambiguous | bm25 | 4 | - | - | - | - | - |
| brand_metric_lookup | bm25 | 5 | - | - | - | - | - |
| brand_specific | bm25 | 14 | - | - | - | - | - |
| cross_brand_comparison | bm25 | 3 | - | - | - | - | - |
| cross_sheet_audit | bm25 | 1 | - | - | - | - | - |
| filter_sensitive_lookup | bm25 | 1 | - | - | - | - | - |
| metadata_filter_heavy | bm25 | 15 | - | - | - | - | - |
| metric_specific | bm25 | 15 | - | - | - | - | - |
| provenance_citation | bm25 | 16 | - | - | - | - | - |
| publishability_boundary | bm25 | 2 | - | - | - | - | - |

## Ambiguous Query Handling

- Ambiguous queries: `4`
- Ambiguous queries with full recall: `0`

## Top Failure Categories

- None.

## Failure Cases

- None.
