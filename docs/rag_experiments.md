# RAG Experiments

This repo now supports a retrieval-only RAG experiment scaffold. It is local,
offline, and intentionally narrower than a production RAG stack.

## Scope

The first experiment is retrieval quality only:

- corpus construction from vetted local artifacts
- BM25 lexical retrieval
- opt-in dense retrieval on small local models
- analyst-authored benchmarked chunk lookup with relevance judgments
- optional lightweight reranking on the first-pass candidate set

Out of scope in this PR:

- answer generation
- chat interfaces
- hosted vector databases
- service deployment
- any path that treats retrieval output as an audited fact

## Retrieval commands

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

These commands write only under `artifacts/rag/`. They do not write analyst
outputs under `reports/` or `strategy/`.

## Approved corpus sources

The retrieval corpus is built only from vetted local artifacts:

- `data/gold/gold_publish_decisions.parquet`
- `data/gold/publishable_kpis.parquet` or `data/gold/blocked_kpis.parquet` as fallback subsets
- `data/gold/reconciled_core_metrics.parquet`
- `data/gold/reference_coverage.parquet`
- `data/gold/validation_flags.parquet`
- `data/gold/provenance_registry.parquet`
- `reports/validation/validation_summary.md` when present
- optional normalized manual reference notes under `data/reference/manual_reference_notes.{parquet,csv}`

Excluded by default:

- raw workbooks
- Bronze sheet dumps
- Silver fact tables

Why raw workbook files are excluded:

- the workbook is a hypothesis artifact, not a source of truth
- Bronze and Silver are working layers, not reviewed retrieval sources
- retrieval experiments must not bypass Gold validation, reconciliation, or publishing decisions

## Retrieval baselines

Default baseline:

- BM25 lexical retrieval

Opt-in dense comparisons:

- `sentence-transformers/all-MiniLM-L6-v2`
- `BAAI/bge-small-en-v1.5`
- `intfloat/e5-small-v2` remains optional and off by default

Dense retrieval is local-only and safely skipped in CI. Model downloads are not
allowed unless explicitly requested.

## Benchmark pack contract

The default fixture still exists for smoke testing, but meaningful evaluation
should use the committed CSV benchmark-pack contract under
`data/rag_benchmarks/templates/`.

Required files:

- `queries.csv`
- `judgments.csv`

Optional files:

- `filters.csv`
- `query_groups.csv`

The benchmark validator catches:

- duplicate query IDs
- duplicate or contradictory judgments
- dangling `doc_id` or `chunk_id` references
- invalid relevance labels
- malformed filters
- empty benchmark packs
- duplicate query text with conflicting intent metadata

Benchmark validation output lives under `artifacts/rag/benchmarks/validation/`.

Human authoring workflow additions:

- `init-rag-benchmark` creates a local pack scaffold under `data/rag_benchmarks/<pack>/`
- `bootstrap-rag-judgments` writes suggestion-only reviewer work files under `data/rag_benchmarks/<pack>/working/`
- `validate-rag-reviewer-file` checks reviewer submissions under `reviewers/<name>/judgments.csv`
- `adjudicate-rag-benchmark` compares reviewer files and writes conflict reports under `artifacts/rag/benchmarks/adjudication/`
- `summarize-rag-benchmark-authoring` reports benchmark coverage gaps under `artifacts/rag/benchmarks/authoring/`

## Evaluation outputs

Minimum evaluation metrics:

- Recall@k
- MRR
- nDCG@k
- citation precision
- metadata filter correctness
- latency
- index size

Benchmark outputs live under `artifacts/rag/benchmarks/`:

- `metrics.json`
- `metrics.csv`
- `per_query_results.parquet`
- `summary.md`
- `failure_cases.md`
- `query_bucket_metrics.csv`
- `rerank_delta.csv` when reranking is enabled

## Relevance judgments

The checked-in templates are only scaffolding. A meaningful analyst benchmark
still requires:

- representative analyst questions
- relevance judgments tied to real local artifact coverage
- explicit metadata filters where the query implies them
- reviewed failure cases for low-recall queries
- ambiguity flags where multiple interpretations are realistic
- citation requirements for provenance-sensitive lookups

Draft or single-reviewer judgments should not be treated as final benchmark
evidence. `eval-rag-retrieval` prefers `adjudicated_judgments.csv` when present
and warns clearly when the pack is still provisional.

## Reranking

Optional reranking is available only for offline comparison on top of a first
retrieval pass. It is not enabled by default.

Current lightweight reranker:

- `cross-encoder/ms-marco-MiniLM-L6-v2`

Reranking remains local-only, opt-in, and safely skipped in CI when weights are
not available. Use it only after the base retrieval benchmark pack is in place,
because reranking cannot fix a weak or mislabeled benchmark contract.

## Guardrails

- Retrieval output is not the same thing as an audited fact.
- Blocked and advisory material may be indexed only when their metadata labels are preserved.
- Retrieval experiments must remain downstream of Gold and provenance-aware reviewed artifacts.
- No answer synthesis should be added until retrieval quality is good enough to justify it.
