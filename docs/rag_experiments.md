# RAG Experiments

This repo now supports a retrieval-only RAG experiment scaffold. It is local,
offline, and intentionally narrower than a production RAG stack.

## Scope

The first experiment is retrieval quality only:

- corpus construction from vetted local artifacts
- BM25 lexical retrieval
- opt-in dense retrieval on small local models
- benchmarked chunk lookup with relevance judgments

Out of scope in this PR:

- answer generation
- chat interfaces
- hosted vector databases
- service deployment
- any path that treats retrieval output as an audited fact

## Retrieval commands

```bash
qsr-audit build-rag-corpus
qsr-audit eval-rag-retrieval --retriever bm25
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

## Benchmark shape

The benchmark harness expects a small JSON fixture of analyst-style questions
plus relevance judgments. Relevance can be authored with explicit chunk IDs or
with stable metadata selectors.

Minimum evaluation metrics:

- Recall@k
- MRR
- nDCG@k
- citation precision
- metadata filter correctness
- latency
- index size

Benchmark outputs live under `artifacts/rag/benchmarks/` as machine-readable
metrics, per-query retrieval results, a concise markdown summary, and failure
case details.

## Relevance judgments

The checked-in default fixture is only a smoke benchmark. A meaningful analyst
benchmark still requires:

- representative analyst questions
- relevance judgments tied to real local artifact coverage
- explicit metadata filters where the query implies them
- reviewed failure cases for low-recall queries

## Guardrails

- Retrieval output is not the same thing as an audited fact.
- Blocked and advisory material may be indexed only when their metadata labels are preserved.
- Retrieval experiments must remain downstream of Gold and provenance-aware reviewed artifacts.
- No answer synthesis should be added until retrieval quality is good enough to justify it.
