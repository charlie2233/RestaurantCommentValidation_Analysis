# 2026q2_pack

This pack is for retrieval benchmarking only. It does not create audited facts,
does not authorize answer generation, and should only reference vetted Gold or
provenance-aware local artifacts.

This committed starter pack is for the first real analyst cycle. The final
`queries.csv` and `judgments.csv` files are intentionally left blank so that
reviewers do not mistake scaffolding for approved benchmark evidence.

## Files

- `queries.csv`: analyst-authored lookup tasks.
- `judgments.csv`: final pack-level judgments when they exist.
- `filters.csv`: optional extra metadata filters.
- `query_groups.csv`: optional grouping for analysis buckets.
- `metadata.json`: pack status and authoring metadata.
- `checklist.md`: authoring and review checklist.
- `working/suggested_queries.csv`: deterministic draft suggestions generated from
  the current vetted corpus and then curated for this cycle.
- `reviewers/alice/judgments.csv` and `reviewers/bob/judgments.csv`: reviewer
  placeholders for the first adjudication pass.

## Reviewer workflow

- Review `working/suggested_queries.csv` and copy only approved rows into
  `queries.csv`.
- Create reviewer files under `reviewers/<name>/judgments.csv`.
- Use `working/judgment_workspace.csv` as a suggestion workspace only.
- Do not treat bootstrap suggestions as final judgments.
- Leave unknown judgments blank instead of inferring them.

## First-cycle focus

Start with the high-friction evidence lookups already surfaced by the repo:

- AUV contradiction brands: Starbucks, Taco Bell, Raising Cane's, Dutch Bros,
  and Shake Shack
- provenance and reference gaps for McDonald's, Domino's, and Chipotle
- cross-sheet validation questions such as the orphan AI brand `Sweetgreen`

## Commands

```bash
qsr-audit build-rag-corpus
qsr-audit seed-rag-queries --benchmark-dir data/rag_benchmarks/2026q2_pack
qsr-audit bootstrap-rag-judgments --benchmark-dir data/rag_benchmarks/2026q2_pack
qsr-audit validate-rag-reviewer-file --benchmark-dir data/rag_benchmarks/2026q2_pack --reviewer alice
qsr-audit adjudicate-rag-benchmark --benchmark-dir data/rag_benchmarks/2026q2_pack
qsr-audit eval-rag-retrieval --benchmark-dir data/rag_benchmarks/2026q2_pack --retriever bm25
```

Generated workflow artifacts belong under `artifacts/rag/...`. Source benchmark pack
files stay under `data/rag_benchmarks/...`.
