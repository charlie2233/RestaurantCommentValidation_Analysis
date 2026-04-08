# 2026q2_pack

This pack is for retrieval benchmarking only. It does not create audited facts,
does not authorize answer generation, and should only reference vetted Gold or
provenance-aware local artifacts.

This committed first-cycle batch now contains 16 analyst-style queries. The
queries are intentionally retrieval-only and evidence-aware, and they remain
provisional until two real reviewers complete the benchmark loop.

## Files

- `queries.csv`: the committed first-cycle analyst lookup batch for reviewer labeling.
- `judgments.csv`: pack-level judgments. Leave it empty until the benchmark is
  fully reviewed and adjudicated.
- `filters.csv`: optional extra metadata filters for the filter-sensitive queries.
- `query_groups.csv`: optional grouping for analysis buckets and reviewer triage.
- `metadata.json`: pack status and authoring metadata.
- `checklist.md`: reviewer and authoring checklist.
- `working/suggested_queries.csv`: deterministic draft suggestions generated from
  the current vetted corpus and then curated for this cycle.

## Reviewer workflow

- Treat `queries.csv` as the committed analyst batch for this cycle.
- Keep any reviewer work in `reviewers/alice/judgments.csv` and
  `reviewers/bob/judgments.csv` when those files are created.
- Do not treat bootstrap suggestions as final judgments.
- Leave unknown judgments blank instead of inferring them.
- Keep draft or provisional judgments clearly marked until both reviewers have
  completed their pass and the disagreements have been adjudicated.

## First-cycle focus

The batch covers the high-friction evidence lookups already surfaced by the repo:

- AUV contradiction brands: Starbucks, Taco Bell, Raising Cane's, Dutch Bros,
  and Shake Shack
- provenance and reference gaps for McDonald's, Domino's, Chipotle, and Chick-fil-A
- blocked versus publishable distinctions and the five-brand status split
- the Sweetgreen orphan strategy row from the cross-sheet validation output
- brand, metric, and filter-sensitive lookups over the non-blocked AUV slice

## Commands

```bash
qsr-audit build-rag-corpus
qsr-audit seed-rag-queries --benchmark-dir data/rag_benchmarks/2026q2_pack
qsr-audit bootstrap-rag-judgments --benchmark-dir data/rag_benchmarks/2026q2_pack
qsr-audit validate-rag-reviewer-file --benchmark-dir data/rag_benchmarks/2026q2_pack --reviewer alice
qsr-audit validate-rag-reviewer-file --benchmark-dir data/rag_benchmarks/2026q2_pack --reviewer bob
qsr-audit adjudicate-rag-benchmark --benchmark-dir data/rag_benchmarks/2026q2_pack
qsr-audit eval-rag-retrieval --benchmark-dir data/rag_benchmarks/2026q2_pack --retriever bm25
```

Generated workflow artifacts belong under `artifacts/rag/...`. Source benchmark
pack files stay under `data/rag_benchmarks/...`.

