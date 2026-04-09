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

## Exact reviewer labeling

- Review one query at a time against the vetted corpus and the bootstrap workspace.
- For each positive judgment, fill either `doc_id` or `chunk_id`. Use `chunk_id`
  when the exact chunk matters; use `doc_id` when the whole document is the
  right retrieval target.
- Use only these `relevance_label` values:
  - `highly_relevant`: the row directly answers the lookup and should usually be
    easy to surface near the top.
  - `relevant`: the row contributes useful grounded evidence but is not the best
    direct answer on its own.
  - `not_relevant`: the row is a reviewed hard negative for this query.
- Fill `rationale` with an evidence-backed note about why the row is relevant or
  not relevant. Do not restate the query without tying it to the artifact.
- Fill `must_appear_in_top_k` only when the row is important enough to require a
  top-k placement. Leave it blank when there is no hard placement requirement.
- Keep `review_state` as `draft` until the reviewer has finished the full file.
- Do not backfill `judgments.csv` at the pack root until both reviewer files are
  complete and the disagreements have been adjudicated.

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
