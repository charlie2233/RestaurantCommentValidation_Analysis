# 2026q2_pack Checklist

- [ ] `queries.csv` contains the committed first-cycle batch of 16 retrieval-only queries, with any future additions reviewed explicitly before landing.
- [ ] `judgments.csv` remains empty until real evidence review begins, and any draft reviewer files stay provisional until both reviewers finish.
- [ ] Corpus was built from vetted Gold and provenance-aware local artifacts.
- [ ] No raw workbook, Bronze, or Silver evidence is referenced.
- [ ] `query_id` values are unique and reflect realistic analyst lookups.
- [ ] Ambiguity, citation requirements, and filter-sensitive cases are marked explicitly where needed.
- [ ] Reviewer files live under paths such as `reviewers/alice/judgments.csv`
      and `reviewers/bob/judgments.csv`, and they do not overwrite
      `judgments.csv`.
- [ ] At least two reviewers participate before any pack is treated as truly adjudicated.
- [ ] Reviewer conflicts were adjudicated before treating the benchmark as final.
- [ ] Benchmark outputs are kept under `artifacts/rag/...`, not `reports/` or `strategy/`.
- [ ] Suggestions and hard-negative candidates are treated as reviewer inputs, not ground truth.
