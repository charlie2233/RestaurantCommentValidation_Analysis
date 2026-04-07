# 2026q2_pack Checklist

- [ ] `queries.csv` remains empty until a human explicitly approves candidate rows from `working/suggested_queries.csv`.
- [ ] `judgments.csv` remains empty until real evidence review begins.
- [ ] Corpus was built from vetted Gold and provenance-aware local artifacts.
- [ ] No raw workbook, Bronze, or Silver evidence is referenced.
- [ ] `query_id` values are unique and reflect realistic analyst lookups.
- [ ] Ambiguity and citation requirements are marked explicitly where needed.
- [ ] Reviewer files live under paths such as `reviewers/alice/judgments.csv`
      and `reviewers/bob/judgments.csv`, and they do not overwrite
      `judgments.csv`.
- [ ] At least two reviewers participate before any pack is treated as truly adjudicated.
- [ ] Reviewer conflicts were adjudicated before treating the benchmark as final.
- [ ] Benchmark outputs are kept under `artifacts/rag/...`, not `reports/` or `strategy/`.
- [ ] Suggestions and hard-negative candidates are treated as reviewer inputs, not ground truth.
