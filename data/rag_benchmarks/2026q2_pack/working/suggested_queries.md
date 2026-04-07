# Suggested RAG Queries

- Benchmark dir: `/Users/hanfei/RestaurantAnalysis/data/rag_benchmarks/2026q2_pack`
- Suggestions: `12`
- Suggestions are deterministic review candidates only. They are not ground truth and they never overwrite `queries.csv`.
- Final `queries.csv` and `judgments.csv` are intentionally blank in this starter pack.

## Group Counts

- `brand_metric_lookup`: 5
- `provenance_citation`: 5
- `cross_brand_comparison`: 1
- `cross_sheet_audit`: 1

## Recommended First Pass

- Review the five AUV contradiction suggestions first: Starbucks, Taco Bell, Raising Cane's, Dutch Bros, and Shake Shack.
- Then review provenance-gap suggestions for McDonald's, Domino's, Chipotle, and Chick-fil-A.
- Keep the cross-sheet `Sweetgreen` lookup in the pack because it is already grounded in the current validation summary.
- Copy only approved rows into `queries.csv`; leave everything else in `working/`.

## Draft Suggestions

- `starter-starbucks-auv-contradiction`: What current vetted evidence explains Starbucks auv and the implied-AUV mismatch? (Current validation output records a 38.9% implied-AUV contradiction and reconciliation still shows no external coverage.)
- `starter-taco-bell-auv-contradiction`: What current vetted evidence explains Taco Bell auv and the implied-AUV mismatch? (Current validation output records a Taco Bell AUV contradiction and reconciliation still shows no external coverage.)
- `starter-raising-canes-auv-contradiction`: What current vetted evidence explains Raising Cane's auv and the implied-AUV mismatch? (Current validation output records a Raising Cane's AUV contradiction and reconciliation still shows no external coverage.)
- `starter-dutch-bros-auv-contradiction`: What current vetted evidence explains Dutch Bros auv and the implied-AUV mismatch? (Current validation output records a Dutch Bros AUV contradiction and reconciliation still shows no external coverage.)
- `starter-shake-shack-auv-contradiction`: What current vetted evidence explains Shake Shack auv and the implied-AUV mismatch? (Current validation output records a Shake Shack AUV contradiction and reconciliation still shows no external coverage.)
- `starter-mcdonalds-system-sales-gap`: What provenance is currently attached to McDonald's system_sales, and what external evidence is still missing? (McDonald's is a high-visibility anchor brand and still has no external reconciliation coverage for system sales.)
- `starter-mcdonalds-store-count-gap`: What provenance is currently attached to McDonald's store_count, and what external evidence is still missing? (McDonald's store_count is a likely first-cycle external KPI candidate but currently has workbook-only provenance locally.)
- `starter-dominos-system-sales-gap`: What vetted evidence currently exists for Domino's system_sales, and what reference gap remains? (Domino's has no current AUV contradiction but still lacks external system-sales coverage, making it a useful clean-gap benchmark query.)
- `starter-chipotle-store-count-gap`: What vetted evidence currently exists for Chipotle store_count, and what reference gap remains? (Chipotle is a major public brand with no local external store-count evidence yet, so it is a good first-cycle reference-intake target.)
- `starter-chick-fil-a-auv-gap`: What vetted evidence currently exists for Chick-fil-A auv, and what external source type is still needed? (Chick-fil-A is private and likely needs careful manual secondary-source handling, so the query should stay review-heavy and citation-sensitive.)
- `starter-sweetgreen-extra-ai-row`: Which brand appears on the AI strategy sheet but not in the Top 30 core table? (The local validation summary already flags one orphan AI brand and this is a concrete early benchmark case for cross-sheet retrieval.)
- `starter-priority-evidence-gap-scan`: Which high-priority brands still have no external evidence for store_count, system_sales, or auv? (The first analyst cycle needs a compact cross-brand query that surfaces where evidence collection should start, but the exact priority grouping still needs human review.)
