# Reference Evidence Backlog

This backlog is the first manual evidence-collection queue for the repo. It is
not evidence itself.

Current local context:

- `reports/reconciliation/reconciliation_summary.md` shows `0` of `30` brands
  with external reference coverage.
- `reports/validation/validation_summary.md` shows five AUV contradiction
  errors: Starbucks, Taco Bell, Raising Cane's, Dutch Bros, and Shake Shack.
- The first analyst cycle should therefore prioritize external-facing KPI
  fields: `store_count`, `system_sales`, and `auv`.

Use this backlog with the committed reference templates under
`data/reference/templates/`. Leave unknown values blank, preserve provenance,
mark `reported` vs `estimated`, and do not infer missing values.

## Top 10 Priority Gaps

| Priority | Brand | Metric | Expected source type | Why now | Current blocker |
|---|---|---|---|---|---|
| P0 | Starbucks | `auv` | `sec_filings_reference` | Local validation shows a 38.9% implied-AUV mismatch and no external coverage. | No populated manual reference row exists locally; current provenance is workbook-only. |
| P0 | Taco Bell | `auv` | `technomic_reference` | Local validation shows a 6.5% implied-AUV mismatch and no external coverage. | No populated brand-level secondary-source row exists locally; workbook claim is unresolved. |
| P0 | Raising Cane's | `auv` | `qsr50_reference` | Local validation shows a 7.9% implied-AUV mismatch and no external coverage. | No populated manual reference row exists locally; AUV contradiction is still unresolved. |
| P0 | Dutch Bros | `auv` | `sec_filings_reference` | Local validation shows a 17.1% implied-AUV mismatch and no external coverage. | No populated manual reference row exists locally; current provenance is workbook-only. |
| P0 | Shake Shack | `auv` | `sec_filings_reference` | Local validation shows an 81.8% implied-AUV mismatch and no external coverage. | No populated manual reference row exists locally; contradiction is severe enough to block external use. |
| P1 | McDonald's | `system_sales` | `sec_filings_reference` | McDonald's is a high-visibility anchor brand and system sales is an external-facing KPI. | No populated manual reference row exists locally; reconciliation still reports missing evidence. |
| P1 | McDonald's | `store_count` | `sec_filings_reference` | Store count is a likely first publishable KPI once external evidence is attached. | No populated manual reference row exists locally; reconciliation still reports missing evidence. |
| P1 | Domino's | `system_sales` | `sec_filings_reference` | Domino's has no current AUV contradiction, so system sales is a good clean-gap evidence target. | No populated manual reference row exists locally; workbook-only provenance remains. |
| P1 | Chipotle | `store_count` | `sec_filings_reference` | Chipotle is a major public chain and store count is a likely forecast-safe target later. | No populated manual reference row exists locally; workbook-only provenance remains. |
| P1 | Chick-fil-A | `auv` | `technomic_reference` | Chick-fil-A is strategically important and AUV is likely to require a manually reviewed secondary source. | No populated manual reference row exists locally; private-brand evidence needs careful manual intake. |

## Operating Notes

- Treat `expected source type` as the next intake lane, not as proof that the
  metric is definitely available there.
- Resolve the P0 contradiction rows before treating those KPIs as credible for
  external use.
- After each filled row, rerun:

```bash
qsr-audit reconcile --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/
qsr-audit gate-gold
qsr-audit preflight-release
```

- Keep this backlog updated as manual reference rows land or as a blocker turns
  out to be a source-availability problem rather than a simple missing row.
