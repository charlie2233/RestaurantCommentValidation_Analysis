# 5-brand happy-path demo walkthrough

This walkthrough is for reviewers and judges who want a fast read on what the
demo shows and where to look in the generated artifacts.

## Run

```bash
qsr-audit demo-happy-path --input data/raw/<workbook>.xlsx --reference-dir data/reference/
qsr-audit package-demo
# or: make demo-bundle
```

## What to inspect

- `reports/demo/index.html` for the entry point and the five-brand overview.
- `reports/validation/core_scorecard.html` for row-level publishability status.
- `reports/reconciliation/brand_deltas.csv` for metric deltas and provenance fields.
- `reports/summary/top_risks.md` for a short list of the highest-signal issues.
- `artifacts/demo_bundle/` for the packaged shareable output set.

## What this proves

- The happy-path five-brand slice can move through the pipeline end to end.
- The demo hub surfaces the important review signals without hiding blocked rows.
- The packaged bundle makes the outputs easy to hand to someone outside the repo.

## What this does not prove

- It does not mean the full workbook is ready for publication.
- It does not cover non-demo brands or unmodeled edge cases.
- It does not replace the underlying validation, reconciliation, or review workflow.
