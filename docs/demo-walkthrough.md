# 5-Brand Happy-Path Demo Walkthrough

This walkthrough is for reviewers, judges, and demo consumers who need to verify
the showcase layer without reading the whole codebase.

## Scope

The demo covers exactly five brands: Starbucks, Taco Bell, Raising Cane's,
Dutch Bros, and Shake Shack. It is a happy-path slice, not a claim that the
entire workbook is ready for external use.

## Run

```bash
qsr-audit package-demo
# or
make demo-bundle
```

The command packages the demo outputs into `artifacts/demo_bundle/` and writes
the screenshot-friendly hub to `reports/demo/index.html`.

## Review Order

1. Open `reports/demo/index.html`.
2. Confirm the five brands listed on the hub match the intended demo slice.
3. Check the publishability summary and verify `publishable`, `advisory`, and
   `blocked` rows remain distinct.
4. Review the reconciliation provenance summary for source coverage and method
   notes.
5. Scan the top invariant failures and the syntheticness review summary for the
   highest-risk items.
6. Open `reports/validation/core_scorecard.html` if you want the full brand-level
   table.
7. Open `reports/reconciliation/brand_deltas.csv` if you want the row-level
   deltas behind the summary.
8. Open `reports/summary/top_risks.md` for the short risk narrative.

## What To Look For

- The hub should make the five-brand scope obvious.
- Publishable rows should stay separate from advisory and blocked rows.
- Reconciliation provenance should be visible enough to explain why a row was
  accepted, downgraded, or blocked.
- Syntheticness review should be presented as review context, not as proof of
  fabrication.

## What Not To Claim

- Do not claim full-workbook validation.
- Do not claim the absence of issues outside the five-brand slice.
- Do not treat advisory or blocked rows as publishable.
- Do not present the bundle as a release preflight replacement.
