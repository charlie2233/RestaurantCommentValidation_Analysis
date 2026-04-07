# Monthly Gold Snapshot Checklist

Use this checklist when you want to retain a Gold snapshot for later
forecasting research. A snapshot is only useful after the month's Gold state is
clean enough to archive.

## When To Run `snapshot-gold`

Run `qsr-audit snapshot-gold` once per closed monthly analyst cycle, after all
of the following are true for that period:

1. manual reference CSV updates for the period are done or explicitly deferred
2. `qsr-audit reconcile` has been rerun
3. `qsr-audit gate-gold` has been rerun
4. `qsr-audit preflight-release` passes or the remaining issues are documented
   as non-snapshot blockers

Do not snapshot from a mid-cycle Gold state that has not been reviewed.

## Period Naming

Use the calendar close date in `YYYY-MM-DD` form as the canonical `as_of_date`.

Examples:

- `2026-04-30`
- `2026-05-31`
- `2026-06-30`

Do not use labels like `2026q2`, `apr-2026`, or `latest` as snapshot period
keys.

## Monthly Operating Steps

1. Refresh Gold for the closed period:

```bash
qsr-audit reconcile --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/
qsr-audit gate-gold
qsr-audit preflight-release
```

2. Record any still-open `advisory` or `blocked` KPI follow-ups outside the
   snapshot itself.

3. Create the snapshot with the closed-period date:

```bash
qsr-audit snapshot-gold --as-of-date 2026-04-30
```

4. Only use `--include-advisory` when you are doing an explicitly documented
   research run. Keep it off by default.

## Snapshot Integrity Checks Before Forecasting Use

Verify all of the following before treating the snapshot as forecast-ready
history:

- `data/gold/history/as_of_date=YYYY-MM-DD/forecast_snapshot.parquet` exists
- `data/gold/history/as_of_date=YYYY-MM-DD/gold_publish_decisions.parquet`
  exists
- `data/gold/history/as_of_date=YYYY-MM-DD/publishable_kpis.parquet` exists
- `data/gold/history/as_of_date=YYYY-MM-DD/manifest.json` exists
- `data/gold/history/snapshot_manifest.parquet` contains a row for the same
  `as_of_date`
- the archived snapshot contains only `publishable` rows unless advisory
  inclusion was explicitly chosen and documented
- no `blocked` rows appear in the archived publishable subset
- the snapshot row count is plausible relative to the current
  `publishable_kpis.parquet` count for that same cycle

If any of those checks fail, do not use the snapshot in a forecast panel until
the issue is corrected or explicitly documented.

## What Not To Do

- Do not backfill missing months with guessed values.
- Do not overwrite a month silently without recording why the replacement was
  necessary.
- Do not treat a snapshot as audited fact if the underlying Gold state still has
  unresolved analyst concerns.
- Do not move forecasting outputs into `reports/` or `strategy/`.
