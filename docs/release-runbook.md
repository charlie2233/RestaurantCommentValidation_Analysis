# Release Runbook

This runbook is for the final external-facing handoff path. It assumes the workbook is still treated as a hypothesis artifact and that only Gold rows marked `publishable` are safe for outward KPI use.

## Minimum release path

```bash
qsr-audit validate-workbook --input data/silver --tolerance-auv 0.05
qsr-audit run-syntheticness --input data/silver/core_brand_metrics.parquet
qsr-audit reconcile --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/
qsr-audit gate-gold
qsr-audit preflight-release
qsr-audit report --output reports/
```

## What `preflight-release` checks

- required Gold decision artifacts exist
- `publishable_kpis.parquet` and `blocked_kpis.parquet` stay consistent with `gold_publish_decisions.parquet`
- required upstream manifests exist
- the Gold gate manifest references the expected upstream lineage artifacts
- required runbooks and controls docs exist
- experimental forecasting and retrieval artifacts have not leaked into analyst-facing paths

## What fails release immediately

- missing Gold decision artifacts
- missing manifests for release-relevant upstream commands
- advisory or blocked rows appearing inside `publishable_kpis.parquet`
- blocked or publishable rows appearing in the wrong subset artifact
- experimental forecasting or RAG outputs under `reports/` or `strategy/`

## What remains internal even after preflight passes

- `gold_publish_decisions.parquet` remains the full internal audit log
- `blocked_kpis.parquet` remains a follow-up queue, not a release artifact
- `advisory` rows remain internal context only
- forecasting outputs under `artifacts/forecasting/` remain experimental
- retrieval benchmark and corpus outputs under `artifacts/rag/` remain experimental

## Release package interpretation

- External KPI sharing should be based on `data/gold/publishable_kpis.parquet`.
- If a metric is only present in `gold_publish_decisions.parquet` as `advisory` or `blocked`, it is not release-safe.
- Use `reports/audit/gold_publish_scorecard.md` and `artifacts/release/preflight_summary.md` together when explaining why a release did or did not proceed.

## Human review that still matters

- Analysts still need to review blocked and advisory causes before deciding whether additional manual evidence should be gathered.
- Analysts still need to judge whether the publishable subset is fit for the actual memo, deck, or external handoff context.
- Preflight is a control layer, not a substitute for domain review.
