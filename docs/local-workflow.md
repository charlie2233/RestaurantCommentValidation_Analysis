# Local Workflow

## Day-1 setup

```bash
python -m venv .venv
source .venv/bin/activate
make setup
```

## Common developer loop

```bash
make lint
make test
make build-package
make check-hygiene
```

## Pipeline loop

```bash
qsr-audit ingest-workbook --input data/raw/source_workbook.xlsx
qsr-audit validate-workbook --input data/silver --tolerance-auv 0.05
qsr-audit run-syntheticness --input data/silver/core_brand_metrics.parquet
qsr-audit reconcile --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/
qsr-audit report --output reports/
```

Or use:

```bash
make run-full-audit
```

## Where outputs land

- Bronze raw sheet dumps: `data/bronze/`
- Silver normalized tables: `data/silver/`
- Gold validation, syntheticness, reconciliation outputs: `data/gold/`
- Analyst scorecards and brand reports: `reports/`
- Machine-readable strategy outputs: `strategy/`

## Cleanup

```bash
make clean-generated
make clean-caches
```

## Commit policy reminders

- Do not commit raw workbook files or local reference data.
- Do not commit Bronze, Silver, Gold, `reports/`, or `strategy/` outputs.
- Keep workbook claims as hypotheses until Gold validation and reconciliation are complete.
