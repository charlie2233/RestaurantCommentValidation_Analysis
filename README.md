# qsr-audit-pipeline

A Python pipeline for auditing Quick-Service Restaurant (QSR) operational workbooks,
enforcing data quality through a Bronze → Silver → Gold medallion architecture.

---

## Purpose

QSR operators routinely export operational data (sales, labour, waste, variance) into
Excel workbooks. These workbooks are *hypothesis artifacts*: they represent someone's
best guess at what happened, assembled from multiple sources with manual intervention.
This pipeline treats every raw workbook as an **untrusted hypothesis artifact**, not a
source of truth, and systematically
validates, normalises, and reconciles it before any analysis is performed.

## Why the workbook is treated as a hypothesis artifact

| Concern | Detail |
|---|---|
| Manual assembly | Cells may be hand-typed, formulae broken, or rows hidden |
| Schema drift | Column names and positions change between export versions |
| Partial data | Missing periods, filtered rows, or cut-off dates |
| Conflation | Multiple data sources merged without provenance tracking |

By treating the workbook as a *claim to be verified* rather than a *truth to be read*,
the pipeline surfaces discrepancies explicitly and halts promotion of bad data.

---

## Bronze / Silver / Gold flow

```
data/raw/          ← original workbooks, untouched
    │
    ▼  qsr-audit ingest-workbook --input <source>
data/bronze/       ← raw data parsed into Parquet, schema-tagged, provenance recorded
    │
    ▼  qsr-audit validate-workbook --input <raw workbook or silver path>
data/silver/       ← cleaned, normalised, de-duplicated, type-cast
    │
    ▼  qsr-audit validate-workbook --input data/silver --tolerance-auv 0.05
data/gold/         ← reconciled, aggregated, ready for reporting
    │
    ▼  qsr-audit report --output reports/
reports/           ← Audit scorecards plus Gold-only strategy playbooks
```

Only Gold-layer outputs are safe for downstream reporting and strategy. Bronze and
Silver are working layers for parsing, normalising, validating, and reconciling the
workbook's claims.

---

## Quick start

```bash
# 1. Install (editable + dev deps)
make setup

# 2. Ingest a workbook
qsr-audit ingest-workbook --input data/raw/source_workbook.xlsx

# 3. Validate the Silver layer
qsr-audit validate-workbook --input data/silver --tolerance-auv 0.05

# 4. Run syntheticness diagnostics on normalized core metrics
qsr-audit run-syntheticness --input data/silver/core_brand_metrics.parquet

# 5. Reconcile against manual reference inputs
qsr-audit reconcile --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/

# 6. Generate audit and strategy reports from Gold outputs
qsr-audit report --output reports/
```

Or use the Makefile shortcuts:

```bash
make run-ingest
make run-validate
make run-reconcile
make run-report
make run-full-audit
```

## Documentation

- Contributor workflow: [`CONTRIBUTING.md`](CONTRIBUTING.md)
- Analyst workflow and interpretation guide: [`docs/analyst-runbook.md`](docs/analyst-runbook.md)
- Dataset and field definitions: [`docs/data-dictionary.md`](docs/data-dictionary.md)
- CLI reference: [`docs/cli.md`](docs/cli.md)
- Local developer workflow: [`docs/local-workflow.md`](docs/local-workflow.md)

## Manual Reference Inputs

Reference ingestion is intentionally manual-first. Populate local CSVs in `data/reference/`
using the committed templates in `data/reference/templates/` rather than scraping unstable
or paid sources automatically. Missing reference coverage is surfaced explicitly during
reconciliation; it is never treated as silent confirmation.

## Next implementation steps

1. `ingest` -> load the workbook into Bronze with provenance and source metadata.
2. `normalize` -> standardise column names, types, and business definitions into Silver.
3. `validate` -> enforce layer-specific schemas and data quality checks.
4. `reconcile` -> compare claims across sources and promote trusted outputs into Gold.
5. `report` -> generate audit scorecards and downstream strategy playbooks from Gold only. The rules-based strategy outputs land in `strategy/` and `reports/strategy/` and should be read alongside validation and reconciliation summaries.

---

## Configuration

Settings are read from environment variables (prefix `QSR_`) or a `.env` file:

| Variable | Default | Description |
|---|---|---|
| `QSR_DATA_RAW` | `data/raw` | Raw input directory |
| `QSR_DATA_BRONZE` | `data/bronze` | Bronze layer directory |
| `QSR_DATA_SILVER` | `data/silver` | Silver layer directory |
| `QSR_DATA_GOLD` | `data/gold` | Gold layer directory |
| `QSR_REPORTS_DIR` | `reports` | Report output directory |
| `QSR_STRATEGY_DIR` | `strategy` | Strategy recommendation output directory |
| `QSR_LOG_LEVEL` | `INFO` | Logging verbosity |

---

## Development

```bash
# Lint
make lint

# Tests
make test

# Packaging smoke test
make build-package

# Repository hygiene check
make check-hygiene

# Clean ignored artifacts and caches
make clean-generated
make clean-caches

# Lint + tests in one shot (CI parity)
pre-commit run --all-files && pytest --cov=src --cov-report=term-missing --cov-fail-under=85
```

---

## Implementation roadmap

| # | Module | Status | Description |
|---|---|---|---|
| 1 | `ingest` | 🔲 Stub | Parse Excel/CSV into Bronze Parquet |
| 2 | `normalize` | 🔲 Stub | Standardise schemas, types, column names |
| 3 | `validate` | 🔲 Stub | Pandera schema checks per layer |
| 4 | `reconcile` | 🔲 Stub | Cross-source reconciliation & variance flagging |
| 5 | `reporting` | 🔲 Stub | Jinja2-templated HTML/PDF/Excel reports |
| 6 | `strategy` | 🟡 Rules-based | Gold-only strategy recommendations and archetype playbooks |
| 7 | `dashboard` | 🔲 Stub | Interactive audit dashboard (Streamlit/Dash) |
| 8 | DVC pipeline | 🔲 Stub | Reproducible data pipeline via DVC |
| 9 | MLflow tracking | 🔲 Stub | Experiment tracking for model-based checks |

---

## Repository layout

```
qsr-audit-pipeline/
├── data/
│   ├── raw/           # Original workbooks (not committed)
│   ├── bronze/        # Parsed Parquet (not committed)
│   ├── silver/        # Normalised Parquet (not committed)
│   ├── gold/          # Reconciled Parquet (not committed)
│   └── reference/     # Local reference/lookup tables (not committed)
├── src/qsr_audit/     # Main package
│   ├── cli.py         # Typer CLI entrypoint
│   ├── config.py      # Pydantic-settings configuration
│   ├── contracts/     # Future data contracts / schemas
│   ├── ingest/
│   ├── normalize/
│   ├── validate/
│   ├── reconcile/
│   ├── reporting/
│   └── strategy/
├── tests/
├── dashboard/         # Dashboard stub
├── dvc/               # DVC pipeline stub
├── mlflow/            # MLflow config stub
├── reports/           # Generated audit outputs and strategy playbooks
├── strategy/          # Generated strategy recommendation parquet/json outputs
├── docs/
├── Makefile
├── pyproject.toml
└── .github/workflows/ci.yml
```

---

## License

See [LICENSE](LICENSE).
created by atrak.dev
