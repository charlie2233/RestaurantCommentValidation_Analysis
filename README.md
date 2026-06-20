# qsr-audit-pipeline

A Python pipeline for auditing Quick-Service Restaurant (QSR) operational workbooks,
enforcing data quality through a Bronze -> Silver -> Gold architecture with explicit
validation, reconciliation, publishing gates, and release controls.

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

## Current repo status

The core workflow is implemented and in active analyst use:

- workbook ingestion into Bronze and normalized Silver
- workbook validation and invariant reporting
- syntheticness diagnostics as a review signal, not proof
- manual-first reference ingestion
- QSR50-backed reconciliation expansion
- primary-source reconciliation for a narrow public-chain slice
- Gold publishing gates with `publishable`, `advisory`, and `blocked` outcomes
- release preflight manifests and audit logs
- a 5-brand end-to-end demo path
- retrieval-only RAG benchmarking and benchmark authoring workflows
- forecast-readiness scaffolding based on repeated Gold snapshots

The current bottleneck is evidence collection and analyst review, not missing
framework code.

Maturity note: this is an evidence-collection MVP. The framework is ready for
controlled analyst cycles, but external-facing claims still depend on reviewed
Gold outputs and provenance-backed reference evidence.

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
    ▼  qsr-audit reconcile --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/
data/gold/         ← reconciled, provenance-tagged, policy-gated outputs
    │
    ▼  qsr-audit gate-gold && qsr-audit preflight-release
reports/           ← Audit scorecards and release-facing summaries
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

# 6. Apply Gold publishing gates and release preflight
qsr-audit gate-gold
qsr-audit preflight-release

# 7. Generate audit and strategy reports from Gold outputs
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

## Main workflows

### Standard audit path

```bash
qsr-audit ingest-workbook --input data/raw/source_workbook.xlsx
qsr-audit validate-workbook --input data/silver --tolerance-auv 0.05
qsr-audit run-syntheticness --input data/silver/core_brand_metrics.parquet
qsr-audit reconcile --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/
qsr-audit gate-gold
qsr-audit preflight-release
qsr-audit report --output reports/
```

### Five-brand demo path

```bash
qsr-audit demo-happy-path --input data/raw/<workbook>.xlsx --reference-dir data/reference/
qsr-audit package-demo
```

### QSR50 broader reconciliation

```bash
qsr-audit reconcile-qsr50 --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/
```

### Primary-source public-chain reconciliation

```bash
qsr-audit reconcile-primary-source --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/
```

This path is intentionally narrow and conservative:

- it only uses normalized rows from `data/reference/sec_filings_reference.csv`
- directly comparable primary-source rows outrank QSR50
- scope mismatches are surfaced explicitly instead of being merged away
- unresolved issuer/brand mappings are reported instead of fuzzy-matched

### Retrieval benchmarking

```bash
qsr-audit build-rag-corpus
qsr-audit init-rag-benchmark --name my-pack --author alice
qsr-audit eval-rag-retrieval --benchmark-dir data/rag_benchmarks/my-pack --retriever bm25
```

Retrieval remains offline, benchmark-only, and must not be treated as audited fact
generation.

## Documentation

- Contributor workflow: [`CONTRIBUTING.md`](CONTRIBUTING.md)
- Analyst workflow and interpretation guide: [`docs/analyst-runbook.md`](docs/analyst-runbook.md)
- 5-brand demo walkthrough: [`docs/demo-walkthrough.md`](docs/demo-walkthrough.md)
- Dataset and field definitions: [`docs/data-dictionary.md`](docs/data-dictionary.md)
- CLI reference: [`docs/cli.md`](docs/cli.md)
- Local developer workflow: [`docs/local-workflow.md`](docs/local-workflow.md)
- Research model candidates: [`docs/model_candidates.md`](docs/model_candidates.md)
- Forecasting experiment plan: [`docs/forecasting_experiments.md`](docs/forecasting_experiments.md)
- Lightweight RAG experiment plan: [`docs/rag_experiments.md`](docs/rag_experiments.md)
- Security and privacy controls: [`docs/security-privacy-controls.md`](docs/security-privacy-controls.md)
- Release controls: [`docs/release-runbook.md`](docs/release-runbook.md)
- Reference backlog: [`docs/reference-evidence-backlog.md`](docs/reference-evidence-backlog.md)

## Manual Reference Inputs

Reference ingestion is intentionally manual-first. Populate local CSVs in `data/reference/`
using the committed templates in `data/reference/templates/` rather than scraping unstable
or paid sources automatically. Missing reference coverage is surfaced explicitly during
reconciliation; it is never treated as silent confirmation.

## 5-brand happy-path demo

This repo includes a focused five-brand showcase built from the happy-path artifacts
introduced by PR #15. It is meant to be easy to run, easy to inspect, and easy to share.

Run the demo and package the review bundle:

```bash
qsr-audit demo-happy-path --input data/raw/<workbook>.xlsx --reference-dir data/reference/
qsr-audit package-demo
# or: make demo-bundle
```

The demo slice includes these five brands:

- Starbucks
- Taco Bell
- Raising Cane's
- Dutch Bros
- Shake Shack

What the demo proves:

- The five-brand happy-path slice can be ingested, validated, reconciled, and summarized.
- The demo hub surfaces publishability, provenance, invariant failures, and syntheticness review signals.
- The bundle collects the review-facing outputs into a single shareable artifact directory under `artifacts/demo_bundle/`.

What the demo does not prove:

- It does not establish full-workbook readiness.
- It does not validate brands outside the five-brand happy-path slice.
- It does not collapse publishable, advisory, and blocked rows into a single approval signal.
- It does not replace the workbook-level validation and reconciliation workflow.

Primary demo artifacts:

- [`reports/demo/index.html`](reports/demo/index.html) - static demo hub with the five-brand summary and links to the supporting outputs.
- [`reports/validation/core_scorecard.html`](reports/validation/core_scorecard.html) - row-level validation and publishability scorecard.
- [`reports/reconciliation/brand_deltas.csv`](reports/reconciliation/brand_deltas.csv) - metric-by-metric reconciliation deltas and provenance fields.
- [`reports/summary/top_risks.md`](reports/summary/top_risks.md) - concise risk summary for reviewers.
- [`artifacts/demo_bundle/`](artifacts/demo_bundle/) - packaged copy of the shareable demo outputs.

## What still needs human work

- filling manual reference rows for unresolved brands and metrics
- reviewing blocked and advisory Gold rows before any external use
- authoring and adjudicating real retrieval benchmark judgments
- deciding when a monthly Gold state is clean enough to snapshot for forecasting
- verifying scope mismatches and contradictions instead of forcing them into publishable facts

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
# List supported commands
make help
make show-targets
make list-diagnostic-targets
make list-verification-targets

# Lint
make lint

# Tests
make test

# Fast CLI help smoke checks
make smoke-cli

# Fast CLI smoke + repo hygiene check
make quick

# Safe local diagnostics
make doctor

# Installed package version and current git SHA
make version

# Latest GitHub Actions status for origin/main
make ci-status

# Full verification gate
make verify

# Packaging smoke test
make build-package

# Repository hygiene check
make check-hygiene

# Clean ignored artifacts and caches
make list-pipeline-targets
make list-clean-targets
make clean-generated
make clean-build
make clean-test
make clean-caches
make clean-all-local

# Full verification expands to hooks, coverage, hygiene, and package build.
```

---

## Implementation snapshot

| Area | Status | Notes |
|---|---|---|
| Ingestion / normalization | Implemented | Workbook -> Bronze -> Silver flow is live |
| Validation | Implemented | Invariant checks, scorecards, JSON/Markdown outputs |
| Reconciliation | Implemented | Manual-first references, QSR50 scale-up, primary-source slice |
| Gold publishing gates | Implemented | Explicit `publishable` / `advisory` / `blocked` policy |
| Release controls | Implemented | Manifests, audit logs, `preflight-release` |
| Demo path | Implemented | 5-brand happy-path bundle and scorecard |
| Retrieval benchmarking | Implemented, experimental | Offline-only, benchmark-only |
| Forecasting | Scaffolded, experimental | Requires repeated monthly Gold snapshots |
| Strategy outputs | Implemented, Gold-only consumer | Must be read with validation and reconciliation context |

---

## Repository layout

```
qsr-audit-pipeline/
├── data/
│   ├── raw/           # Original workbooks (not committed)
│   ├── bronze/        # Parsed Parquet (not committed)
│   ├── silver/        # Normalised Parquet (not committed)
│   ├── gold/          # Reconciled and gated outputs (mostly ignored; a few review artifacts are committed)
│   └── reference/     # Local reference tables and committed templates/manual starter data
├── src/qsr_audit/     # Main package
│   ├── cli.py         # Typer CLI entrypoint
│   ├── config.py      # Pydantic-settings configuration
│   ├── contracts/     # Future data contracts / schemas
│   ├── ingest/
│   ├── normalize/
│   ├── validate/
│   ├── reconcile/
│   ├── reporting/
│   ├── strategy/
│   └── models/        # Research-only model metadata and offline stubs
├── tests/
├── dashboard/         # Dashboard stub
├── dvc/               # DVC pipeline stub
├── mlflow/            # MLflow config stub
├── reports/           # Generated audit outputs and selected committed demo/reconciliation summaries
├── strategy/          # Generated strategy recommendation parquet/json outputs
├── artifacts/         # Internal manifests, audit logs, forecasting, RAG, and release controls
├── docs/
├── Makefile
├── pyproject.toml
└── .github/workflows/ci.yml
```

---

## License

See [LICENSE](LICENSE).
created by atrak.dev
