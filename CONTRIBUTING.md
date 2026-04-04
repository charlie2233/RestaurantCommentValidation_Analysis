# Contributing

## Working principles

- Treat every workbook as a hypothesis artifact, not a source of truth.
- Gold outputs are the only safe downstream inputs for strategy or executive reporting.
- Do not commit raw workbooks, local reference data, Bronze/Silver/Gold artifacts, generated reports, or strategy output blobs.
- Prefer deterministic, auditable logic over opaque automation.

## Local setup

```bash
python -m venv .venv
source .venv/bin/activate
make setup
```

## Core developer workflow

```bash
# Lint and repo hygiene
make lint
make check-hygiene

# Test suite
make test

# Packaging smoke test
make build-package

# Clean ignored artifacts and caches
make clean-generated
make clean-caches
```

## Sample local pipeline workflow

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

## Testing expectations

- Add or update tests for every user-visible workflow change.
- Prefer small fixture workbooks and deterministic CSV/parquet fixtures.
- Cover CLI help text whenever command behavior or discoverability changes.
- For data-quality work, include both good-path and contradiction-path tests.

## CI expectations

Every PR should pass:

- `pre-commit run --all-files`
- `pytest --cov=src --cov-report=term-missing --cov-fail-under=85`
- `python -m build`
- `python scripts/check_repo_hygiene.py`

## Data handling and provenance expectations

- Manual reference ingestion is the default path for unstable or paid sources.
- Strategy code must consume Gold validated and reconciled outputs only.
- Do not add speculative ROI claims without explicit provenance support.
- If a new field changes business meaning, update `docs/data-dictionary.md` and the analyst runbook.
- Research-only model helpers may read local Silver or Gold snapshots for offline evaluation, but they must not change source-of-truth metrics or gate the supported CLI flow.
- Do not wire `src/qsr_audit/models/` into validation, reconciliation, reporting, or strategy without a separate hardening proposal.

## Pull requests

- Keep patches surgical when possible.
- Summaries should explain user-facing behavior, data-layer impact, and verification.
- Update docs when commands, outputs, or interpretation rules change.
