# Local Workflow

## Day-1 setup

```bash
python -m venv .venv
source .venv/bin/activate
make setup
```

## Common developer loop

```bash
make help
make lint
make smoke-cli
make quick
make test
make verify
make build-package
make check-hygiene
```

Use `make smoke-cli` for a quick, non-mutating CLI discoverability check while
iterating on command names, help text, or operator guidance.

Use `make quick` for a faster pre-push check that combines CLI smoke coverage
with repository artifact hygiene.

Use `make verify` before pushing broad or release-relevant changes. It runs the
full local gate: pre-commit hooks, coverage-enforced tests, repository hygiene,
and package build.

## Pipeline loop

```bash
qsr-audit ingest-workbook --input data/raw/source_workbook.xlsx
qsr-audit validate-workbook --input data/silver --tolerance-auv 0.05
qsr-audit run-syntheticness --input data/silver/core_brand_metrics.parquet
qsr-audit reconcile --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/
qsr-audit gate-gold
qsr-audit preflight-release
qsr-audit report --output reports/
```

Or use:

```bash
make run-full-audit
```

`make run-full-audit` follows the same standard path, including Gold gate
generation and release preflight before report generation.

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

## Optional offline experimentation loop

Research-only model work should stay separate from the supported pipeline:

```bash
python - <<'PY'
from qsr_audit.models import get_embedding_candidates

for candidate in get_embedding_candidates():
    print(candidate.repo_id, "->", candidate.preferred_role)
PY
```

Use the experiment docs in `docs/model_candidates.md`,
`docs/forecasting_experiments.md`, and `docs/rag_experiments.md` as the source
of truth for research scope and guardrails.

## Commit policy reminders

- Do not commit raw workbook files or local reference data.
- Do not commit Bronze, Silver, Gold, `reports/`, or `strategy/` outputs.
- Keep workbook claims as hypotheses until Gold validation and reconciliation are complete.
