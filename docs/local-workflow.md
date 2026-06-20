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
make show-targets
make list-workflow-targets
make list-diagnostic-targets
make list-verification-targets
make list-data-targets
make list-report-targets
make lint
make smoke-cli
make quick
make doctor
make version
make ci-status
make test
make verify
make build-package
make check-hygiene
```

`make show-targets` is an alias for `make help`; use whichever wording is
easier to remember.

Use `make list-workflow-targets` when you want a compact index of the
diagnostic, verification, data-layer, report/demo, pipeline, and cleanup command-list
helpers.

Use `make list-diagnostic-targets` to compare non-mutating diagnostic and
discovery helpers without running tests, cleanup, or pipeline commands.

Use `make list-verification-targets` to compare verification and check helpers
without running hooks, tests, hygiene checks, or package builds.

Use `make list-data-targets` to compare data-layer commands and their
Raw/Bronze/Silver/Gold scopes without ingesting, validating, or reconciling data.

Use `make list-report-targets` to compare report and demo artifact-producing
commands and their output locations without generating files.

Use `make smoke-cli` for a quick, non-mutating CLI discoverability check while
iterating on command names, help text, or operator guidance.

Use `make quick` for a faster pre-push check that combines CLI smoke coverage
with repository artifact hygiene.

Use `make doctor` when you need a safe local diagnostics snapshot: Python
version, CLI help, current Git branch/SHA, and short repository status.

Use `make version` when you need to confirm which installed `qsr-audit` package
version is being exercised and which git commit produced the working tree.

Use `make ci-status` to print the latest GitHub Actions run for `origin/main`.
If GitHub CLI is not installed, the target exits successfully with a setup hint.

Use `make verify` before pushing broad or release-relevant changes. It runs the
full local gate: pre-commit hooks, coverage-enforced tests, repository hygiene,
and package build.

## Pipeline loop

```bash
make list-pipeline-targets
```

Use `make list-pipeline-targets` before running pipeline shortcuts if you want
to compare their scope without executing the workflow.

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
make list-clean-targets
make clean-build
make clean-test
make clean-generated
make clean-caches
make clean-all-local
```

Use `make list-clean-targets` before cleanup if you want to compare cleanup
scope without deleting anything.

Use `make clean-build` when you only want to remove Python packaging outputs
(`dist/`, `build/`, and `*.egg-info`) without touching generated data, reports,
or strategy artifacts.

Use `make clean-test` when you only want to remove pytest and coverage outputs
(`.pytest_cache/`, `.coverage`, `coverage.xml`, and `htmlcov/`) without touching
build outputs or generated data.

`make clean-caches` composes `clean-test` and `clean-build`, then removes Python
bytecode caches and `.ruff_cache`.

Use `make clean-all-local` only when you intentionally want a broad local reset:
it removes generated data/report/strategy artifacts and local caches.

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
