.PHONY: help show-targets setup lint test smoke-cli quick doctor version ci-status list-diagnostic-targets list-verification-targets verify check-hygiene build-package list-clean-targets clean-generated clean-build clean-test clean-caches clean-all-local list-pipeline-targets run-ingest run-validate run-syntheticness run-reconcile run-report run-full-audit demo-bundle

help:
	@printf "qsr-audit developer commands\n"
	@printf "\n"
	@printf "Setup and verification:\n"
	@printf "  make show-targets       Alias for make help\n"
	@printf "  make setup              Install editable dev dependencies and pre-commit hooks\n"
	@printf "  make lint               Run pre-commit hooks on all files\n"
	@printf "  make test               Run the pytest suite\n"
	@printf "  make smoke-cli          Run fast CLI help smoke checks\n"
	@printf "  make quick              Run CLI smoke checks and repository hygiene\n"
	@printf "  make doctor             Print safe local diagnostics without modifying files\n"
	@printf "  make version            Print installed package version and git commit\n"
	@printf "  make ci-status          Print latest GitHub Actions status for origin/main\n"
	@printf "  make list-diagnostic-targets  Print diagnostic/discovery commands and scope notes\n"
	@printf "  make list-verification-targets  Print verification/check commands and scope notes\n"
	@printf "  make verify             Run hooks, coverage tests, repo hygiene, and package build\n"
	@printf "  make check-hygiene      Run repository artifact hygiene checks\n"
	@printf "  make build-package      Build the Python package\n"
	@printf "\n"
	@printf "Pipeline shortcuts:\n"
	@printf "  make list-pipeline-targets    Print pipeline commands and scope notes\n"
	@printf "  make run-ingest         Ingest the default workbook path into Bronze/Silver\n"
	@printf "  make run-validate       Validate the default Silver input\n"
	@printf "  make run-syntheticness  Run syntheticness diagnostics on core metrics\n"
	@printf "  make run-reconcile      Reconcile Silver metrics against manual references\n"
	@printf "  make run-report         Generate reports from Gold outputs\n"
	@printf "  make run-full-audit     Run the standard audit path through release preflight\n"
	@printf "  make demo-bundle        Package the demo showcase bundle\n"
	@printf "\n"
	@printf "Cleanup:\n"
	@printf "  make list-clean-targets       Print cleanup commands and scope notes\n"
	@printf "  make clean-generated    Remove ignored generated data/report artifacts\n"
	@printf "  make clean-build        Remove package build outputs only\n"
	@printf "  make clean-test         Remove pytest and coverage artifacts only\n"
	@printf "  make clean-caches       Remove Python/test/build caches\n"
	@printf "  make clean-all-local    Remove generated local artifacts and caches\n"

show-targets:
	$(MAKE) help

setup:
	pip install -e ".[dev]"
	pre-commit install

lint:
	pre-commit run --all-files

test:
	pytest

smoke-cli:
	pytest tests/test_cli_help.py

quick:
	$(MAKE) smoke-cli
	$(MAKE) check-hygiene

doctor:
	@printf "Python:\n"
	@python --version
	@printf "\nqsr-audit CLI help:\n"
	@qsr-audit --help
	@printf "\nGit branch:\n"
	@git branch --show-current
	@printf "\nGit commit:\n"
	@git rev-parse --short HEAD
	@printf "\nGit status:\n"
	@git status --short --branch

version:
	@python -c "from importlib.metadata import version; print('qsr-audit package:', version('qsr-audit'))"
	@printf "git commit: "
	@git rev-parse --short HEAD

ci-status:
	@if ! command -v gh >/dev/null 2>&1; then \
		printf "GitHub CLI (gh) is not installed. Install gh to check CI status from Make.\n"; \
	else \
		branch=$$(git symbolic-ref --quiet --short refs/remotes/origin/HEAD 2>/dev/null | sed 's#^origin/##'); \
		if [ -z "$$branch" ]; then branch=main; fi; \
		printf "Latest GitHub Actions run for origin/%s:\n" "$$branch"; \
		gh run list --branch "$$branch" --limit 1; \
	fi

list-diagnostic-targets:
	@printf "Diagnostic and discovery targets\n"
	@printf "\n"
	@printf "  make help               Full Make command index\n"
	@printf "  make show-targets       Alias for make help\n"
	@printf "  make list-verification-targets  Verification/check commands and scope notes\n"
	@printf "  make list-pipeline-targets    Pipeline shortcut commands and scope notes\n"
	@printf "  make list-clean-targets       Cleanup commands and scope notes\n"
	@printf "  make doctor             Python, CLI help, git branch/SHA, and repo status\n"
	@printf "  make version            Installed package version and git commit\n"
	@printf "  make ci-status          Latest GitHub Actions status when gh is installed\n"

list-verification-targets:
	@printf "Verification and check targets\n"
	@printf "\n"
	@printf "  make lint               Pre-commit hooks across the repository\n"
	@printf "  make test               Full test suite without coverage gate\n"
	@printf "  make smoke-cli          Fast CLI help smoke checks\n"
	@printf "  make quick              CLI smoke checks plus repository hygiene\n"
	@printf "  make verify             Full local gate: hooks, coverage, hygiene, package build\n"
	@printf "  make check-hygiene      Repository artifact hygiene checks\n"
	@printf "  make build-package      Source distribution and wheel build\n"

verify:
	pre-commit run --all-files
	pytest --cov=src --cov-report=term-missing --cov-fail-under=85
	python scripts/check_repo_hygiene.py
	python -m build

check-hygiene:
	python scripts/check_repo_hygiene.py

build-package:
	python -m build

list-clean-targets:
	@printf "qsr-audit cleanup targets\n"
	@printf "\n"
	@printf "  make clean-build        Packaging outputs only: dist/, build/, *.egg-info\n"
	@printf "  make clean-test         Test outputs only: .pytest_cache/, .coverage, coverage.xml, htmlcov/\n"
	@printf "  make clean-caches       Test/build outputs plus Python bytecode and .ruff_cache\n"
	@printf "  make clean-generated    Generated local data/report artifacts under data/, reports/, strategy/\n"
	@printf "  make clean-all-local    Broad local reset: clean-generated plus clean-caches\n"

clean-generated:
	find data/raw -mindepth 1 ! -name '.gitkeep' -delete
	find data/bronze -mindepth 1 ! -name '.gitkeep' -delete
	find data/silver -mindepth 1 ! -name '.gitkeep' -delete
	find data/gold -mindepth 1 ! -name '.gitkeep' -delete
	find reports -mindepth 1 ! -name '.gitkeep' -delete
	find strategy -mindepth 1 ! -name '.gitkeep' -delete

clean-build:
	rm -rf dist build
	find . -path './.venv' -prune -o -path './.git' -prune -o -name '*.egg-info' -type d -prune -exec rm -rf {} +

clean-test:
	rm -rf .pytest_cache .coverage coverage.xml htmlcov

clean-caches:
	$(MAKE) clean-test
	$(MAKE) clean-build
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	rm -rf .ruff_cache

clean-all-local:
	$(MAKE) clean-generated
	$(MAKE) clean-caches

list-pipeline-targets:
	@printf "Pipeline shortcut targets\n"
	@printf "\n"
	@printf "  make run-ingest         Workbook ingest into Bronze/Silver\n"
	@printf "  make run-validate       Silver workbook validation with AUV tolerance\n"
	@printf "  make run-syntheticness  Syntheticness diagnostics on core metrics\n"
	@printf "  make run-reconcile      Manual-reference reconciliation into Gold artifacts\n"
	@printf "  make run-report         Report generation from Gold outputs\n"
	@printf "  make run-full-audit     Full standard audit path through release preflight\n"
	@printf "  make demo-bundle        Package the five-brand demo showcase bundle\n"

run-ingest:
	qsr-audit ingest-workbook --input data/raw/source_workbook.xlsx

run-validate:
	qsr-audit validate-workbook --input data/silver --tolerance-auv 0.05

run-syntheticness:
	qsr-audit run-syntheticness --input data/silver/core_brand_metrics.parquet

run-reconcile:
	qsr-audit reconcile --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/

run-report:
	qsr-audit report --output reports/

run-full-audit:
	qsr-audit ingest-workbook --input data/raw/source_workbook.xlsx
	qsr-audit validate-workbook --input data/silver --tolerance-auv 0.05
	qsr-audit run-syntheticness --input data/silver/core_brand_metrics.parquet
	qsr-audit reconcile --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/
	qsr-audit gate-gold
	qsr-audit preflight-release
	qsr-audit report --output reports/

demo-bundle:
	qsr-audit package-demo
