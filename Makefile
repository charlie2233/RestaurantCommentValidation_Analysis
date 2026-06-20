.PHONY: help setup lint test smoke-cli verify check-hygiene build-package clean-generated clean-caches run-ingest run-validate run-syntheticness run-reconcile run-report run-full-audit demo-bundle

help:
	@printf "qsr-audit developer commands\n"
	@printf "\n"
	@printf "Setup and verification:\n"
	@printf "  make setup              Install editable dev dependencies and pre-commit hooks\n"
	@printf "  make lint               Run pre-commit hooks on all files\n"
	@printf "  make test               Run the pytest suite\n"
	@printf "  make smoke-cli          Run fast CLI help smoke checks\n"
	@printf "  make verify             Run hooks, coverage tests, repo hygiene, and package build\n"
	@printf "  make check-hygiene      Run repository artifact hygiene checks\n"
	@printf "  make build-package      Build the Python package\n"
	@printf "\n"
	@printf "Pipeline shortcuts:\n"
	@printf "  make run-ingest         Ingest the default workbook path into Bronze/Silver\n"
	@printf "  make run-validate       Validate the default Silver input\n"
	@printf "  make run-syntheticness  Run syntheticness diagnostics on core metrics\n"
	@printf "  make run-reconcile      Reconcile Silver metrics against manual references\n"
	@printf "  make run-report         Generate reports from Gold outputs\n"
	@printf "  make run-full-audit     Run the standard audit path through release preflight\n"
	@printf "  make demo-bundle        Package the demo showcase bundle\n"
	@printf "\n"
	@printf "Cleanup:\n"
	@printf "  make clean-generated    Remove ignored generated data/report artifacts\n"
	@printf "  make clean-caches       Remove Python/test/build caches\n"

setup:
	pip install -e ".[dev]"
	pre-commit install

lint:
	pre-commit run --all-files

test:
	pytest

smoke-cli:
	pytest tests/test_cli_help.py

verify:
	pre-commit run --all-files
	pytest --cov=src --cov-report=term-missing --cov-fail-under=85
	python scripts/check_repo_hygiene.py
	python -m build

check-hygiene:
	python scripts/check_repo_hygiene.py

build-package:
	python -m build

clean-generated:
	find data/raw -mindepth 1 ! -name '.gitkeep' -delete
	find data/bronze -mindepth 1 ! -name '.gitkeep' -delete
	find data/silver -mindepth 1 ! -name '.gitkeep' -delete
	find data/gold -mindepth 1 ! -name '.gitkeep' -delete
	find reports -mindepth 1 ! -name '.gitkeep' -delete
	find strategy -mindepth 1 ! -name '.gitkeep' -delete

clean-caches:
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache htmlcov .coverage coverage.xml dist build

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
