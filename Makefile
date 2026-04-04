.PHONY: setup lint test check-hygiene build-package clean-generated clean-caches run-ingest run-validate run-syntheticness run-reconcile run-report run-full-audit

setup:
	pip install -e ".[dev]"
	pre-commit install

lint:
	pre-commit run --all-files

test:
	pytest

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
	qsr-audit report --output reports/
