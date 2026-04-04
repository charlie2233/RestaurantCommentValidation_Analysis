.PHONY: setup lint test run-ingest run-validate run-syntheticness run-reconcile run-report

setup:
	pip install -e ".[dev]"
	pre-commit install

lint:
	ruff check src tests
	ruff format --check src tests

test:
	pytest

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
