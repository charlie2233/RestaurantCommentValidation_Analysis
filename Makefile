.PHONY: setup lint test run-ingest run-validate run-report

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
	qsr-audit validate --layer silver

run-report:
	qsr-audit report --output reports/
