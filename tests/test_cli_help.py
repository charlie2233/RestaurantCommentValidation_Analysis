"""CLI help text coverage for discoverability and operator guidance."""

from __future__ import annotations

import re

import pytest
from qsr_audit.cli import app
from typer.testing import CliRunner


@pytest.mark.parametrize(
    ("args", "expected_snippets"),
    [
        (
            ["--help"],
            [
                "QSR workbook audit pipeline CLI",
                "ingest-workbook",
                "validate-workbook",
                "run-syntheticness",
                "reconcile",
                "audit-reference",
                "report",
            ],
        ),
        (
            ["ingest-workbook", "--help"],
            ["Bronze and Silver artifacts", "source workbook", "raw/Bronze workflow"],
        ),
        (
            ["validate-workbook", "--help"],
            ["Silver directory", "Silver parquet file", "implied AUV"],
        ),
        (
            ["run-syntheticness", "--help"],
            ["core_brand_metrics", "optional"],
        ),
        (
            ["reconcile", "--help"],
            ["manual reference", "reference coverage", "CSV files and templates"],
        ),
        (
            ["audit-reference", "--help"],
            [
                "manual reference coverage",
                "core_brand_metrics parquet file",
                "CSV files and templates",
            ],
        ),
        (
            ["report", "--help"],
            ["Markdown/HTML/JSON audit reports", "Gold-derived strategy outputs"],
        ),
        (
            ["ingest", "--help"],
            ["Legacy placeholder", "ingest-workbook"],
        ),
        (
            ["validate", "--help"],
            ["Legacy placeholder", "validate-workbook"],
        ),
    ],
)
def test_cli_help_is_descriptive(args: list[str], expected_snippets: list[str]) -> None:
    runner = CliRunner()
    result = runner.invoke(app, args)

    assert result.exit_code == 0
    normalized_output = _normalize_help_output(result.stdout)
    for snippet in expected_snippets:
        assert snippet in normalized_output


def _normalize_help_output(text: str) -> str:
    stripped = re.sub(r"\x1b\[[0-9;]*m", "", text)
    return re.sub(r"\s+", " ", stripped)
