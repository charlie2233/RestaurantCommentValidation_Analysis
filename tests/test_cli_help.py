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
            ["Bronze and Silver artifacts", "--input"],
        ),
        (
            ["validate-workbook", "--help"],
            ["Silver directory", "--tolerance-auv"],
        ),
        (
            ["run-syntheticness", "--help"],
            ["core_brand_metrics", "optional"],
        ),
        (
            ["reconcile", "--help"],
            ["manual reference", "reference coverage", "--reference-dir"],
        ),
        (
            ["audit-reference", "--help"],
            ["manual reference coverage", "--core", "--reference-dir"],
        ),
        (
            ["report", "--help"],
            ["Markdown/HTML/JSON audit reports", "--output"],
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
