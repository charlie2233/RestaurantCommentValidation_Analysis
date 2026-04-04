"""CLI help text coverage for discoverability and operator guidance."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from qsr_audit.cli import app


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
            ["manual reference", "--reference-dir"],
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
    for snippet in expected_snippets:
        assert snippet in result.stdout
