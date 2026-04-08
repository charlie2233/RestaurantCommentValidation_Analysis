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
                "reconcile-qsr50",
                "reconcile-primary-source",
                "audit-reference",
                "gate-gold",
                "snapshot-gold",
                "build-forecast-panel",
                "forecast-baseline",
                "build-rag-corpus",
                "init-rag-benchmark",
                "bootstrap-rag-judgments",
                "seed-rag-queries",
                "validate-rag-benchmark",
                "validate-rag-reviewer-file",
                "adjudicate-rag-benchmark",
                "mine-rag-hard-negatives",
                "summarize-rag-failures",
                "summarize-rag-benchmark-authoring",
                "eval-rag-retrieval",
                "inspect-rag-benchmark",
                "rag-search",
                "preflight-release",
                "demo-happy-path",
                "package-demo",
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
            ["reconcile-qsr50", "--help"],
            [
                "QSR50-only broader reconciliation slice",
                "analyst-readable deltas",
                "reference-dir",
            ],
        ),
        (
            ["reconcile-primary-source", "--help"],
            [
                "primary-source-first reconciliation slice",
                "public-chain reference rows",
                "reference-dir",
            ],
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
            ["preflight-release", "--help"],
            ["Gold outputs", "lineage artifacts", "external-facing handoff"],
        ),
        (
            ["demo-happy-path", "--help"],
            [
                "five-brand demo slice",
                "final demo scorecard outputs",
                "reference directory override",
            ],
        ),
        (
            ["package-demo", "--help"],
            [
                "shareable artifact directory",
                "five-brand demo outputs",
                "Bundle",
            ],
        ),
        (
            ["gate-gold", "--help"],
            [
                "Gold publishing gates",
                "KPI export decisions",
                "audit scorecard",
            ],
        ),
        (
            ["snapshot-gold", "--help"],
            [
                "dated history",
                "future forecasting",
                "Include advisory rows",
            ],
        ),
        (
            ["build-forecast-panel", "--help"],
            [
                "forecast-ready longitudinal panel",
                "artifacts/forecasting",
                "allow-short-history",
            ],
        ),
        (
            ["forecast-baseline", "--help"],
            [
                "offline forecast baselines",
                "holdout periods",
                "seasonal naive",
            ],
        ),
        (
            ["build-rag-corpus", "--help"],
            [
                "retrieval-only corpus",
                "vetted Gold and provenance-aware artifacts",
                "artifacts/rag",
            ],
        ),
        (
            ["init-rag-benchmark", "--help"],
            [
                "local benchmark pack",
                "metadata",
                "checklist",
            ],
        ),
        (
            ["bootstrap-rag-judgments", "--help"],
            [
                "candidate retrieval suggestions",
                "manual benchmark judgment authoring",
                "first-pass candidate chunks",
            ],
        ),
        (
            ["seed-rag-queries", "--help"],
            [
                "deterministic analyst query suggestions",
                "working/",
                "queries.csv",
            ],
        ),
        (
            ["validate-rag-benchmark", "--help"],
            [
                "analyst-authored benchmark",
                "queries.csv",
                "judgments.csv",
            ],
        ),
        (
            ["validate-rag-reviewer-file", "--help"],
            [
                "reviewer-specific",
                "reviewers/<name>/judgments.csv",
                "Validate one reviewer",
            ],
        ),
        (
            ["eval-rag-retrieval", "--help"],
            [
                "retrieval-only baselines",
                "analyst-authored benchmark pack",
                "dense-minilm",
            ],
        ),
        (
            ["adjudicate-rag-benchmark", "--help"],
            [
                "Compare reviewer judgments",
                "adjudicated_judgments.csv",
                "explicit override",
            ],
        ),
        (
            ["mine-rag-hard-negatives", "--help"],
            [
                "review-candidate hard negatives",
                "retrieval run",
                "final judgments",
            ],
        ),
        (
            ["summarize-rag-failures", "--help"],
            [
                "triage categories",
                "benchmark cleanup",
                "run-dir",
            ],
        ),
        (
            ["summarize-rag-benchmark-authoring", "--help"],
            [
                "authoring coverage",
                "current judgment readiness",
                "benchmark-dir",
            ],
        ),
        (
            ["inspect-rag-benchmark", "--help"],
            [
                "Inspect one benchmark query",
                "query_id",
                "failure diagnosis",
            ],
        ),
        (
            ["rag-search", "--help"],
            [
                "ranked chunks plus metadata",
                "not generated answers",
                "Retriever slug",
            ],
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
