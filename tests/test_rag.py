"""Tests for the retrieval-only RAG experiment scaffold."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from qsr_audit.cli import app
from qsr_audit.rag import build_rag_corpus, eval_rag_retrieval
from typer.testing import CliRunner

from tests.helpers import build_settings


def _write_rag_source_artifacts(
    settings,
    *,
    include_optional: bool = True,
    long_validation_summary: bool = False,
) -> None:
    decisions = pd.DataFrame(
        [
            {
                "brand_name": "McDonald's",
                "canonical_brand_name": "McDonald's",
                "metric_name": "system_sales",
                "metric_value": 53.5,
                "publish_status": "publishable",
                "blocking_reasons": "[]",
                "warning_reasons": "[]",
                "source_type": "qsr50",
                "source_name": "QSR 50",
                "source_url_or_doc_id": "doc-mcd",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.95,
                "validation_references": "[]",
                "reconciliation_grade": "A",
                "reference_source_count": 1,
            },
            {
                "brand_name": "Taco Bell",
                "canonical_brand_name": "Taco Bell",
                "metric_name": "auv",
                "metric_value": 2100.0,
                "publish_status": "blocked",
                "blocking_reasons": json.dumps(["AUV contradiction unresolved"]),
                "warning_reasons": "[]",
                "source_type": "qsr50",
                "source_name": "QSR 50",
                "source_url_or_doc_id": "doc-taco",
                "as_of_date": "2024-12-31",
                "method_reported_or_estimated": "reported",
                "confidence_score": 0.4,
                "validation_references": json.dumps(["auv_mismatch"]),
                "reconciliation_grade": "D",
                "reference_source_count": 1,
            },
            {
                "brand_name": "Domino's",
                "canonical_brand_name": "Domino's",
                "metric_name": "margin_mid_pct",
                "metric_value": 18.0,
                "publish_status": "advisory",
                "blocking_reasons": "[]",
                "warning_reasons": json.dumps(["Estimated margin is advisory only"]),
                "source_type": "workbook",
                "source_name": "fixture.xlsx",
                "source_url_or_doc_id": "local-workbook",
                "as_of_date": None,
                "method_reported_or_estimated": "estimated",
                "confidence_score": 0.35,
                "validation_references": "[]",
                "reconciliation_grade": None,
                "reference_source_count": 0,
            },
        ]
    )
    decisions.to_parquet(settings.data_gold / "gold_publish_decisions.parquet", index=False)

    if include_optional:
        pd.DataFrame(
            [
                {
                    "rank": 2,
                    "brand_name": "Taco Bell",
                    "canonical_brand_name": "Taco Bell",
                    "category": "Mexican",
                    "us_store_count_2024": 8000,
                    "systemwide_revenue_usd_billions_2024": 18.0,
                    "average_unit_volume_usd_thousands": 2100.0,
                    "fte_mid": 13.0,
                    "margin_mid_pct": 18.0,
                    "brand_match_confidence": 1.0,
                    "brand_match_method": "alias_exact",
                    "reference_source_count": 1,
                    "overall_credibility_grade": "D",
                    "reconciliation_warning": "AUV contradiction unresolved.",
                }
            ]
        ).to_parquet(settings.data_gold / "reconciled_core_metrics.parquet", index=False)
        pd.DataFrame(
            [
                {
                    "coverage_kind": "brand",
                    "brand_name": "Taco Bell",
                    "canonical_brand_name": "Taco Bell",
                    "metric_name": None,
                    "source_type": None,
                    "reference_row_count": 1,
                    "reference_source_count": 1,
                    "covered_metrics_count": 3,
                    "covered_brand_count": None,
                    "coverage_rate": 0.75,
                    "missing_metrics": json.dumps(["auv"]),
                    "provenance_completeness_score": 1.0,
                    "provenance_completeness_summary": "All provenance fields populated.",
                    "provenance_confidence_summary": "Average confidence 0.92 across 1 row(s).",
                    "warning": "AUV reference is still missing.",
                    "details": json.dumps({"source_names": ["QSR 50"]}),
                    "source_type_names": json.dumps(["qsr50"]),
                }
            ]
        ).to_parquet(settings.data_gold / "reference_coverage.parquet", index=False)
        pd.DataFrame(
            [
                {
                    "source_type": "qsr50",
                    "source_name": "QSR 50",
                    "source_url_or_doc_id": "doc-taco",
                    "as_of_date": "2024-12-31",
                    "method_reported_or_estimated": "reported",
                    "confidence_score": 0.92,
                    "notes": "Annual ranking record.",
                    "extra": json.dumps({"canonical_brand_name": "Taco Bell"}),
                }
            ]
        ).to_parquet(settings.data_gold / "provenance_registry.parquet", index=False)
        pd.DataFrame(
            [
                {
                    "severity": "error",
                    "category": "arithmetic_invariant",
                    "check_name": "core_brand_metrics.auv_matches_sales",
                    "dataset": "core_brand_metrics",
                    "message": "Taco Bell implied AUV conflicts with the recorded AUV.",
                    "sheet_name": "QSR Top30 核心数据",
                    "field_name": "average_unit_volume_usd_thousands",
                    "brand_name": "Taco Bell",
                    "row_number": 5,
                    "expected": "within tolerance",
                    "observed": "6.5% delta",
                    "details": json.dumps({"tolerance": 0.05}),
                }
            ]
        ).to_parquet(settings.data_gold / "validation_flags.parquet", index=False)
        summary_body = (
            "# Validation Summary\n\n"
            "- Status: `FAIL`\n\n"
            "## Findings\n\n"
            "Taco Bell AUV mismatch remains unresolved.\n\n"
        )
        if long_validation_summary:
            summary_body += ("\n\n".join(["Long validation paragraph. " * 40] * 3)) + "\n"
        validation_dir = settings.reports_dir / "validation"
        validation_dir.mkdir(parents=True, exist_ok=True)
        (validation_dir / "validation_summary.md").write_text(summary_body, encoding="utf-8")

    (settings.data_raw / "source_workbook.xlsx").write_text(
        "raw workbook placeholder", encoding="utf-8"
    )
    (settings.data_bronze / "raw_dump.csv").write_text("bronze dump placeholder", encoding="utf-8")
    pd.DataFrame([{"field_name": "store_count", "note_text": "Silver workbook note"}]).to_parquet(
        settings.data_silver / "data_notes.parquet", index=False
    )


def _write_manual_corpus(path: Path) -> pd.DataFrame:
    corpus = pd.DataFrame(
        [
            {
                "doc_id": "gold-publish-decision-taco-bell-auv-blocked",
                "chunk_id": "gold-publish-decision-taco-bell-auv-blocked::chunk-001",
                "source_kind": "gold_publish_decision",
                "title": "Gold publish decision - Taco Bell - auv",
                "text": "Gold publish decision for Taco Bell. Metric: auv. Publish status: blocked.",
                "artifact_path": "data/gold/gold_publish_decisions.parquet",
                "brand_names": json.dumps(["Taco Bell"]),
                "metric_names": json.dumps(["auv"]),
                "as_of_date": "2024-12-31",
                "publish_status": "blocked",
                "confidence_score": 0.4,
                "source_name": "QSR 50",
                "source_url_or_doc_id": "doc-taco",
                "metadata_json": json.dumps({"severity": "warning"}),
            },
            {
                "doc_id": "gold-provenance-taco-bell",
                "chunk_id": "gold-provenance-taco-bell::chunk-001",
                "source_kind": "gold_provenance_registry",
                "title": "Provenance record - Taco Bell",
                "text": "Provenance record for Taco Bell from QSR 50 with annual ranking notes.",
                "artifact_path": "data/gold/provenance_registry.parquet",
                "brand_names": json.dumps(["Taco Bell"]),
                "metric_names": json.dumps([]),
                "as_of_date": "2024-12-31",
                "publish_status": None,
                "confidence_score": 0.92,
                "source_name": "QSR 50",
                "source_url_or_doc_id": "doc-taco",
                "metadata_json": json.dumps({"severity": "info"}),
            },
            {
                "doc_id": "gold-validation-flag-auv",
                "chunk_id": "gold-validation-flag-auv::chunk-001",
                "source_kind": "gold_validation_flag",
                "title": "Validation finding - error - Taco Bell",
                "text": "Validation finding with severity error. Taco Bell implied AUV conflicts with recorded AUV.",
                "artifact_path": "data/gold/validation_flags.parquet",
                "brand_names": json.dumps(["Taco Bell"]),
                "metric_names": json.dumps(["average_unit_volume_usd_thousands"]),
                "as_of_date": None,
                "publish_status": None,
                "confidence_score": None,
                "source_name": "validation_flags",
                "source_url_or_doc_id": None,
                "metadata_json": json.dumps({"severity": "error"}),
            },
        ]
    )
    corpus.to_parquet(path, index=False)
    return corpus


def _write_benchmark_fixture(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "queries": [
                    {
                        "query_id": "blocked-kpi",
                        "query": "Which KPI rows are blocked for external export?",
                        "metadata_filters": {"publish_status": "blocked"},
                        "relevant_chunk_ids": [
                            "gold-publish-decision-taco-bell-auv-blocked::chunk-001"
                        ],
                    },
                    {
                        "query_id": "validation-error",
                        "query": "Show validation findings with error severity.",
                        "metadata_filters": {"source_kind": "gold_validation_flag"},
                        "relevant_chunk_ids": ["gold-validation-flag-auv::chunk-001"],
                    },
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def test_build_rag_corpus_excludes_raw_bronze_silver_by_default(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    _write_rag_source_artifacts(settings)

    run = build_rag_corpus(settings=settings)

    assert set(run.corpus["publish_status"].dropna()) == {
        "publishable",
        "blocked",
        "advisory",
    }
    excluded_paths = {str(settings.data_raw), str(settings.data_bronze), str(settings.data_silver)}
    assert not run.corpus["artifact_path"].str.startswith(tuple(excluded_paths)).any()
    exclusion_reasons = {item["path"] for item in run.manifest["policy_exclusions"]}
    assert excluded_paths.issubset(exclusion_reasons)


def test_build_rag_corpus_records_missing_optional_artifacts(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    _write_rag_source_artifacts(settings, include_optional=False)

    run = build_rag_corpus(settings=settings)

    assert run.artifacts.manifest_path.exists()
    source_status = {item["source_kind"]: item["status"] for item in run.manifest["sources"]}
    assert source_status["gold_reference_coverage"] == "missing"
    assert source_status["gold_provenance_registry"] == "missing"
    assert source_status["validation_summary_markdown"] == "missing"
    assert source_status["manual_reference_notes"] == "missing"


def test_build_rag_corpus_chunking_is_deterministic(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    _write_rag_source_artifacts(settings, long_validation_summary=True)

    first = build_rag_corpus(
        settings=settings, output_root=settings.artifacts_dir / "rag" / "corpus-a"
    )
    second = build_rag_corpus(
        settings=settings, output_root=settings.artifacts_dir / "rag" / "corpus-b"
    )

    assert first.corpus[["chunk_id", "doc_id", "text"]].to_dict(orient="records") == second.corpus[
        ["chunk_id", "doc_id", "text"]
    ].to_dict(orient="records")


def test_eval_rag_retrieval_bm25_runs_on_tiny_fixture(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_manual_corpus(corpus_path)
    benchmark_path = tmp_path / "benchmark.json"
    _write_benchmark_fixture(benchmark_path)

    run = eval_rag_retrieval(
        settings=settings,
        corpus_path=corpus_path,
        benchmark_path=benchmark_path,
        retrievers=("bm25",),
        top_k=2,
    )

    bm25 = run.metrics.loc[run.metrics["retriever_name"] == "bm25"].iloc[0]
    assert bm25["status"] == "ok"
    assert bm25["recall_at_k"] == pytest.approx(1.0)
    assert bm25["mrr"] == pytest.approx(1.0)
    summary = run.artifacts.summary_markdown_path.read_text(encoding="utf-8")
    assert "RAG Retrieval Benchmark Summary" in summary
    assert "Recall@k" in summary
    assert "Citation precision" in summary
    assert "Metadata filter correctness" in summary
    assert "Latency ms" in summary
    assert "Index size bytes" in summary


def test_eval_rag_retrieval_dense_guard_skips_in_ci(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_manual_corpus(corpus_path)
    benchmark_path = tmp_path / "benchmark.json"
    _write_benchmark_fixture(benchmark_path)
    monkeypatch.setenv("CI", "true")

    run = eval_rag_retrieval(
        settings=settings,
        corpus_path=corpus_path,
        benchmark_path=benchmark_path,
        retrievers=("dense-minilm",),
    )

    row = run.metrics.iloc[0]
    assert row["status"] == "skipped"
    assert "disabled in CI" in row["status_reason"]


def test_rag_artifacts_cannot_write_under_reports_or_strategy(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    _write_rag_source_artifacts(settings)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_manual_corpus(corpus_path)
    benchmark_path = tmp_path / "benchmark.json"
    _write_benchmark_fixture(benchmark_path)

    with pytest.raises(ValueError, match="must not be written under analyst-facing paths"):
        build_rag_corpus(settings=settings, output_root=settings.reports_dir)

    with pytest.raises(ValueError, match="must not be written under analyst-facing paths"):
        eval_rag_retrieval(
            settings=settings,
            corpus_path=corpus_path,
            benchmark_path=benchmark_path,
            output_root=settings.strategy_dir,
        )


def test_rag_search_returns_chunks_and_metadata_only(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_manual_corpus(corpus_path)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "rag-search",
            "--query",
            "blocked taco bell auv",
            "--corpus-path",
            str(corpus_path),
            "--top-k",
            "2",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload
    assert "chunk_id" in payload[0]
    assert "metadata_json" in payload[0]
    assert "publish_status" in payload[0]
    assert "answer" not in payload[0]


def test_rag_benchmark_summary_includes_required_sections(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_manual_corpus(corpus_path)
    benchmark_path = tmp_path / "benchmark.json"
    _write_benchmark_fixture(benchmark_path)

    run = eval_rag_retrieval(
        settings=settings,
        corpus_path=corpus_path,
        benchmark_path=benchmark_path,
        retrievers=("bm25",),
        top_k=2,
    )

    summary = run.artifacts.summary_markdown_path.read_text(encoding="utf-8")
    assert "## Retriever Results" in summary
    assert "## Failure Cases" in summary
    assert "Recall@k" in summary
    assert "MRR" in summary
    assert "nDCG@k" in summary
