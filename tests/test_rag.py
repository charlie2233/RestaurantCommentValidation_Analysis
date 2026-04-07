"""Tests for the retrieval-only RAG experiment scaffold."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from qsr_audit.cli import app
from qsr_audit.rag import (
    adjudicate_rag_benchmark,
    build_rag_corpus,
    eval_rag_retrieval,
    init_rag_benchmark,
    inspect_rag_benchmark_query,
    summarize_rag_benchmark_authoring,
    validate_rag_benchmark_pack,
    validate_rag_reviewer_file,
)
from qsr_audit.rag.authoring import bootstrap_rag_judgments
from typer.testing import CliRunner

from tests.helpers import build_settings


def _set_cli_env(monkeypatch: pytest.MonkeyPatch, settings) -> None:
    monkeypatch.setenv("QSR_DATA_RAW", str(settings.data_raw))
    monkeypatch.setenv("QSR_DATA_BRONZE", str(settings.data_bronze))
    monkeypatch.setenv("QSR_DATA_SILVER", str(settings.data_silver))
    monkeypatch.setenv("QSR_DATA_GOLD", str(settings.data_gold))
    monkeypatch.setenv("QSR_DATA_REFERENCE", str(settings.data_reference))
    monkeypatch.setenv("QSR_REPORTS_DIR", str(settings.reports_dir))
    monkeypatch.setenv("QSR_STRATEGY_DIR", str(settings.strategy_dir))
    monkeypatch.setenv("QSR_ARTIFACTS_DIR", str(settings.artifacts_dir))


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


def _write_multi_chunk_doc_corpus(path: Path) -> pd.DataFrame:
    corpus = pd.DataFrame(
        [
            {
                "doc_id": "gold-publish-decision-taco-bell-auv-blocked",
                "chunk_id": "gold-publish-decision-taco-bell-auv-blocked::chunk-001",
                "source_kind": "gold_publish_decision",
                "title": "Gold publish decision - Taco Bell - auv",
                "text": "Taco Bell AUV blocked for external export because contradiction remains unresolved.",
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
                "doc_id": "gold-publish-decision-taco-bell-auv-blocked",
                "chunk_id": "gold-publish-decision-taco-bell-auv-blocked::chunk-002",
                "source_kind": "gold_publish_decision",
                "title": "Gold publish decision - Taco Bell - details",
                "text": "Annual source details and provenance appendix.",
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


def _write_benchmark_pack(benchmark_dir: Path) -> None:
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "query_id": "blocked-kpi",
                "query_text": "Which Taco Bell KPI rows are blocked for external export?",
                "language": "en",
                "notes": "Blocked KPI smoke benchmark.",
                "brand_filter": "Taco Bell",
                "metric_filter": "auv",
                "publish_status_scope": "blocked",
                "expected_source_kinds": "gold_publish_decision",
                "ambiguity_flag": "false",
                "requires_citation": "true",
            },
            {
                "query_id": "validation-error",
                "query_text": "Show Taco Bell validation findings with error severity.",
                "language": "en",
                "notes": "Validation error smoke benchmark.",
                "brand_filter": "Taco Bell",
                "metric_filter": "",
                "publish_status_scope": "all",
                "expected_source_kinds": "gold_validation_flag",
                "ambiguity_flag": "false",
                "requires_citation": "false",
            },
            {
                "query_id": "brand-compare",
                "query_text": "Compare Taco Bell and McDonald's export readiness.",
                "language": "en",
                "notes": "Cross-brand readiness query.",
                "brand_filter": "Taco Bell|McDonald's",
                "metric_filter": "auv",
                "publish_status_scope": "all",
                "expected_source_kinds": "gold_publish_decision",
                "ambiguity_flag": "true",
                "requires_citation": "true",
            },
        ]
    ).to_csv(benchmark_dir / "queries.csv", index=False)
    pd.DataFrame(
        [
            {
                "query_id": "blocked-kpi",
                "doc_id": "gold-publish-decision-taco-bell-auv-blocked",
                "chunk_id": "",
                "relevance_label": "highly_relevant",
                "rationale": "Blocked Taco Bell AUV decision should be retrieved.",
                "must_appear_in_top_k": "2",
            },
            {
                "query_id": "validation-error",
                "doc_id": "",
                "chunk_id": "gold-validation-flag-auv::chunk-001",
                "relevance_label": "highly_relevant",
                "rationale": "The validation flag is the direct answer.",
                "must_appear_in_top_k": "1",
            },
            {
                "query_id": "brand-compare",
                "doc_id": "gold-publish-decision-taco-bell-auv-blocked",
                "chunk_id": "",
                "relevance_label": "relevant",
                "rationale": "Taco Bell readiness should be part of comparison context.",
                "must_appear_in_top_k": "",
            },
        ]
    ).to_csv(benchmark_dir / "judgments.csv", index=False)
    pd.DataFrame(
        [
            {
                "query_id": "validation-error",
                "filter_key": "source_kind",
                "filter_value": "gold_validation_flag",
                "notes": "Restrict to validation findings.",
            }
        ]
    ).to_csv(benchmark_dir / "filters.csv", index=False)
    pd.DataFrame(
        [
            {
                "query_id": "brand-compare",
                "query_group": "cross_brand_comparison",
                "notes": "Custom cross-brand group.",
            }
        ]
    ).to_csv(benchmark_dir / "query_groups.csv", index=False)


def _write_reviewer_judgments(
    benchmark_dir: Path,
    reviewer: str,
    rows: list[dict[str, str]],
) -> Path:
    reviewer_dir = benchmark_dir / "reviewers" / reviewer
    reviewer_dir.mkdir(parents=True, exist_ok=True)
    reviewer_path = reviewer_dir / "judgments.csv"
    pd.DataFrame(rows, columns=JUDGMENT_COLUMNS).to_csv(reviewer_path, index=False)
    return reviewer_path


JUDGMENT_COLUMNS = [
    "query_id",
    "doc_id",
    "chunk_id",
    "relevance_label",
    "rationale",
    "must_appear_in_top_k",
]


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


def test_eval_rag_retrieval_defaults_to_settings_artifacts_dir_corpus(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    _write_rag_source_artifacts(settings)
    build_rag_corpus(settings=settings)

    run = eval_rag_retrieval(
        settings=settings,
        retrievers=("bm25",),
        top_k=2,
    )

    assert run.summary["corpus_path"] == str(
        (settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet").resolve()
    )


def test_rag_cli_default_build_then_eval_uses_default_corpus_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = build_settings(tmp_path)
    _write_rag_source_artifacts(settings)
    _set_cli_env(monkeypatch, settings)

    runner = CliRunner()
    build_result = runner.invoke(app, ["build-rag-corpus"])
    assert build_result.exit_code == 0

    eval_result = runner.invoke(app, ["eval-rag-retrieval", "--retriever", "bm25"])
    assert eval_result.exit_code == 0
    assert (settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet").exists()
    assert (settings.artifacts_dir / "rag" / "benchmarks" / "metrics.json").exists()
    assert "RAG retrieval benchmark complete" in eval_result.output


def test_rag_cli_eval_accepts_relative_corpus_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = build_settings(tmp_path)
    _write_rag_source_artifacts(settings)
    _set_cli_env(monkeypatch, settings)

    runner = CliRunner()
    build_result = runner.invoke(app, ["build-rag-corpus"])
    assert build_result.exit_code == 0

    monkeypatch.chdir(tmp_path)
    result = runner.invoke(
        app,
        [
            "eval-rag-retrieval",
            "--corpus-path",
            "artifacts/rag/corpus/corpus.parquet",
            "--retriever",
            "bm25",
        ],
    )

    assert result.exit_code == 0
    assert "RAG retrieval benchmark complete" in result.output


def test_rag_cli_eval_accepts_absolute_corpus_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = build_settings(tmp_path)
    _write_rag_source_artifacts(settings)
    _set_cli_env(monkeypatch, settings)

    runner = CliRunner()
    build_result = runner.invoke(app, ["build-rag-corpus"])
    assert build_result.exit_code == 0

    corpus_path = (settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet").resolve()
    result = runner.invoke(
        app,
        [
            "eval-rag-retrieval",
            "--corpus-path",
            str(corpus_path),
            "--retriever",
            "bm25",
        ],
    )

    assert result.exit_code == 0
    assert "RAG retrieval benchmark complete" in result.output


def test_rag_cli_eval_missing_default_corpus_errors_cleanly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = build_settings(tmp_path)
    _set_cli_env(monkeypatch, settings)

    runner = CliRunner()
    result = runner.invoke(app, ["eval-rag-retrieval", "--retriever", "bm25"])

    assert result.exit_code != 0
    assert "build-rag-corpus" in result.output
    assert "corpus parquet" in result.output


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
    assert "## Query Bucket Results" in summary
    assert "## Failure Cases" in summary
    assert "Recall@k" in summary
    assert "MRR" in summary
    assert "nDCG@k" in summary


def test_validate_rag_benchmark_pack_catches_dangling_chunk_references(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    corpus = _write_manual_corpus(corpus_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    _write_benchmark_pack(benchmark_dir)

    judgments = pd.read_csv(benchmark_dir / "judgments.csv", dtype=str, keep_default_na=False)
    judgments.loc[0, "chunk_id"] = "missing::chunk-001"
    judgments.loc[0, "doc_id"] = ""
    judgments.to_csv(benchmark_dir / "judgments.csv", index=False)

    run = validate_rag_benchmark_pack(
        benchmark_dir=benchmark_dir,
        corpus=corpus,
        settings=settings,
    )

    assert not run.passed
    assert any(issue["category"] == "dangling_chunk_id" for issue in run.issues)


def test_validate_rag_benchmark_pack_fails_on_empty_pack(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    corpus = _write_manual_corpus(corpus_path)
    benchmark_dir = tmp_path / "empty-pack"
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        columns=[
            "query_id",
            "query_text",
            "language",
            "notes",
            "brand_filter",
            "metric_filter",
            "publish_status_scope",
            "expected_source_kinds",
            "ambiguity_flag",
            "requires_citation",
        ]
    ).to_csv(benchmark_dir / "queries.csv", index=False)
    pd.DataFrame(
        columns=[
            "query_id",
            "doc_id",
            "chunk_id",
            "relevance_label",
            "rationale",
            "must_appear_in_top_k",
        ]
    ).to_csv(benchmark_dir / "judgments.csv", index=False)

    run = validate_rag_benchmark_pack(
        benchmark_dir=benchmark_dir,
        corpus=corpus,
        settings=settings,
    )

    assert not run.passed
    assert any(issue["category"] == "empty_pack" for issue in run.issues)


def test_validate_rag_benchmark_writes_required_sections(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    corpus = _write_manual_corpus(corpus_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    _write_benchmark_pack(benchmark_dir)

    run = validate_rag_benchmark_pack(
        benchmark_dir=benchmark_dir,
        corpus=corpus,
        settings=settings,
    )

    summary = run.artifacts.validation_markdown_path.read_text(encoding="utf-8")
    assert "RAG Benchmark Validation Summary" in summary
    assert "## Errors" in summary
    assert "## Warnings" in summary
    assert "## Required Files" in summary


def test_validate_rag_benchmark_pack_malformed_rows_emit_structured_artifacts(
    tmp_path: Path,
) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    corpus = _write_manual_corpus(corpus_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    _write_benchmark_pack(benchmark_dir)

    queries = pd.read_csv(benchmark_dir / "queries.csv", dtype=str, keep_default_na=False)
    queries.loc[0, "ambiguity_flag"] = "maybe"
    queries.to_csv(benchmark_dir / "queries.csv", index=False)

    judgments = pd.read_csv(benchmark_dir / "judgments.csv", dtype=str, keep_default_na=False)
    judgments.loc[0, "must_appear_in_top_k"] = "top-two"
    judgments.to_csv(benchmark_dir / "judgments.csv", index=False)

    run = validate_rag_benchmark_pack(
        benchmark_dir=benchmark_dir,
        corpus=corpus,
        settings=settings,
    )

    assert not run.passed
    assert run.query_specs == []
    categories = {issue["category"] for issue in run.issues}
    assert "invalid_boolean_field" in categories
    assert "invalid_must_appear_threshold" in categories
    assert run.artifacts.validation_json_path.exists()
    assert run.artifacts.validation_markdown_path.exists()
    assert run.artifacts.query_specs_json_path.exists()
    validation_payload = json.loads(run.artifacts.validation_json_path.read_text(encoding="utf-8"))
    assert validation_payload["issue_count"] >= 2
    assert json.loads(run.artifacts.query_specs_json_path.read_text(encoding="utf-8")) == []


def test_eval_rag_retrieval_accepts_benchmark_dir_and_writes_bucket_metrics(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_manual_corpus(corpus_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    _write_benchmark_pack(benchmark_dir)

    run = eval_rag_retrieval(
        settings=settings,
        corpus_path=corpus_path,
        benchmark_dir=benchmark_dir,
        retrievers=("bm25",),
        top_k=2,
    )

    bucket_metrics = pd.read_csv(run.artifacts.query_bucket_metrics_csv_path)
    assert "brand_specific" in set(bucket_metrics["bucket_name"])
    assert "provenance_citation" in set(bucket_metrics["bucket_name"])
    assert "metadata_filter_heavy" in set(bucket_metrics["bucket_name"])
    assert "cross_brand_comparison" in set(bucket_metrics["bucket_name"])
    assert "ambiguous" in set(bucket_metrics["bucket_name"])


def test_eval_rag_retrieval_doc_level_judgment_is_satisfied_by_any_chunk_from_doc(
    tmp_path: Path,
) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_multi_chunk_doc_corpus(corpus_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "query_id": "blocked-kpi",
                "query_text": "Which Taco Bell KPI rows are blocked for external export?",
                "language": "en",
                "notes": "Doc-level judgment should be satisfied by any chunk from the doc.",
                "brand_filter": "Taco Bell",
                "metric_filter": "auv",
                "publish_status_scope": "blocked",
                "expected_source_kinds": "gold_publish_decision",
                "ambiguity_flag": "false",
                "requires_citation": "true",
            }
        ]
    ).to_csv(benchmark_dir / "queries.csv", index=False)
    pd.DataFrame(
        [
            {
                "query_id": "blocked-kpi",
                "doc_id": "gold-publish-decision-taco-bell-auv-blocked",
                "chunk_id": "",
                "relevance_label": "highly_relevant",
                "rationale": "Any chunk from the blocked decision doc should satisfy the judgment.",
                "must_appear_in_top_k": "1",
            }
        ]
    ).to_csv(benchmark_dir / "judgments.csv", index=False)

    run = eval_rag_retrieval(
        settings=settings,
        corpus_path=corpus_path,
        benchmark_dir=benchmark_dir,
        retrievers=("bm25",),
        top_k=1,
    )

    row = run.metrics.loc[run.metrics["stage"] == "retrieval"].iloc[0]
    assert row["status"] == "ok"
    assert row["recall_at_k"] == pytest.approx(1.0)


def test_eval_rag_retrieval_chunk_level_judgment_requires_exact_chunk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_multi_chunk_doc_corpus(corpus_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "query_id": "blocked-kpi",
                "query_text": "Why is Taco Bell blocked for external export because contradiction remains unresolved?",
                "language": "en",
                "notes": "Chunk-level judgment should require the exact judged chunk.",
                "brand_filter": "Taco Bell",
                "metric_filter": "auv",
                "publish_status_scope": "blocked",
                "expected_source_kinds": "gold_publish_decision",
                "ambiguity_flag": "false",
                "requires_citation": "true",
            }
        ]
    ).to_csv(benchmark_dir / "queries.csv", index=False)
    pd.DataFrame(
        [
            {
                "query_id": "blocked-kpi",
                "doc_id": "",
                "chunk_id": "gold-publish-decision-taco-bell-auv-blocked::chunk-002",
                "relevance_label": "highly_relevant",
                "rationale": "Only the second chunk should satisfy the exact chunk-level judgment.",
                "must_appear_in_top_k": "1",
            }
        ]
    ).to_csv(benchmark_dir / "judgments.csv", index=False)

    from qsr_audit.rag.retrieval import RagSearchRun

    def _fake_rag_search(**kwargs):
        results = pd.DataFrame(
            [
                {
                    "rank": 1,
                    "score": 10.0,
                    "retriever_name": "bm25",
                    "doc_id": "gold-publish-decision-taco-bell-auv-blocked",
                    "chunk_id": "gold-publish-decision-taco-bell-auv-blocked::chunk-001",
                    "source_kind": "gold_publish_decision",
                    "title": "Gold publish decision - Taco Bell - auv",
                    "artifact_path": "data/gold/gold_publish_decisions.parquet",
                    "brand_names": '["Taco Bell"]',
                    "metric_names": '["auv"]',
                    "as_of_date": "2024-12-31",
                    "publish_status": "blocked",
                    "confidence_score": 0.4,
                    "source_name": "QSR 50",
                    "source_url_or_doc_id": "doc-taco",
                    "metadata_json": "{}",
                    "citation_present": True,
                    "filter_match": True,
                    "text": "Taco Bell AUV blocked for external export because contradiction remains unresolved.",
                }
            ]
        )
        return RagSearchRun(
            retriever_name="bm25",
            results=results,
            status="ok",
            reason=None,
            latency_ms=1.0,
            index_size_bytes=123,
        )

    monkeypatch.setattr("qsr_audit.rag.benchmark.rag_search", _fake_rag_search)

    run = eval_rag_retrieval(
        settings=settings,
        corpus_path=corpus_path,
        benchmark_dir=benchmark_dir,
        retrievers=("bm25",),
        top_k=1,
    )

    row = run.metrics.loc[run.metrics["stage"] == "retrieval"].iloc[0]
    assert row["recall_at_k"] == pytest.approx(0.0)
    assert run.failure_cases[0]["query_id"] == "blocked-kpi"
    assert run.failure_cases[0]["failure_source"] in {"ranking", "retrieval"}


def test_eval_rag_retrieval_reranker_is_opt_in_and_skipped_in_ci(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_manual_corpus(corpus_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    _write_benchmark_pack(benchmark_dir)
    monkeypatch.setenv("CI", "true")

    run = eval_rag_retrieval(
        settings=settings,
        corpus_path=corpus_path,
        benchmark_dir=benchmark_dir,
        retrievers=("bm25",),
        top_k=2,
        reranker_name="rerank-cross-minilm",
    )

    reranked = run.metrics.loc[run.metrics["stage"] == "reranked"].iloc[0]
    assert reranked["status"] == "skipped"
    assert "disabled in CI" in reranked["status_reason"]
    assert run.artifacts.rerank_delta_csv_path is not None
    rerank_delta = pd.read_csv(run.artifacts.rerank_delta_csv_path)
    assert rerank_delta["status"].iloc[0] == "skipped"


@pytest.mark.parametrize("payload", ['"not-a-query-list"', "7"])
def test_eval_rag_retrieval_rejects_malformed_benchmark_json_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    payload: str,
) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_manual_corpus(corpus_path)
    benchmark_path = tmp_path / "benchmark.json"
    benchmark_path.write_text(payload, encoding="utf-8")
    _set_cli_env(monkeypatch, settings)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "eval-rag-retrieval",
            "--corpus-path",
            str(corpus_path),
            "--benchmark-path",
            str(benchmark_path),
            "--retriever",
            "bm25",
        ],
    )

    assert result.exit_code != 0
    assert "Benchmark JSON" in result.output


def test_eval_rag_retrieval_reranker_preserves_or_improves_ordering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    corpus = pd.DataFrame(
        [
            {
                "doc_id": "distractor-doc",
                "chunk_id": "distractor-doc::chunk-001",
                "source_kind": "gold_publish_decision",
                "title": "Distractor",
                "text": "Taco Bell blocked decision note export blocked blocked citation",
                "artifact_path": "data/gold/gold_publish_decisions.parquet",
                "brand_names": json.dumps(["Taco Bell"]),
                "metric_names": json.dumps(["auv"]),
                "as_of_date": "2024-12-31",
                "publish_status": "blocked",
                "confidence_score": 0.4,
                "source_name": "QSR 50",
                "source_url_or_doc_id": "doc-distractor",
                "metadata_json": json.dumps({}),
            },
            {
                "doc_id": "relevant-doc",
                "chunk_id": "relevant-doc::chunk-001",
                "source_kind": "gold_publish_decision",
                "title": "Relevant",
                "text": "Taco Bell AUV blocked for external export because contradiction remains unresolved.",
                "artifact_path": "data/gold/gold_publish_decisions.parquet",
                "brand_names": json.dumps(["Taco Bell"]),
                "metric_names": json.dumps(["auv"]),
                "as_of_date": "2024-12-31",
                "publish_status": "blocked",
                "confidence_score": 0.4,
                "source_name": "QSR 50",
                "source_url_or_doc_id": "doc-relevant",
                "metadata_json": json.dumps({}),
            },
        ]
    )
    corpus.to_parquet(corpus_path, index=False)
    benchmark_dir = tmp_path / "benchmark-pack"
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "query_id": "blocked-kpi",
                "query_text": "Which Taco Bell KPI rows are blocked for external export?",
                "language": "en",
                "notes": "Rerank comparison query.",
                "brand_filter": "Taco Bell",
                "metric_filter": "auv",
                "publish_status_scope": "blocked",
                "expected_source_kinds": "gold_publish_decision",
                "ambiguity_flag": "false",
                "requires_citation": "true",
            }
        ]
    ).to_csv(benchmark_dir / "queries.csv", index=False)
    pd.DataFrame(
        [
            {
                "query_id": "blocked-kpi",
                "doc_id": "relevant-doc",
                "chunk_id": "",
                "relevance_label": "highly_relevant",
                "rationale": "The relevant blocked KPI row should rank first after reranking.",
                "must_appear_in_top_k": "1",
            }
        ]
    ).to_csv(benchmark_dir / "judgments.csv", index=False)

    from qsr_audit.rag.retrieval import RagRerankRun

    def _fake_rerank_results(**kwargs):
        reranked = kwargs["results"].copy()
        reranked = reranked.sort_values(by=["doc_id"], ascending=[False], kind="mergesort")
        reranked = reranked.reset_index(drop=True)
        reranked["rank"] = range(1, len(reranked.index) + 1)
        return RagRerankRun(
            reranker_name="rerank-cross-minilm",
            results=reranked.head(kwargs["top_k"]),
            status="ok",
            reason=None,
            latency_ms=1.0,
        )

    monkeypatch.setattr("qsr_audit.rag.benchmark.rerank_results", _fake_rerank_results)

    run = eval_rag_retrieval(
        settings=settings,
        corpus_path=corpus_path,
        benchmark_dir=benchmark_dir,
        retrievers=("bm25",),
        top_k=1,
        reranker_name="rerank-cross-minilm",
    )

    rerank_delta = pd.read_csv(run.artifacts.rerank_delta_csv_path)
    assert rerank_delta["mrr_delta"].iloc[0] >= 0.0


def test_validate_rag_benchmark_artifacts_cannot_write_under_reports_or_strategy(
    tmp_path: Path,
) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    corpus = _write_manual_corpus(corpus_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    _write_benchmark_pack(benchmark_dir)

    with pytest.raises(ValueError, match="must not be written under analyst-facing paths"):
        validate_rag_benchmark_pack(
            benchmark_dir=benchmark_dir,
            corpus=corpus,
            settings=settings,
            output_root=settings.reports_dir,
        )


def test_inspect_rag_benchmark_query_returns_metadata_and_failure_source(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_manual_corpus(corpus_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    _write_benchmark_pack(benchmark_dir)

    payload = inspect_rag_benchmark_query(
        settings=settings,
        corpus_path=corpus_path,
        benchmark_dir=benchmark_dir,
        query_id="blocked-kpi",
        top_k=2,
    )

    assert payload["query_id"] == "blocked-kpi"
    assert "retrieved_chunks" in payload
    assert "expected_relevant_chunks" in payload
    assert "diagnosis" in payload
    assert "answer" not in payload


def test_init_rag_benchmark_creates_expected_pack_structure(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)

    run = init_rag_benchmark(
        name="analyst-pack",
        settings=settings,
        root_dir=tmp_path / "data" / "rag_benchmarks",
        authors=("Alice", "Bob"),
        notes="Draft benchmark pack.",
    )

    assert run.benchmark_dir.name == "analyst-pack"
    assert run.artifacts.metadata_path.exists()
    assert run.artifacts.readme_path.exists()
    assert run.artifacts.checklist_path.exists()
    assert run.artifacts.queries_path.exists()
    assert run.artifacts.judgments_path.exists()
    metadata = json.loads(run.artifacts.metadata_path.read_text(encoding="utf-8"))
    assert metadata["benchmark_version"] == "v1"
    assert metadata["authors"] == ["Alice", "Bob"]
    assert metadata["pack_status"] == "draft"
    assert metadata["corpus_manifest_path"].endswith("artifacts/rag/corpus/manifest.json")
    assert "reviewers/<name>/judgments.csv" in run.artifacts.readme_path.read_text(encoding="utf-8")


def test_bootstrap_rag_judgments_does_not_overwrite_final_judgments(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_manual_corpus(corpus_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    _write_benchmark_pack(benchmark_dir)
    original_judgments = (benchmark_dir / "judgments.csv").read_text(encoding="utf-8")

    run = bootstrap_rag_judgments(
        benchmark_dir=benchmark_dir,
        settings=settings,
        corpus_path=corpus_path,
        retriever_name="bm25",
        top_k=2,
    )

    assert run.artifacts.candidate_results_parquet_path.exists()
    assert run.artifacts.candidate_results_csv_path.exists()
    assert run.artifacts.judgment_workspace_csv_path.exists()
    assert run.artifacts.bootstrap_manifest_path.exists()
    assert (benchmark_dir / "judgments.csv").read_text(encoding="utf-8") == original_judgments
    workspace = pd.read_csv(
        run.artifacts.judgment_workspace_csv_path, dtype=str, keep_default_na=False
    )
    assert "suggestion_status" in workspace.columns
    assert "candidate_suggestion" in set(workspace["suggestion_status"])


def test_validate_rag_reviewer_file_fails_cleanly_on_malformed_submission(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_manual_corpus(corpus_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    _write_benchmark_pack(benchmark_dir)
    _write_reviewer_judgments(
        benchmark_dir,
        "alice",
        [
            {
                "query_id": "blocked-kpi",
                "doc_id": "gold-publish-decision-taco-bell-auv-blocked",
                "chunk_id": "",
                "relevance_label": "highly_relevant",
                "rationale": "",
                "must_appear_in_top_k": "top-two",
            }
        ],
    )

    run = validate_rag_reviewer_file(
        benchmark_dir=benchmark_dir,
        reviewer="alice",
        settings=settings,
        corpus_path=corpus_path,
    )

    assert not run.passed
    categories = {issue["category"] for issue in run.issues}
    assert "missing_rationale" in categories
    assert "invalid_must_appear_threshold" in categories
    assert run.artifacts.validation_json_path.exists()
    assert run.pack.judgments_path.name == "judgments.csv"
    assert "reviewer-alice" in str(run.artifacts.validation_markdown_path)


def test_adjudicate_rag_benchmark_detects_conflicts_and_keeps_pack_in_review(
    tmp_path: Path,
) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_manual_corpus(corpus_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    _write_benchmark_pack(benchmark_dir)
    _write_reviewer_judgments(
        benchmark_dir,
        "alice",
        [
            {
                "query_id": "blocked-kpi",
                "doc_id": "gold-publish-decision-taco-bell-auv-blocked",
                "chunk_id": "",
                "relevance_label": "highly_relevant",
                "rationale": "This should be a direct hit.",
                "must_appear_in_top_k": "1",
            }
        ],
    )
    _write_reviewer_judgments(
        benchmark_dir,
        "bob",
        [
            {
                "query_id": "blocked-kpi",
                "doc_id": "gold-publish-decision-taco-bell-auv-blocked",
                "chunk_id": "",
                "relevance_label": "relevant",
                "rationale": "Still relevant, but weaker.",
                "must_appear_in_top_k": "2",
            }
        ],
    )

    with pytest.raises(ValueError, match="Reviewer conflicts remain unresolved"):
        adjudicate_rag_benchmark(
            benchmark_dir=benchmark_dir,
            settings=settings,
            corpus_path=corpus_path,
            force=False,
        )

    adjudication_root = settings.artifacts_dir / "rag" / "benchmarks" / "adjudication"
    conflict_runs = list(adjudication_root.iterdir())
    assert len(conflict_runs) == 1
    conflicts = pd.read_csv(conflict_runs[0] / "conflicts.csv", dtype=str, keep_default_na=False)
    assert "relevance_label_conflict" in conflicts["conflict_type"].iloc[0]
    assert not (benchmark_dir / "adjudicated_judgments.csv").exists()
    metadata = json.loads((benchmark_dir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["pack_status"] == "in_review"


def test_adjudicate_rag_benchmark_single_reviewer_stays_provisional(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_manual_corpus(corpus_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    _write_benchmark_pack(benchmark_dir)
    _write_reviewer_judgments(
        benchmark_dir,
        "alice",
        [
            {
                "query_id": "blocked-kpi",
                "doc_id": "gold-publish-decision-taco-bell-auv-blocked",
                "chunk_id": "",
                "relevance_label": "highly_relevant",
                "rationale": "Single reviewer judgment.",
                "must_appear_in_top_k": "1",
            }
        ],
    )

    run = adjudicate_rag_benchmark(
        benchmark_dir=benchmark_dir,
        settings=settings,
        corpus_path=corpus_path,
        force=False,
    )

    assert run.metadata["pack_status"] == "in_review"
    assert run.artifacts.adjudicated_judgments_path is None
    assert run.agreement_summary["minimum_reviewer_coverage_met"] is False
    assert run.agreement_summary["true_adjudication_eligible"] is False
    metadata = json.loads((benchmark_dir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["pack_status"] == "in_review"
    assert "at least 2 reviewer submissions" in metadata["notes"]


def test_adjudicate_rag_benchmark_force_single_reviewer_is_clearly_provisional(
    tmp_path: Path,
) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_manual_corpus(corpus_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    _write_benchmark_pack(benchmark_dir)
    _write_reviewer_judgments(
        benchmark_dir,
        "alice",
        [
            {
                "query_id": "blocked-kpi",
                "doc_id": "gold-publish-decision-taco-bell-auv-blocked",
                "chunk_id": "",
                "relevance_label": "highly_relevant",
                "rationale": "Single reviewer judgment.",
                "must_appear_in_top_k": "1",
            }
        ],
    )

    run = adjudicate_rag_benchmark(
        benchmark_dir=benchmark_dir,
        settings=settings,
        corpus_path=corpus_path,
        force=True,
    )

    assert run.metadata["pack_status"] == "in_review"
    assert run.artifacts.adjudicated_judgments_path is not None
    assert run.agreement_summary["forced_provisional"] is True
    assert "Forced provisional adjudication artifact" in run.metadata["notes"]


def test_adjudicate_rag_benchmark_canonicalizes_equivalent_reviewer_values(
    tmp_path: Path,
) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_manual_corpus(corpus_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    _write_benchmark_pack(benchmark_dir)
    _write_reviewer_judgments(
        benchmark_dir,
        "alice",
        [
            {
                "query_id": "blocked-kpi",
                "doc_id": "gold-publish-decision-taco-bell-auv-blocked",
                "chunk_id": "",
                "relevance_label": "Relevant",
                "rationale": "Equivalent normalized label.",
                "must_appear_in_top_k": "01",
            }
        ],
    )
    _write_reviewer_judgments(
        benchmark_dir,
        "bob",
        [
            {
                "query_id": "blocked-kpi",
                "doc_id": "gold-publish-decision-taco-bell-auv-blocked",
                "chunk_id": "",
                "relevance_label": "relevant",
                "rationale": "Equivalent normalized label.",
                "must_appear_in_top_k": "1",
            }
        ],
    )

    run = adjudicate_rag_benchmark(
        benchmark_dir=benchmark_dir,
        settings=settings,
        corpus_path=corpus_path,
        force=False,
    )

    assert run.conflict_count == 0
    assert run.metadata["pack_status"] == "adjudicated"
    adjudicated = pd.read_csv(
        run.artifacts.adjudicated_judgments_path, dtype=str, keep_default_na=False
    )
    row = adjudicated.iloc[0]
    assert row["relevance_label"] == "relevant"
    assert row["must_appear_in_top_k"] == "1"


def test_adjudicate_rag_benchmark_preserves_doc_and_chunk_semantics(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_multi_chunk_doc_corpus(corpus_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "query_id": "doc-level",
                "query_text": "Find the blocked Taco Bell decision document.",
                "language": "en",
                "notes": "",
                "brand_filter": "Taco Bell",
                "metric_filter": "auv",
                "publish_status_scope": "blocked",
                "expected_source_kinds": "gold_publish_decision",
                "ambiguity_flag": "false",
                "requires_citation": "true",
            },
            {
                "query_id": "chunk-level",
                "query_text": "Find the appendix chunk for Taco Bell.",
                "language": "en",
                "notes": "",
                "brand_filter": "Taco Bell",
                "metric_filter": "auv",
                "publish_status_scope": "blocked",
                "expected_source_kinds": "gold_publish_decision",
                "ambiguity_flag": "false",
                "requires_citation": "true",
            },
        ]
    ).to_csv(benchmark_dir / "queries.csv", index=False)
    pd.DataFrame(columns=JUDGMENT_COLUMNS).to_csv(benchmark_dir / "judgments.csv", index=False)
    pd.DataFrame(columns=["query_id", "filter_key", "filter_value", "notes"]).to_csv(
        benchmark_dir / "filters.csv", index=False
    )
    pd.DataFrame(columns=["query_id", "query_group", "notes"]).to_csv(
        benchmark_dir / "query_groups.csv", index=False
    )
    (benchmark_dir / "metadata.json").write_text(
        json.dumps(
            {
                "benchmark_version": "v1",
                "created_at": "2026-04-06T00:00:00+00:00",
                "corpus_manifest_path": "artifacts/rag/corpus/manifest.json",
                "authors": ["Alice", "Bob"],
                "pack_status": "draft",
                "notes": "",
            }
        ),
        encoding="utf-8",
    )
    reviewer_rows = [
        {
            "query_id": "doc-level",
            "doc_id": "gold-publish-decision-taco-bell-auv-blocked",
            "chunk_id": "",
            "relevance_label": "highly_relevant",
            "rationale": "Any chunk from the document is acceptable.",
            "must_appear_in_top_k": "1",
        },
        {
            "query_id": "chunk-level",
            "doc_id": "",
            "chunk_id": "gold-publish-decision-taco-bell-auv-blocked::chunk-002",
            "relevance_label": "highly_relevant",
            "rationale": "Only the appendix chunk should satisfy this query.",
            "must_appear_in_top_k": "1",
        },
    ]
    _write_reviewer_judgments(benchmark_dir, "alice", reviewer_rows)
    _write_reviewer_judgments(benchmark_dir, "bob", reviewer_rows)

    run = adjudicate_rag_benchmark(
        benchmark_dir=benchmark_dir,
        settings=settings,
        corpus_path=corpus_path,
        force=False,
    )

    adjudicated = pd.read_csv(
        run.artifacts.adjudicated_judgments_path, dtype=str, keep_default_na=False
    )
    doc_row = adjudicated.loc[adjudicated["query_id"] == "doc-level"].iloc[0]
    chunk_row = adjudicated.loc[adjudicated["query_id"] == "chunk-level"].iloc[0]
    assert doc_row["doc_id"] == "gold-publish-decision-taco-bell-auv-blocked"
    assert doc_row["chunk_id"] == ""
    assert chunk_row["doc_id"] == ""
    assert chunk_row["chunk_id"] == "gold-publish-decision-taco-bell-auv-blocked::chunk-002"
    assert run.metadata["pack_status"] == "adjudicated"


def test_summarize_rag_benchmark_authoring_is_deterministic(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    _write_benchmark_pack(benchmark_dir)
    summary_one = summarize_rag_benchmark_authoring(
        benchmark_dir=benchmark_dir,
        settings=settings,
    )
    summary_two = summarize_rag_benchmark_authoring(
        benchmark_dir=benchmark_dir,
        settings=settings,
    )

    one = dict(summary_one.summary)
    two = dict(summary_two.summary)
    one.pop("built_at_utc", None)
    two.pop("built_at_utc", None)
    assert one == two
    markdown = summary_one.artifacts.summary_markdown_path.read_text(encoding="utf-8")
    assert "## Unjudged Queries" in markdown
    assert "## Under-Covered Query Groups" in markdown
    assert "## Query Groups Without Hard Negatives" in markdown
    assert summary_one.artifacts.summary_json_path.exists()
    assert summary_one.artifacts.coverage_rows_csv_path.exists()
    assert not str(summary_one.artifacts.summary_json_path).startswith(str(settings.reports_dir))
    assert not str(summary_one.artifacts.summary_json_path).startswith(str(settings.strategy_dir))


def test_eval_rag_retrieval_prefers_adjudicated_judgments_when_present(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_manual_corpus(corpus_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    _write_benchmark_pack(benchmark_dir)
    pd.DataFrame(columns=JUDGMENT_COLUMNS).to_csv(benchmark_dir / "judgments.csv", index=False)
    pd.DataFrame(
        [
            {
                "query_id": "blocked-kpi",
                "doc_id": "gold-publish-decision-taco-bell-auv-blocked",
                "chunk_id": "",
                "relevance_label": "highly_relevant",
                "rationale": "Adjudicated blocked KPI decision.",
                "must_appear_in_top_k": "1",
            }
        ]
    ).to_csv(benchmark_dir / "adjudicated_judgments.csv", index=False)
    (benchmark_dir / "metadata.json").write_text(
        json.dumps(
            {
                "benchmark_version": "v1",
                "created_at": "2026-04-06T00:00:00+00:00",
                "corpus_manifest_path": "artifacts/rag/corpus/manifest.json",
                "authors": ["Alice"],
                "pack_status": "adjudicated",
                "notes": "",
            }
        ),
        encoding="utf-8",
    )

    run = eval_rag_retrieval(
        settings=settings,
        corpus_path=corpus_path,
        benchmark_dir=benchmark_dir,
        retrievers=("bm25",),
        top_k=2,
    )

    assert run.summary["judgments_source"] == "adjudicated_judgments.csv"
    assert run.summary["benchmark_pack_status"] == "adjudicated"
    assert run.metrics.loc[run.metrics["retriever_name"] == "bm25"].iloc[0]["status"] == "ok"


def test_eval_rag_retrieval_ignores_provisional_adjudicated_file(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_manual_corpus(corpus_path)
    benchmark_dir = tmp_path / "benchmark-pack"
    _write_benchmark_pack(benchmark_dir)
    pd.DataFrame(
        [
            {
                "query_id": "blocked-kpi",
                "doc_id": "gold-publish-decision-taco-bell-auv-blocked",
                "chunk_id": "",
                "relevance_label": "highly_relevant",
                "rationale": "Forced provisional adjudication row.",
                "must_appear_in_top_k": "1",
            }
        ]
    ).to_csv(benchmark_dir / "adjudicated_judgments.csv", index=False)
    (benchmark_dir / "metadata.json").write_text(
        json.dumps(
            {
                "benchmark_version": "v1",
                "created_at": "2026-04-06T00:00:00+00:00",
                "corpus_manifest_path": "artifacts/rag/corpus/manifest.json",
                "authors": ["Alice"],
                "pack_status": "in_review",
                "notes": "Forced provisional adjudication artifact generated without satisfying the normal adjudication requirements.",
            }
        ),
        encoding="utf-8",
    )

    run = eval_rag_retrieval(
        settings=settings,
        corpus_path=corpus_path,
        benchmark_dir=benchmark_dir,
        retrievers=("bm25",),
        top_k=2,
    )

    assert run.summary["judgments_source"] == "judgments.csv"
    assert run.summary["benchmark_pack_status"] == "in_review"
    assert any(
        "provisional adjudicated_judgments.csv exists" in warning
        for warning in run.summary["benchmark_warnings"]
    )
