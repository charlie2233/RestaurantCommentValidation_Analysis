from __future__ import annotations

from pathlib import Path

import pandas as pd
from qsr_audit.rag import eval_rag_retrieval, validate_rag_benchmark_pack

from tests.helpers import build_settings

JUDGMENT_COLUMNS = [
    "query_id",
    "doc_id",
    "chunk_id",
    "relevance_label",
    "rationale",
    "must_appear_in_top_k",
]


def _benchmark_pack_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "rag_benchmarks" / "2026q2_pack"


def _write_tiny_eval_corpus(path: Path) -> None:
    corpus = pd.DataFrame(
        [
            {
                "doc_id": "gold-publish-decision-taco-bell-auv-blocked",
                "chunk_id": "gold-publish-decision-taco-bell-auv-blocked::chunk-001",
                "source_kind": "gold_publish_decision",
                "title": "Gold publish decision - Taco Bell - auv",
                "text": "Taco Bell AUV blocked for external export because contradiction remains unresolved.",
                "artifact_path": "data/gold/gold_publish_decisions.parquet",
                "brand_names": '["Taco Bell"]',
                "metric_names": '["auv"]',
                "as_of_date": "2024-12-31",
                "publish_status": "blocked",
                "confidence_score": 0.4,
                "source_name": "QSR 50",
                "source_url_or_doc_id": "doc-taco",
                "metadata_json": '{"severity": "warning"}',
            },
            {
                "doc_id": "gold-provenance-taco-bell",
                "chunk_id": "gold-provenance-taco-bell::chunk-001",
                "source_kind": "gold_provenance_registry",
                "title": "Provenance record - Taco Bell",
                "text": "Provenance record for Taco Bell from QSR 50 with annual ranking notes.",
                "artifact_path": "data/gold/provenance_registry.parquet",
                "brand_names": '["Taco Bell"]',
                "metric_names": "[]",
                "as_of_date": "2024-12-31",
                "publish_status": None,
                "confidence_score": 0.92,
                "source_name": "QSR 50",
                "source_url_or_doc_id": "doc-taco",
                "metadata_json": '{"severity": "info"}',
            },
            {
                "doc_id": "gold-validation-flag-auv",
                "chunk_id": "gold-validation-flag-auv::chunk-001",
                "source_kind": "gold_validation_flag",
                "title": "Validation finding - error - Taco Bell",
                "text": "Validation finding with severity error. Taco Bell implied AUV conflicts with recorded AUV.",
                "artifact_path": "data/gold/validation_flags.parquet",
                "brand_names": '["Taco Bell"]',
                "metric_names": '["average_unit_volume_usd_thousands"]',
                "as_of_date": None,
                "publish_status": None,
                "confidence_score": None,
                "source_name": "validation_flags",
                "source_url_or_doc_id": None,
                "metadata_json": '{"severity": "error"}',
            },
        ]
    )
    corpus.to_parquet(path, index=False)


def test_first_cycle_pack_validates_and_keeps_reviewer_starters_in_place(
    tmp_path: Path,
) -> None:
    pack_dir = _benchmark_pack_dir()
    queries = pd.read_csv(pack_dir / "queries.csv", dtype=str, keep_default_na=False)

    assert 12 <= len(queries.index) <= 20
    assert {
        "starter-starbucks-auv-contradiction",
        "starter-sweetgreen-extra-ai-row",
        "starter-taco-bell-blocked-vs-publishable",
    }.issubset(set(queries["query_id"]))

    validation = validate_rag_benchmark_pack(
        benchmark_dir=pack_dir,
        corpus=pd.DataFrame(),
        settings=build_settings(tmp_path),
        require_judgments=False,
    )

    assert validation.passed
    assert validation.issues == []
    assert validation.pack.metadata["pack_status"] == "in_review"
    assert len(validation.query_specs) == len(queries.index)

    for reviewer in ("alice", "bob"):
        reviewer_path = pack_dir / "reviewers" / reviewer / "judgments.csv"
        assert reviewer_path.exists(), f"Missing reviewer starter file: {reviewer_path}"
        reviewer_frame = pd.read_csv(reviewer_path, dtype=str, keep_default_na=False)
        assert list(reviewer_frame.columns[: len(JUDGMENT_COLUMNS)]) == JUDGMENT_COLUMNS
        assert {"query_text", "notes", "review_state", "starter_note"}.issubset(
            reviewer_frame.columns
        )
        assert len(reviewer_frame.index) == len(queries.index)
        assert reviewer_frame["query_id"].str.startswith("starter-").all()
        assert reviewer_frame["review_state"].eq("draft").all()
        assert reviewer_frame["starter_note"].str.startswith("PROVISIONAL starter row").all()
        assert reviewer_path.parent.name == reviewer
        assert reviewer_path.parent.parent.name == "reviewers"


def test_draft_pack_eval_warns_and_writes_benchmark_artifacts_outside_reports_and_strategy(
    tmp_path: Path,
) -> None:
    settings = build_settings(tmp_path)
    corpus_path = settings.artifacts_dir / "rag" / "corpus" / "corpus.parquet"
    corpus_path.parent.mkdir(parents=True, exist_ok=True)
    _write_tiny_eval_corpus(corpus_path)

    run = eval_rag_retrieval(
        settings=settings,
        corpus_path=corpus_path,
        benchmark_dir=_benchmark_pack_dir(),
        retrievers=("bm25",),
        top_k=2,
    )

    assert run.summary["benchmark_pack_status"] == "in_review"
    assert run.summary["benchmark_run_status"] == "provisional"
    assert run.summary["judgments_source"] == "judgments.csv"
    assert run.summary["benchmark_warnings"]
    assert run.summary["failure_case_count"] == 0
    assert run.summary["failure_cases"] == []
    assert any(
        "draft or single-reviewer judgments" in warning
        for warning in run.summary["benchmark_warnings"]
    )
    assert any(
        "Benchmark pack status is `in_review`" in warning
        for warning in run.summary["benchmark_warnings"]
    )

    summary_text = run.artifacts.summary_markdown_path.read_text(encoding="utf-8")
    assert "Benchmark is provisional until the pack is adjudicated." in summary_text
    assert "Run ID:" in summary_text
    assert "## Failure Cases\n\n- None." in summary_text

    failure_text = run.artifacts.failure_cases_markdown_path.read_text(encoding="utf-8")
    assert failure_text.strip() == "# RAG Retrieval Failure Cases\n\n- None."

    run_root = run.artifacts.summary_markdown_path.parent
    assert run_root == run.artifacts.failure_cases_markdown_path.parent
    assert run_root == run.artifacts.results_parquet_path.parent
    assert str(run_root).startswith(str(settings.artifacts_dir / "rag" / "benchmarks"))
    assert not run_root.is_relative_to(settings.reports_dir)
    assert not run_root.is_relative_to(settings.strategy_dir)

    assert run.artifacts.summary_markdown_path.exists()
    assert run.artifacts.failure_cases_markdown_path.exists()
    assert run.artifacts.results_parquet_path.exists()
    assert run.artifacts.metrics_json_path.exists()
    assert run.artifacts.query_bucket_metrics_csv_path.exists()
