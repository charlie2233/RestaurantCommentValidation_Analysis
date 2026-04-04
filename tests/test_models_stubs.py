"""Tests for research-only model metadata stubs."""

from __future__ import annotations

import importlib

from qsr_audit.models import (
    DEFAULT_RESEARCH_BOUNDARY,
    get_asr_candidates,
    get_embedding_candidates,
    get_forecasting_candidates,
)
from qsr_audit.models.stubs import get_experiment_plan


def test_models_package_importable() -> None:
    module = importlib.import_module("qsr_audit.models")
    assert module is not None


def test_research_boundary_is_non_production() -> None:
    assert DEFAULT_RESEARCH_BOUNDARY.reads_from_validated_layers_only is True
    assert DEFAULT_RESEARCH_BOUNDARY.production_cli_integrated is False
    assert DEFAULT_RESEARCH_BOUNDARY.may_write_analyst_outputs is False
    assert DEFAULT_RESEARCH_BOUNDARY.allowed_input_layers == ("silver", "gold")


def test_candidate_groups_expose_expected_repo_ids() -> None:
    assert [candidate.repo_id for candidate in get_forecasting_candidates()] == [
        "amazon/chronos-bolt-small",
        "amazon/chronos-bolt-base",
        "amazon/chronos-t5-small",
    ]
    assert [candidate.repo_id for candidate in get_asr_candidates()] == [
        "distil-whisper/distil-large-v3",
        "openai/whisper-large-v3-turbo",
        "distil-whisper/distil-small.en",
    ]
    assert [candidate.repo_id for candidate in get_embedding_candidates()] == [
        "sentence-transformers/all-MiniLM-L6-v2",
        "BAAI/bge-small-en-v1.5",
        "intfloat/e5-small-v2",
        "intfloat/multilingual-e5-small",
    ]


def test_experiment_plans_match_docs_and_status() -> None:
    forecasting = get_experiment_plan("forecasting")
    rag = get_experiment_plan("embedding_rag")
    asr = get_experiment_plan("asr")

    assert forecasting.status == "blocked"
    assert forecasting.entrypoint_doc == "docs/forecasting_experiments.md"
    assert rag.entrypoint_doc == "docs/rag_experiments.md"
    assert asr.entrypoint_doc == "docs/model_candidates.md"
