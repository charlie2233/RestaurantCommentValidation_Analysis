"""Research-only candidate registries and offline evaluation plans."""

from __future__ import annotations

from qsr_audit.models.protocols import ExperimentPlan, ModelCandidate, ResearchBoundary

DEFAULT_RESEARCH_BOUNDARY = ResearchBoundary(
    reads_from_validated_layers_only=True,
    allowed_input_layers=("silver", "gold"),
    production_cli_integrated=False,
    may_write_analyst_outputs=False,
    notes=(
        "Research helpers may read validated local snapshots for offline evaluation, "
        "but they must not gate validation, reconciliation, reporting, strategy, or "
        "executive-facing outputs."
    ),
)

_FORECASTING_CANDIDATES: tuple[ModelCandidate, ...] = (
    ModelCandidate(
        repo_id="amazon/chronos-bolt-small",
        task_family="forecasting",
        summary="Small Chronos Bolt checkpoint for first-pass zero-shot forecasting.",
        license_name="apache-2.0",
        source_url="https://hf.co/amazon/chronos-bolt-small",
        size_hint="47.7M params",
        preferred_role="default lightweight forecasting benchmark",
    ),
    ModelCandidate(
        repo_id="amazon/chronos-bolt-base",
        task_family="forecasting",
        summary="Larger Bolt-family checkpoint for a second-pass comparison.",
        license_name="apache-2.0",
        source_url="https://hf.co/amazon/chronos-bolt-base",
        size_hint="205M params",
        preferred_role="follow-up forecasting benchmark",
    ),
    ModelCandidate(
        repo_id="amazon/chronos-t5-small",
        task_family="forecasting",
        summary="Original Chronos-family small T5 control for comparison.",
        license_name="apache-2.0",
        source_url="https://hf.co/amazon/chronos-t5-small",
        size_hint="46.2M params",
        preferred_role="historical forecasting control",
    ),
)

_ASR_CANDIDATES: tuple[ModelCandidate, ...] = (
    ModelCandidate(
        repo_id="distil-whisper/distil-large-v3",
        task_family="asr",
        summary="Distilled English Whisper-family checkpoint for fast ASR experiments.",
        license_name="mit",
        source_url="https://hf.co/distil-whisper/distil-large-v3",
        size_hint="distilled large-v3 family",
        preferred_role="first English ASR benchmark",
    ),
    ModelCandidate(
        repo_id="openai/whisper-large-v3-turbo",
        task_family="asr",
        summary="Multilingual Whisper-family control with strong speed/quality tradeoff.",
        license_name="apache-2.0",
        source_url="https://hf.co/openai/whisper-large-v3-turbo",
        size_hint="large-v3 turbo",
        preferred_role="baseline multilingual ASR control",
    ),
    ModelCandidate(
        repo_id="distil-whisper/distil-small.en",
        task_family="asr",
        summary="Smaller English-only Distil-Whisper fallback for budget experiments.",
        license_name="mit",
        source_url="https://hf.co/distil-whisper/distil-small.en",
        size_hint="166M params",
        preferred_role="budget ASR fallback",
    ),
)

_EMBEDDING_CANDIDATES: tuple[ModelCandidate, ...] = (
    ModelCandidate(
        repo_id="sentence-transformers/all-MiniLM-L6-v2",
        task_family="embedding_rag",
        summary="Very small dense retrieval baseline for local experiments.",
        license_name="apache-2.0",
        source_url="https://hf.co/sentence-transformers/all-MiniLM-L6-v2",
        size_hint="22.7M params",
        preferred_role="default low-friction embedding baseline",
    ),
    ModelCandidate(
        repo_id="BAAI/bge-small-en-v1.5",
        task_family="embedding_rag",
        summary="Lightweight English retrieval model with strong community uptake.",
        license_name="mit",
        source_url="https://hf.co/BAAI/bge-small-en-v1.5",
        size_hint="33.4M params",
        preferred_role="quality-focused dense retrieval candidate",
    ),
    ModelCandidate(
        repo_id="intfloat/e5-small-v2",
        task_family="embedding_rag",
        summary="Compact embedding candidate for retrieval behavior comparisons.",
        license_name="mit",
        source_url="https://hf.co/intfloat/e5-small-v2",
        size_hint="small",
        preferred_role="alternative embedding baseline",
    ),
    ModelCandidate(
        repo_id="intfloat/multilingual-e5-small",
        task_family="embedding_rag",
        summary="Lightweight multilingual embedding fallback for non-English corpora.",
        license_name="mit",
        source_url="https://hf.co/intfloat/multilingual-e5-small",
        size_hint="small multilingual",
        preferred_role="multilingual embedding fallback",
    ),
)

_EXPERIMENT_PLANS: dict[str, ExperimentPlan] = {
    "forecasting": ExperimentPlan(
        task_family="forecasting",
        entrypoint_doc="docs/forecasting_experiments.md",
        status="blocked",
        prerequisites=(
            "Repeated-period Gold series exist",
            "Simple non-ML forecasting baselines are defined",
            "Time-based train/validation/test splits are available",
        ),
        success_criteria=(
            "Beat naive baselines on held-out periods",
            "Remain fully downstream of Gold metrics",
        ),
    ),
    "embedding_rag": ExperimentPlan(
        task_family="embedding_rag",
        entrypoint_doc="docs/rag_experiments.md",
        status="planned",
        prerequisites=(
            "Gold and provenance documents selected for indexing",
            "Analyst-authored retrieval benchmark exists",
        ),
        success_criteria=(
            "Retrieval quality exceeds lexical baseline on benchmark queries",
            "Pipeline boundaries stay unchanged",
        ),
    ),
    "asr": ExperimentPlan(
        task_family="asr",
        entrypoint_doc="docs/model_candidates.md",
        status="planned",
        prerequisites=(
            "A concrete audio transcription use case exists",
            "Offline audio evaluation set exists",
        ),
        success_criteria=(
            "Latency and quality are acceptable for the use case",
            "No production dependency is introduced",
        ),
    ),
}


def get_forecasting_candidates() -> tuple[ModelCandidate, ...]:
    """Return forecasting candidates in the recommended evaluation order."""

    return _FORECASTING_CANDIDATES


def get_asr_candidates() -> tuple[ModelCandidate, ...]:
    """Return research-only ASR candidates."""

    return _ASR_CANDIDATES


def get_embedding_candidates() -> tuple[ModelCandidate, ...]:
    """Return lightweight embedding candidates for retrieval experiments."""

    return _EMBEDDING_CANDIDATES


def get_experiment_plan(task_family: str) -> ExperimentPlan:
    """Return the offline experiment plan metadata for a task family."""

    return _EXPERIMENT_PLANS[task_family]
