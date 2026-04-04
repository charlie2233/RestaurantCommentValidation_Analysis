"""Research-only model candidate metadata and offline experiment helpers."""

from qsr_audit.models.protocols import ExperimentPlan, ModelCandidate, ResearchBoundary
from qsr_audit.models.stubs import (
    DEFAULT_RESEARCH_BOUNDARY,
    get_asr_candidates,
    get_embedding_candidates,
    get_forecasting_candidates,
)

__all__ = [
    "DEFAULT_RESEARCH_BOUNDARY",
    "ExperimentPlan",
    "ModelCandidate",
    "ResearchBoundary",
    "get_asr_candidates",
    "get_embedding_candidates",
    "get_forecasting_candidates",
]
