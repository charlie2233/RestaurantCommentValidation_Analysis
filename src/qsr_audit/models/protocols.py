"""Typed research-only protocols for future model experimentation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

TaskFamily = Literal["forecasting", "asr", "embedding_rag"]


@dataclass(frozen=True)
class ResearchBoundary:
    """Hard boundaries for model experimentation in this repository."""

    reads_from_validated_layers_only: bool
    allowed_input_layers: tuple[str, ...]
    production_cli_integrated: bool
    may_write_analyst_outputs: bool
    notes: str


@dataclass(frozen=True)
class ModelCandidate:
    """Metadata stub for a candidate model under evaluation."""

    repo_id: str
    task_family: TaskFamily
    summary: str
    license_name: str
    source_url: str
    size_hint: str
    preferred_role: str


@dataclass(frozen=True)
class ExperimentPlan:
    """Lightweight plan metadata for offline model experiments."""

    task_family: TaskFamily
    entrypoint_doc: str
    status: Literal["planned", "blocked", "ready_for_offline_eval"]
    prerequisites: tuple[str, ...]
    success_criteria: tuple[str, ...]
