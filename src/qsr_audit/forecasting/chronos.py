"""Opt-in Chronos experiment guard for offline forecast benchmarking."""

from __future__ import annotations

import os
from dataclasses import dataclass
from importlib.util import find_spec
from pathlib import Path

import pandas as pd

from qsr_audit.models import get_forecasting_candidates


@dataclass(frozen=True)
class ChronosExperimentStatus:
    """Readiness status for a future Chronos benchmark."""

    enabled: bool
    runnable: bool
    reason: str
    candidate_repo_ids: tuple[str, ...]
    panel_path: str


def prepare_chronos_experiment(
    panel_path: Path,
    *,
    enabled: bool = False,
) -> ChronosExperimentStatus:
    """Guard Chronos experiments behind explicit opt-in and panel readiness checks."""

    candidates = tuple(candidate.repo_id for candidate in get_forecasting_candidates())
    resolved_panel_path = panel_path.expanduser().resolve()

    if not enabled:
        return ChronosExperimentStatus(
            enabled=False,
            runnable=False,
            reason="Chronos experiments are opt-in and disabled by default in this scaffold.",
            candidate_repo_ids=candidates,
            panel_path=str(resolved_panel_path),
        )

    if os.environ.get("CI", "").strip().lower() == "true":
        return ChronosExperimentStatus(
            enabled=True,
            runnable=False,
            reason="Chronos experiments are disabled in CI to avoid automatic model downloads.",
            candidate_repo_ids=candidates,
            panel_path=str(resolved_panel_path),
        )

    if not resolved_panel_path.exists():
        return ChronosExperimentStatus(
            enabled=True,
            runnable=False,
            reason="Chronos benchmarking requires an existing longitudinal panel parquet.",
            candidate_repo_ids=candidates,
            panel_path=str(resolved_panel_path),
        )

    panel = pd.read_parquet(resolved_panel_path)
    if panel.empty or panel["as_of_date"].nunique() < 3:
        return ChronosExperimentStatus(
            enabled=True,
            runnable=False,
            reason="Chronos benchmarking requires a longitudinal panel with at least 3 periods.",
            candidate_repo_ids=candidates,
            panel_path=str(resolved_panel_path),
        )

    if find_spec("torch") is None or find_spec("transformers") is None:
        return ChronosExperimentStatus(
            enabled=True,
            runnable=False,
            reason=(
                "Chronos runtime dependencies are not installed. This scaffold records readiness only "
                "and does not auto-install or auto-download model weights."
            ),
            candidate_repo_ids=candidates,
            panel_path=str(resolved_panel_path),
        )

    return ChronosExperimentStatus(
        enabled=True,
        runnable=True,
        reason="Chronos experiment guard passed. Model execution remains offline and experimental only.",
        candidate_repo_ids=candidates,
        panel_path=str(resolved_panel_path),
    )


__all__ = ["ChronosExperimentStatus", "prepare_chronos_experiment"]
