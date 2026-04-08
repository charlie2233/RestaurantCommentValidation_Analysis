"""Repository hygiene regressions."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "check_repo_hygiene.py"


def _load_hygiene_module():
    spec = importlib.util.spec_from_file_location("check_repo_hygiene", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_check_repo_hygiene_rejects_workstation_specific_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_hygiene_module()
    tracked_file = tmp_path / "docs" / "starter.md"
    tracked_file.parent.mkdir(parents=True, exist_ok=True)
    tracked_file.write_text(
        "See /Users/alice/RestaurantAnalysis/docs/runbook.md\n", encoding="utf-8"
    )

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(module, "_tracked_files", lambda: ["docs/starter.md"])

    assert module.main() == 1
    captured = capsys.readouterr()
    assert "workstation-specific path '/Users/'" in captured.err
    assert "docs/starter.md" in captured.err


def test_check_repo_hygiene_rejects_malformed_placeholder_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_hygiene_module()
    tracked_file = tmp_path / "docs" / "starter.md"
    tracked_file.parent.mkdir(parents=True, exist_ok=True)
    tracked_file.write_text("Use reviewers//judgments.csv during review.\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(module, "_tracked_files", lambda: ["docs/starter.md"])

    assert module.main() == 1
    captured = capsys.readouterr()
    assert "malformed placeholder path 'reviewers//judgments.csv'" in captured.err
    assert "docs/starter.md" in captured.err


def test_check_repo_hygiene_ignores_legitimate_urls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_hygiene_module()
    tracked_file = tmp_path / "docs" / "links.md"
    tracked_file.parent.mkdir(parents=True, exist_ok=True)
    tracked_file.write_text(
        "Reference https://example.com/Users/guide for context.\n", encoding="utf-8"
    )

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(module, "_tracked_files", lambda: ["docs/links.md"])

    assert module.main() == 0
    captured = capsys.readouterr()
    assert "passed" in captured.out


def test_check_repo_hygiene_allows_committed_reference_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_hygiene_module()
    tracked_file = tmp_path / "data" / "reference" / "qsr50_reference.csv"
    tracked_file.parent.mkdir(parents=True, exist_ok=True)
    tracked_file.write_text("brand_name,source_name\nStarbucks,QSR 50 2025\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(module, "_tracked_files", lambda: ["data/reference/qsr50_reference.csv"])

    assert module.main() == 0
    captured = capsys.readouterr()
    assert "passed" in captured.out


def test_check_repo_hygiene_allows_explicit_primary_source_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_hygiene_module()
    coverage_file = tmp_path / "reports" / "reconciliation" / "primary_source_coverage.md"
    gold_file = tmp_path / "data" / "gold" / "primary_source_gold_candidates.parquet"
    coverage_file.parent.mkdir(parents=True, exist_ok=True)
    gold_file.parent.mkdir(parents=True, exist_ok=True)
    coverage_file.write_text("# Primary Source Coverage\n", encoding="utf-8")
    gold_file.write_bytes(b"PAR1")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        module,
        "_tracked_files",
        lambda: [
            "reports/reconciliation/primary_source_coverage.md",
            "data/gold/primary_source_gold_candidates.parquet",
        ],
    )

    assert module.main() == 0
    captured = capsys.readouterr()
    assert "passed" in captured.out
