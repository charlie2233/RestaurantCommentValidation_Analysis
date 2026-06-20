"""Makefile workflow regressions."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _make_target_commands(target_name: str) -> list[str]:
    makefile = REPO_ROOT / "Makefile"
    lines = makefile.read_text(encoding="utf-8").splitlines()
    target_header = f"{target_name}:"
    start = lines.index(target_header) + 1
    commands: list[str] = []
    for line in lines[start:]:
        if line and not line.startswith(("\t", " ")):
            break
        if line.startswith("\t"):
            commands.append(line.strip())
    return commands


def test_run_full_audit_includes_release_gate_before_reports() -> None:
    commands = _make_target_commands("run-full-audit")

    assert "qsr-audit gate-gold" in commands
    assert "qsr-audit preflight-release" in commands
    assert commands.index("qsr-audit gate-gold") < commands.index("qsr-audit preflight-release")
    assert commands.index("qsr-audit preflight-release") < commands.index(
        "qsr-audit report --output reports/"
    )


def test_show_targets_aliases_help() -> None:
    commands = _make_target_commands("show-targets")

    assert commands == ["$(MAKE) help"]


def test_smoke_cli_target_runs_help_only_cli_regressions() -> None:
    commands = _make_target_commands("smoke-cli")

    assert commands == ["pytest tests/test_cli_help.py"]
    assert all(not command.startswith("qsr-audit ") for command in commands)


def test_quick_target_runs_cli_smoke_and_hygiene_checks() -> None:
    commands = _make_target_commands("quick")

    assert commands == ["$(MAKE) smoke-cli", "$(MAKE) check-hygiene"]
    assert all(not command.startswith("qsr-audit ") for command in commands)


def test_doctor_target_prints_safe_diagnostics_only() -> None:
    commands = [command.removeprefix("@") for command in _make_target_commands("doctor")]

    assert "python --version" in commands
    assert "qsr-audit --help" in commands
    assert "git branch --show-current" in commands
    assert "git rev-parse --short HEAD" in commands
    assert "git status --short --branch" in commands
    qsr_commands = [command for command in commands if command.startswith("qsr-audit ")]
    assert qsr_commands == ["qsr-audit --help"]


def test_version_target_prints_package_version_and_git_commit() -> None:
    commands = [command.removeprefix("@") for command in _make_target_commands("version")]
    recipe = "\n".join(commands)

    assert "from importlib.metadata import version" in recipe
    assert "version('qsr-audit')" in recipe
    assert "git rev-parse --short HEAD" in commands
    assert all(not command.startswith("qsr-audit ") for command in commands)


def test_ci_status_target_uses_github_cli_with_soft_missing_gh_exit() -> None:
    commands = [command.removeprefix("@") for command in _make_target_commands("ci-status")]
    recipe = "\n".join(commands)

    assert "command -v gh" in recipe
    assert "GitHub CLI (gh) is not installed" in recipe
    assert "else" in recipe
    assert "refs/remotes/origin/HEAD" in recipe
    assert "gh run list" in recipe
    assert '--branch "$$branch"' in recipe
    assert all("qsr-audit " not in command for command in commands)


def test_clean_build_target_removes_only_packaging_outputs() -> None:
    commands = _make_target_commands("clean-build")
    recipe = "\n".join(commands)

    assert "rm -rf dist build" in commands
    assert "-name '*.egg-info'" in recipe
    assert "./.venv" in recipe
    assert "./.git" in recipe
    assert "data/" not in recipe
    assert "reports" not in recipe
    assert "strategy" not in recipe


def test_clean_test_target_removes_only_test_and_coverage_outputs() -> None:
    commands = _make_target_commands("clean-test")
    recipe = "\n".join(commands)

    assert commands == ["rm -rf .pytest_cache .coverage coverage.xml htmlcov"]
    assert "dist" not in recipe
    assert "build" not in recipe
    assert "egg-info" not in recipe
    assert "data/" not in recipe
    assert "reports" not in recipe
    assert "strategy" not in recipe


def test_clean_caches_composes_narrow_cleanup_targets() -> None:
    commands = _make_target_commands("clean-caches")

    assert commands == [
        "$(MAKE) clean-test",
        "$(MAKE) clean-build",
        "find . -type d -name '__pycache__' -prune -exec rm -rf {} +",
        "rm -rf .ruff_cache",
    ]
