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
    assert commands.index("qsr-audit gate-gold") < commands.index(
        "qsr-audit preflight-release"
    )
    assert commands.index("qsr-audit preflight-release") < commands.index(
        "qsr-audit report --output reports/"
    )
