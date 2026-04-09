#!/usr/bin/env python3
"""Fail when tracked raw/generated artifacts or oversized blobs slip into git."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024
TEXT_HYGIENE_SUFFIXES = {".md", ".json", ".csv", ".toml", ".yaml", ".yml", ".txt"}
WORKSTATION_PATH_SNIPPETS = ("/Users/", "C:\\Users\\", "/home/")
MALFORMED_PLACEHOLDER_SNIPPETS = (
    "reviewers//judgments.csv",
    "artifacts/manifests//latest.json",
)
ALLOWED_TRACKED = {
    "data/raw/.gitkeep",
    "data/bronze/.gitkeep",
    "data/silver/.gitkeep",
    "data/gold/.gitkeep",
    "data/gold/primary_source_gold_candidates.parquet",
    "data/reference/.gitkeep",
    "reports/.gitkeep",
    "reports/reconciliation/primary_source_coverage.md",
    "reports/reconciliation/primary_source_deltas.csv",
    "strategy/.gitkeep",
    "artifacts/rag/benchmarks/2026q2_pack_cycle1_bm25/summary.md",
    "artifacts/rag/benchmarks/2026q2_pack_cycle1_bm25/failure_cases.md",
    "artifacts/rag/benchmarks/2026q2_pack_cycle1_bm25/per_query_results.parquet",
}
ALLOWED_PREFIXES = ("data/reference/",)
FORBIDDEN_PREFIXES = (
    "data/raw/",
    "data/bronze/",
    "data/silver/",
    "data/gold/",
    "reports/",
    "strategy/",
    "artifacts/",
)


def main() -> int:
    tracked_files = _tracked_files()
    violations: list[str] = []

    for relative_path in tracked_files:
        if _is_forbidden_tracked_artifact(relative_path):
            violations.append(f"Tracked local artifact path is not allowed: {relative_path}")

        size = Path(relative_path).stat().st_size
        if size > MAX_FILE_SIZE_BYTES:
            violations.append(
                f"Tracked file exceeds {MAX_FILE_SIZE_BYTES // (1024 * 1024)} MiB: {relative_path}"
            )

        violations.extend(_content_hygiene_violations(relative_path))

    if violations:
        print("Repository hygiene check failed:", file=sys.stderr)
        for violation in violations:
            print(f"- {violation}", file=sys.stderr)
        return 1

    print("Repository hygiene check passed.")
    return 0


def _tracked_files() -> list[str]:
    result = subprocess.run(
        ["git", "ls-files"],
        check=True,
        capture_output=True,
        text=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _is_forbidden_tracked_artifact(relative_path: str) -> bool:
    if relative_path in ALLOWED_TRACKED:
        return False
    if any(relative_path.startswith(prefix) for prefix in ALLOWED_PREFIXES):
        return False
    return any(relative_path.startswith(prefix) for prefix in FORBIDDEN_PREFIXES)


def _content_hygiene_violations(relative_path: str) -> list[str]:
    path = Path(relative_path)
    if path.suffix.lower() not in TEXT_HYGIENE_SUFFIXES:
        return []

    try:
        content = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return []

    violations: list[str] = []
    for snippet in WORKSTATION_PATH_SNIPPETS:
        if _line_contains_non_url_snippet(content, snippet):
            violations.append(
                f"Tracked text contains workstation-specific path '{snippet}': {relative_path}"
            )

    for snippet in MALFORMED_PLACEHOLDER_SNIPPETS:
        if snippet in content:
            violations.append(
                f"Tracked text contains malformed placeholder path '{snippet}': {relative_path}"
            )

    return violations


def _line_contains_non_url_snippet(content: str, snippet: str) -> bool:
    for line in content.splitlines():
        if snippet not in line:
            continue
        if "http://" in line or "https://" in line:
            continue
        return True
    return False


if __name__ == "__main__":
    raise SystemExit(main())
