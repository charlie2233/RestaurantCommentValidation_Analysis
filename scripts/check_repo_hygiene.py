#!/usr/bin/env python3
"""Fail when tracked raw/generated artifacts or oversized blobs slip into git."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024
ALLOWED_TRACKED = {
    "data/raw/.gitkeep",
    "data/bronze/.gitkeep",
    "data/silver/.gitkeep",
    "data/gold/.gitkeep",
    "data/reference/.gitkeep",
    "reports/.gitkeep",
    "strategy/.gitkeep",
}
ALLOWED_PREFIXES = ("data/reference/templates/",)
FORBIDDEN_PREFIXES = (
    "data/raw/",
    "data/bronze/",
    "data/silver/",
    "data/gold/",
    "data/reference/",
    "reports/",
    "strategy/",
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


if __name__ == "__main__":
    raise SystemExit(main())
