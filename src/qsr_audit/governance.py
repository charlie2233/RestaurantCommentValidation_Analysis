"""Artifact manifests and machine-readable audit logs for release-sensitive commands."""

from __future__ import annotations

import hashlib
import json
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from itertools import count
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from qsr_audit.config import Settings

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_VERSION = "1.0.0"
AUDIT_LOG_VERSION = "1.0.0"
_RUN_ID_SEQUENCE = count()


class DataClassification(StrEnum):
    """Versioned data classification labels for project artifacts."""

    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


class ArtifactManifest(BaseModel):
    """Machine-readable lineage manifest for a CLI command run."""

    manifest_version: str = Field(default=MANIFEST_VERSION)
    command_name: str
    run_timestamp: str
    git_sha: str | None
    input_paths: list[str]
    output_paths: list[str]
    row_counts: dict[str, int]
    file_hashes: dict[str, str]
    data_classification: DataClassification
    intended_audience: str
    publish_status_scope: str
    upstream_artifact_references: list[str]
    warnings_count: int
    errors_count: int


class AuditLogEntry(BaseModel):
    """Machine-readable audit log entry for a CLI command run."""

    log_version: str = Field(default=AUDIT_LOG_VERSION)
    command_name: str
    start_timestamp: str
    end_timestamp: str
    status: str
    input_paths: list[str]
    output_paths: list[str]
    warnings_count: int
    errors_count: int
    manifest_path: str | None
    failure_type: str | None = None


@dataclass(frozen=True)
class CommandAuditSession:
    """In-memory audit session state for one CLI command run."""

    command_name: str
    start_timestamp: str
    run_id: str


def begin_command_audit(command_name: str) -> CommandAuditSession:
    """Start an audit session for one CLI command."""

    timestamp = utc_now_iso()
    return CommandAuditSession(
        command_name=command_name,
        start_timestamp=timestamp,
        run_id=_unique_run_id(timestamp),
    )


def latest_manifest_path(settings: Settings, command_name: str) -> Path:
    """Return the stable latest manifest path for a command."""

    return _manifest_directory(settings, command_name) / "latest.json"


def write_artifact_manifest(
    *,
    settings: Settings,
    command_name: str,
    input_paths: Sequence[Path | str],
    output_paths: Sequence[Path | str],
    row_counts: dict[str, int] | None,
    data_classification: DataClassification,
    intended_audience: str,
    publish_status_scope: str,
    upstream_artifact_references: Sequence[Path | str] = (),
    warnings_count: int = 0,
    errors_count: int = 0,
    run_timestamp: str | None = None,
    run_id: str | None = None,
) -> Path:
    """Write a versioned artifact manifest plus a stable latest pointer."""

    resolved_inputs = _normalize_paths(input_paths)
    resolved_outputs = _normalize_paths(output_paths)
    manifest_timestamp = run_timestamp or utc_now_iso()
    manifest_run_id = run_id or _unique_run_id(manifest_timestamp)

    payload = ArtifactManifest(
        command_name=command_name,
        run_timestamp=manifest_timestamp,
        git_sha=_git_sha(),
        input_paths=[format_artifact_path(path) for path in resolved_inputs],
        output_paths=[format_artifact_path(path) for path in resolved_outputs],
        row_counts={key: int(value) for key, value in (row_counts or {}).items()},
        file_hashes={
            format_artifact_path(path): _sha256_file(path)
            for path in resolved_outputs
            if path.exists() and path.is_file()
        },
        data_classification=data_classification,
        intended_audience=intended_audience,
        publish_status_scope=publish_status_scope,
        upstream_artifact_references=[
            format_artifact_path(Path(path).expanduser().resolve())
            for path in upstream_artifact_references
        ],
        warnings_count=int(warnings_count),
        errors_count=int(errors_count),
    )

    manifest_dir = _manifest_directory(settings, command_name)
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / f"{manifest_run_id}.json"
    latest_path = manifest_dir / "latest.json"
    serialized = json.dumps(payload.model_dump(mode="json"), ensure_ascii=False, indent=2)
    manifest_path.write_text(serialized, encoding="utf-8")
    latest_path.write_text(serialized, encoding="utf-8")
    return manifest_path


def load_artifact_manifest(path: Path) -> ArtifactManifest:
    """Load and validate an artifact manifest from disk."""

    payload = json.loads(path.read_text(encoding="utf-8"))
    return ArtifactManifest.model_validate(payload)


def write_command_audit_log(
    *,
    settings: Settings,
    session: CommandAuditSession,
    status: str,
    input_paths: Sequence[Path | str],
    output_paths: Sequence[Path | str] = (),
    warnings_count: int = 0,
    errors_count: int = 0,
    manifest_path: Path | None = None,
    failure_type: str | None = None,
) -> Path:
    """Write a machine-readable audit log entry for one CLI command run."""

    resolved_inputs = _normalize_paths(input_paths)
    resolved_outputs = _normalize_paths(output_paths)
    payload = AuditLogEntry(
        command_name=session.command_name,
        start_timestamp=session.start_timestamp,
        end_timestamp=utc_now_iso(),
        status=status,
        input_paths=[format_artifact_path(path) for path in resolved_inputs],
        output_paths=[format_artifact_path(path) for path in resolved_outputs],
        warnings_count=int(warnings_count),
        errors_count=int(errors_count),
        manifest_path=format_artifact_path(manifest_path) if manifest_path is not None else None,
        failure_type=failure_type,
    )

    log_dir = _audit_log_directory(settings, session.command_name)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{session.run_id}.json"
    log_path.write_text(
        json.dumps(payload.model_dump(mode="json"), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return log_path


def utc_now_iso() -> str:
    """Return a UTC timestamp suitable for JSON logs and manifests."""

    return datetime.now(UTC).isoformat(timespec="microseconds")


def _manifest_directory(settings: Settings, command_name: str) -> Path:
    root = settings.validate_artifact_root(
        settings.artifacts_dir / "manifests",
        purpose="artifact manifests",
    )
    return root / _slugify(command_name)


def _audit_log_directory(settings: Settings, command_name: str) -> Path:
    root = settings.validate_artifact_root(
        settings.artifacts_dir / "audit_logs",
        purpose="CLI audit logs",
    )
    return root / _slugify(command_name)


def _normalize_paths(paths: Sequence[Path | str]) -> list[Path]:
    normalized: list[Path] = []
    for raw_path in paths:
        path = Path(raw_path).expanduser().resolve()
        if path not in normalized:
            normalized.append(path)
    return normalized


def format_artifact_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path.resolve())


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_sha() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    value = result.stdout.strip()
    return value or None


def _slugify(value: str) -> str:
    slug = [character.lower() if character.isalnum() else "-" for character in value.strip()]
    rendered = "".join(slug).strip("-")
    while "--" in rendered:
        rendered = rendered.replace("--", "-")
    return rendered or "command"


def _timestamp_slug(timestamp: str) -> str:
    return timestamp.replace(":", "").replace("-", "").replace("+00:00", "Z").replace("T", "T")


def _unique_run_id(timestamp: str) -> str:
    return f"{_timestamp_slug(timestamp)}-{next(_RUN_ID_SEQUENCE):04d}"


__all__ = [
    "AUDIT_LOG_VERSION",
    "ArtifactManifest",
    "AuditLogEntry",
    "CommandAuditSession",
    "DataClassification",
    "MANIFEST_VERSION",
    "begin_command_audit",
    "format_artifact_path",
    "latest_manifest_path",
    "load_artifact_manifest",
    "utc_now_iso",
    "write_artifact_manifest",
    "write_command_audit_log",
]
