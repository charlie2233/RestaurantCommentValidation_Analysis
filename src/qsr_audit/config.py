"""Application settings loaded from environment variables / .env file."""

import os
from pathlib import Path

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PATH_FIELDS = (
    "data_raw",
    "data_bronze",
    "data_silver",
    "data_gold",
    "data_reference",
    "gold_history_dir",
    "reports_dir",
    "strategy_dir",
    "artifacts_dir",
)
SAFE_DEBUG_SECRET_ENV_NAMES = (
    "OPENAI_API_KEY",
    "QSR_OPENAI_API_KEY",
    "GITHUB_TOKEN",
    "QSR_GITHUB_TOKEN",
    "HF_TOKEN",
    "HUGGINGFACE_HUB_TOKEN",
)


def _validate_no_overlaps(roots: dict[str, Path]) -> None:
    items = list(roots.items())
    for index, (left_name, left_path) in enumerate(items):
        for right_name, right_path in items[index + 1 :]:
            if _is_relative_to(left_path, right_path) or _is_relative_to(right_path, left_path):
                raise ValueError(
                    f"Configured roots `{left_name}` and `{right_name}` must not overlap: "
                    f"{left_path} vs {right_path}."
                )


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _redacted_env_value(value: str | None) -> str | None:
    if value is None:
        return None
    return "***REDACTED***"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="QSR_", env_file=".env", extra="ignore")

    data_raw: Path = Path("data/raw")
    data_bronze: Path = Path("data/bronze")
    data_silver: Path = Path("data/silver")
    data_gold: Path = Path("data/gold")
    data_reference: Path = Path("data/reference")
    gold_history_dir: Path | None = None

    reports_dir: Path = Path("reports")
    strategy_dir: Path = Path("strategy")
    artifacts_dir: Path = Path("artifacts")

    log_level: str = "INFO"

    @field_validator(*PATH_FIELDS, mode="before")
    @classmethod
    def _resolve_path(cls, value: Path | str | None) -> Path | None:
        if value is None:
            return None
        return Path(value).expanduser().resolve()

    @model_validator(mode="after")
    def _validate_roots(self) -> "Settings":
        if self.gold_history_dir is None:
            self.gold_history_dir = (self.data_gold / "history").resolve()
        if not _is_relative_to(self.gold_history_dir, self.data_gold):
            raise ValueError("QSR_GOLD_HISTORY_DIR must live under QSR_DATA_GOLD.")

        top_level_roots = {
            "data_raw": self.data_raw,
            "data_bronze": self.data_bronze,
            "data_silver": self.data_silver,
            "data_gold": self.data_gold,
            "data_reference": self.data_reference,
            "reports_dir": self.reports_dir,
            "strategy_dir": self.strategy_dir,
            "artifacts_dir": self.artifacts_dir,
        }
        _validate_no_overlaps(top_level_roots)
        return self

    def validate_artifact_root(self, path: Path, *, purpose: str) -> Path:
        """Reject artifact destinations that bleed into analyst-facing directories."""

        resolved = Path(path).expanduser().resolve()
        for forbidden_root in (self.reports_dir, self.strategy_dir):
            if _is_relative_to(resolved, forbidden_root):
                raise ValueError(
                    f"{purpose} must not be written under analyst-facing paths like {forbidden_root}."
                )
        return resolved

    def safe_debug_summary(self) -> dict[str, object]:
        """Return a CI-safe settings summary with secret-like env values redacted."""

        return {
            "paths": {field: str(getattr(self, field)) for field in PATH_FIELDS},
            "log_level": self.log_level,
            "secret_environment": {
                name: _redacted_env_value(os.getenv(name))
                for name in SAFE_DEBUG_SECRET_ENV_NAMES
                if os.getenv(name) is not None
            },
        }


def get_settings() -> Settings:
    return Settings()


settings = Settings()
