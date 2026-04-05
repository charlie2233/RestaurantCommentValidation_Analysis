"""Application settings loaded from environment variables / .env file."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="QSR_", env_file=".env", extra="ignore")

    data_raw: Path = Path("data/raw")
    data_bronze: Path = Path("data/bronze")
    data_silver: Path = Path("data/silver")
    data_gold: Path = Path("data/gold")
    data_reference: Path = Path("data/reference")
    gold_history_dir: Path = Path("data/gold/history")

    reports_dir: Path = Path("reports")
    strategy_dir: Path = Path("strategy")
    artifacts_dir: Path = Path("artifacts")

    log_level: str = "INFO"


def get_settings() -> Settings:
    return Settings()


settings = Settings()
