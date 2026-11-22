"""Typed settings loaded from environment / .env (12-factor)."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    # PROD: load from a secret manager (Vault, AWS Secrets Manager) instead of .env files.
    model_config = SettingsConfigDict(env_file=REPO_ROOT / ".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    schema_dir: Path = REPO_ROOT / "schemas"

    llm_provider: str = "mock"  # openai | mock
    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"
    llm_timeout_seconds: int = 30

    postgres_dsn: str = "postgresql://recruiting:recruiting@localhost:5432/recruiting"

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
