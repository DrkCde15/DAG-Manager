from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# Raiz do projeto (funciona de qualquer CWD: backend/, raiz, etc.)
_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "sqlite+aiosqlite:///./dagmanager.db"
    sync_interval_minutes: int = 5
    runs_sync_limit: int = 20
    api_title: str = "DAG Manager API"
    api_version: str = "0.1.0"
    api_token: str | None = None
    secret_key: str | None = None


settings = Settings()
