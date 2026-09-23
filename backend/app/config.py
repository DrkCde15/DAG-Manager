from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "sqlite+aiosqlite:///./dagmanager.db"
    sync_interval_minutes: int = 5
    runs_sync_limit: int = 20
    api_title: str = "DAG Manager API"
    api_version: str = "0.1.0"
    api_token: str | None = None
    secret_key: str | None = None

    class Config:
        env_file = ".env"


settings = Settings()
