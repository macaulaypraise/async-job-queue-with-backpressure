from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # App
    app_env: str = "development"
    app_host: str = "0.0.0.0"
    app_port: int = 8000

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # PostgreSQL
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/jobqueue"

    # Backpressure watermarks
    high_watermark: int = 10000
    low_watermark: int = 2000

    # Worker settings
    max_retries: int = 5
    worker_count: int = 4

    # Priority weights (must sum to 100)
    weight_critical: int = 60
    weight_high: int = 30
    weight_normal: int = 10


@lru_cache
def get_settings() -> Settings:
    return Settings()
