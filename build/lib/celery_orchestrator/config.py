from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    redis_url: str = Field(default="redis://localhost:6379/0", description="Broker, results, orchestration storage.")

    orch_key_prefix: str = Field(
        default="orch",
        description="Redis key prefix for orchestration documents (separate from Celery broker keys).",
    )

    settings_manager_base_url: str = "http://localhost:8080"
    crawler_base_url: str = "http://localhost:8083"
    evaluator_base_url: str = "http://localhost:8082"

    http_timeout_seconds: float = 60.0
    collection_query_wait_seconds: float = 3600.0
    evaluation_wait_seconds: float = 3600.0

    api_host: str = "0.0.0.0"
    api_port: int = 8001

    celery_beat_enabled: bool = False

    flower_port: int = 5555


@lru_cache
def get_settings() -> Settings:
    return Settings()
