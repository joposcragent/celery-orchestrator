from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    redis_url: str = "redis://localhost:6379/0"
    orch_redis_prefix: str = "orch:"

    settings_manager_base_url: str = "http://localhost:8080"
    crawler_base_url: str = "http://localhost:3000"
    evaluator_base_url: str = "http://localhost:8082"

    http_timeout_seconds: float = 60.0

    #: Имя очереди Celery для send_task (должно совпадать с очередями воркера, см. -Q).
    celery_default_queue: str = "celery"
    #: Сколько ждать task.finish для collection-query / evaluation (сек).
    orchestration_finish_wait_timeout_seconds: float = 604800.0
    orchestration_finish_poll_interval_seconds: float = 0.25


@lru_cache
def get_settings() -> Settings:
    return Settings()
