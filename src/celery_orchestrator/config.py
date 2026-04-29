import os
from functools import lru_cache
from typing import Self

from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # В Docker: REDIS_URL, SETTINGS_*, CRAWLER_*, EVALUATOR_* (см. infra docker-compose)
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        validation_alias=AliasChoices("REDIS_URL", "redis_url"),
    )
    orch_redis_prefix: str = "orch:"

    settings_manager_base_url: str = Field(
        default="http://localhost:8080",
        validation_alias=AliasChoices("SETTINGS_MANAGER_BASE_URL", "settings_manager_base_url"),
    )
    crawler_base_url: str = Field(
        default="http://localhost:3000",
        validation_alias=AliasChoices("CRAWLER_BASE_URL", "crawler_base_url"),
    )
    evaluator_base_url: str = Field(
        default="http://localhost:8082",
        validation_alias=AliasChoices("EVALUATOR_BASE_URL", "evaluator_base_url"),
    )

    http_timeout_seconds: float = 60.0

    #: DEBUG, INFO, WARNING, ... — для `celery_orchestrator` (API, worker, задачи).
    log_level: str = "INFO"

    #: Имя очереди Celery для send_task (должно совпадать с очередями воркера, см. -Q).
    celery_default_queue: str = "celery"
    #: TTL записей Celery result backend в Redis для задач не в терминальном состоянии (сек).
    celery_result_ttl_seconds: int = Field(
        default=432_000,
        ge=1,
        validation_alias=AliasChoices("CELERY_RESULT_TTL_SECONDS", "celery_result_ttl_seconds"),
    )
    #: TTL в Redis для завершённых (SUCCESS/FAILURE/…) записей result backend (сек).
    celery_result_ready_ttl_seconds: int = Field(
        default=86_400,
        ge=1,
        validation_alias=AliasChoices(
            "CELERY_RESULT_READY_TTL_SECONDS",
            "celery_result_ready_ttl_seconds",
        ),
    )
    #: TTL снимков оркестрации (`orch:task:*`, `orch:children:*`) в Redis (сек).
    orch_redis_record_ttl_seconds: int = Field(
        default=432_000,
        ge=1,
        validation_alias=AliasChoices(
            "ORCH_REDIS_RECORD_TTL_SECONDS",
            "orch_redis_record_ttl_seconds",
        ),
    )
    #: Сколько ждать task.finish для collection-query / evaluation (сек).
    orchestration_finish_wait_timeout_seconds: float = 604800.0
    orchestration_finish_poll_interval_seconds: float = 0.25

    @model_validator(mode="after")
    def _prefer_url_env_from_os(self) -> Self:
        """Сырые значения в os.environ (Docker/K8s) важнее .env-файла, если тот смонтирован с localhost."""
        for attr, var in (
            ("redis_url", "REDIS_URL"),
            ("settings_manager_base_url", "SETTINGS_MANAGER_BASE_URL"),
            ("crawler_base_url", "CRAWLER_BASE_URL"),
            ("evaluator_base_url", "EVALUATOR_BASE_URL"),
        ):
            v = (os.environ.get(var) or "").strip()
            if v:
                setattr(self, attr, v)
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
