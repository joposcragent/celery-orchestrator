from __future__ import annotations

from typing import Annotated, Any

from fastapi import Depends, Request
from redis import Redis

from celery_orchestrator.celery_app import celery_app
from celery_orchestrator.storage.redis_orchestration import OrchestrationStore


def get_redis(request: Request) -> Redis:
    return request.app.state.redis_client


def get_store(request: Request) -> OrchestrationStore:
    return request.app.state.store


def get_celery_app() -> Any:
    return celery_app


RedisDep = Annotated[Redis, Depends(get_redis)]
StoreDep = Annotated[OrchestrationStore, Depends(get_store)]
CeleryDep = Annotated[Any, Depends(get_celery_app)]
