from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse
from redis import Redis

from celery_orchestrator.api.queue_broker_router import router as queue_broker_router
from celery_orchestrator.api.tasks_router import router as tasks_router
from celery_orchestrator.config import get_settings
from celery_orchestrator.storage.redis_orchestration import OrchestrationStore

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    client = Redis.from_url(settings.redis_url, decode_responses=True)
    app.state.redis_client = client
    app.state.store = OrchestrationStore(client, settings.orch_key_prefix)
    logger.info("Connected to Redis for API state")
    yield
    client.close()


app = FastAPI(
    title="Celery orchestrator",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(queue_broker_router)
app.include_router(tasks_router)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_request: Request, exc: RequestValidationError) -> PlainTextResponse:
    parts = [f"{e['loc']}: {e['msg']}" for e in exc.errors()]
    return PlainTextResponse("Validation failed: " + "; ".join(parts), status_code=400)


def create_app() -> FastAPI:
    return app
