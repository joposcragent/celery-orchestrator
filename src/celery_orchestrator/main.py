from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.exception_handlers import (
    http_exception_handler,
    request_validation_exception_handler,
)
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import PlainTextResponse

from celery_orchestrator import __version__
from celery_orchestrator.api.routes import router
from celery_orchestrator.logging_setup import configure_logging

_log = logging.getLogger("celery_orchestrator.api")


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    configure_logging()
    yield


app = FastAPI(
    title="celery-orchestrator",
    version=__version__,
    description="REST facade for Celery broker and orchestration task views",
    lifespan=_lifespan,
)
app.include_router(router)


@app.exception_handler(RequestValidationError)
async def validation_handler(request, exc: RequestValidationError):
    return await request_validation_exception_handler(request, exc)


@app.exception_handler(StarletteHTTPException)
async def starlette_http_handler(request, exc: StarletteHTTPException):
    return await http_exception_handler(request, exc)


@app.exception_handler(Exception)
async def uncaught_exception_handler(request, exc: Exception):
    _log.exception(
        "unhandled error method=%s path=%s",
        request.method,
        request.url.path,
    )
    return PlainTextResponse(str(exc), status_code=500)
