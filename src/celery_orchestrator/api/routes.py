from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import PlainTextResponse

from celery_orchestrator.celery_app import app as celery_app
from celery_orchestrator.config import get_settings
from celery_orchestrator.storage.redis_store import RedisTaskStorage, get_redis_client
from celery_orchestrator.view_builder import orchestration_task_view

_log = logging.getLogger("celery_orchestrator.api.routes")

router = APIRouter()

_EVENT_NAMES = frozenset({"collection-batch", "collection-query", "evaluation", "notification"})
_RESERVED_QUEUE_KEYS = frozenset({"executionLog", "result", "status"})


async def _read_json_object(request: Request) -> dict[str, Any]:
    """Parse JSON body as an object. Tolerates any extra keys (not validated by Pydantic)."""
    raw = await request.body()
    if not raw:
        raise HTTPException(status_code=422, detail="request body is required")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=422, detail=f"invalid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise HTTPException(status_code=422, detail="JSON body must be an object")
    return data


def _storage() -> RedisTaskStorage:
    s = get_settings()
    return RedisTaskStorage(get_redis_client(s), s.orch_redis_prefix)


def _kwargs_for_celery(body: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in body.items() if k not in _RESERVED_QUEUE_KEYS}


def _body_correlation_id(body: dict[str, Any]) -> str | None:
    c = body.get("correlationId")
    if c is None and "correlation_id" in body:
        c = body.get("correlation_id")
    return str(c) if c is not None else None


def _snapshot_init_kwargs(body: dict[str, Any]) -> dict[str, Any]:
    """Map HTTP body fields into init_task snapshot kwargs (not sent to Celery)."""
    out: dict[str, Any] = {}
    if "result" in body and body["result"] is not None:
        out["snapshot_result"] = body["result"]
    if "executionLog" in body and body["executionLog"] is not None:
        out["snapshot_execution_log"] = body["executionLog"]
    if "status" in body and body["status"] is not None:
        out["finish_event_status"] = str(body["status"])
    return out


def _enqueue(task_name: str, body: dict[str, Any]) -> None:
    task_id = str(uuid.uuid4())
    s = get_settings()
    st = _storage()
    kwargs_celery = _kwargs_for_celery(body)
    q = s.celery_default_queue
    corr = _body_correlation_id(body)
    try:
        st.init_task(task_id, name=task_name, kwargs=kwargs_celery, **_snapshot_init_kwargs(body))
        celery_app.send_task(task_name, task_id=task_id, kwargs=kwargs_celery, queue=q)
    except Exception:
        _log.exception(
            "enqueue failed task_id=%s task_name=%s queue=%s body_correlationId=%s",
            task_id,
            task_name,
            q,
            corr or "none",
        )
        raise
    _log.info(
        "enqueued task_id=%s task_name=%s queue=%s body_correlationId=%s; use task_id for GET /tasks/... (not the body correlationId as task uuid)",
        task_id,
        task_name,
        q,
        corr or "none",
    )


@router.post("/events-queue/progress", status_code=204)
async def post_progress(request: Request) -> Response:
    body = await _read_json_object(request)
    _enqueue("task.progress", dict(body))
    return Response(status_code=204)


@router.post("/events-queue/finish", status_code=204)
async def post_finish(request: Request) -> Response:
    body = await _read_json_object(request)
    _enqueue("task.finish", dict(body))
    return Response(status_code=204)


@router.post("/events-queue/{event_name}", status_code=204)
async def post_event(event_name: str, request: Request) -> Response:
    if event_name not in _EVENT_NAMES:
        raise HTTPException(status_code=422, detail="unknown event name")
    body = await _read_json_object(request)
    task_name = f"task.{event_name}"
    _enqueue(task_name, dict(body))
    return Response(status_code=204)


@router.get("/tasks/{task_uuid}", response_model=None)
def get_task(task_uuid: str) -> dict[str, Any] | PlainTextResponse:
    st = _storage()
    doc = st.get_raw(task_uuid)
    if doc is None:
        _log.info("get_task not_found task_uuid=%s", task_uuid)
        return PlainTextResponse("not found", status_code=404)
    _log.info("get_task ok task_uuid=%s", task_uuid)
    return orchestration_task_view(doc)


@router.get("/tasks/{task_uuid}/children", response_model=None)
def get_children(task_uuid: str) -> list[dict[str, Any]] | PlainTextResponse:
    st = _storage()
    if not st.exists(task_uuid):
        _log.info("get_children not_found task_uuid=%s", task_uuid)
        return PlainTextResponse("not found", status_code=404)
    views = st.get_children_views(task_uuid)
    _log.info("get_children ok task_uuid=%s child_views=%d", task_uuid, len(views))
    return [orchestration_task_view(d) for d in views if d]
