from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Response
from fastapi.responses import PlainTextResponse

from celery_orchestrator.celery_app import app as celery_app
from celery_orchestrator.config import get_settings
from celery_orchestrator.storage.redis_store import RedisTaskStorage, get_redis_client
from celery_orchestrator.view_builder import orchestration_task_view

router = APIRouter()

_EVENT_NAMES = frozenset({"collection-batch", "collection-query", "evaluation", "notification"})


def _storage() -> RedisTaskStorage:
    s = get_settings()
    return RedisTaskStorage(get_redis_client(s), s.orch_redis_prefix)


def _enqueue(task_name: str, kwargs: dict[str, Any]) -> None:
    task_id = str(uuid.uuid4())
    st = _storage()
    st.init_task(task_id, name=task_name, kwargs=kwargs)
    q = get_settings().celery_default_queue
    celery_app.send_task(task_name, task_id=task_id, kwargs=kwargs, queue=q)


@router.post("/events-queue/progress", status_code=204)
def post_progress(body: dict[str, Any] = Body(...)) -> Response:
    _enqueue("task.progress", dict(body))
    return Response(status_code=204)


@router.post("/events-queue/finish", status_code=204)
def post_finish(body: dict[str, Any] = Body(...)) -> Response:
    kwargs = dict(body)
    kwargs["parentTaskResult"] = body.get("result")
    kwargs["parentTaskStatus"] = body.get("status")
    _enqueue("task.finish", kwargs)
    return Response(status_code=204)


@router.post("/events-queue/{event_name}", status_code=204)
def post_event(event_name: str, body: dict[str, Any] = Body(...)) -> Response:
    if event_name not in _EVENT_NAMES:
        raise HTTPException(status_code=422, detail="unknown event name")
    task_name = f"task.{event_name}"
    _enqueue(task_name, dict(body))
    return Response(status_code=204)


@router.get("/tasks/{task_uuid}", response_model=None)
def get_task(task_uuid: str) -> dict[str, Any] | PlainTextResponse:
    st = _storage()
    doc = st.get_raw(task_uuid)
    if doc is None:
        return PlainTextResponse("not found", status_code=404)
    return orchestration_task_view(doc)


@router.get("/tasks/{task_uuid}/children", response_model=None)
def get_children(task_uuid: str) -> list[dict[str, Any]] | PlainTextResponse:
    st = _storage()
    if not st.exists(task_uuid):
        return PlainTextResponse("not found", status_code=404)
    return [orchestration_task_view(d) for d in st.get_children_views(task_uuid) if d]
