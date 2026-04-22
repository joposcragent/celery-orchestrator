from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Body, status
from fastapi.responses import PlainTextResponse, Response

from celery_orchestrator.api.deps import StoreDep
from celery_orchestrator.api.schemas import (
    OrchestrationTaskCompletion,
    OrchestrationTaskCreate,
    OrchestrationTaskProgress,
)
from celery_orchestrator.storage.redis_orchestration import ParentNotFoundError, TaskConflictError

router = APIRouter(tags=["tasks"])

_ALLOWED_TYPES = frozenset({"COLLECTION_BATCH", "COLLECTION_QUERY", "EVALUATION"})
_TERMINAL = frozenset({"SUCCEEDED", "FAILED", "CANCELLED"})


@router.post("/tasks", status_code=status.HTTP_201_CREATED, response_model=None)
def post_task(
    store: StoreDep,
    body: OrchestrationTaskCreate = Body(...),
) -> dict | PlainTextResponse:
    if body.task_type not in _ALLOWED_TYPES:
        return PlainTextResponse(f"Invalid task type: {body.task_type}", status_code=400)
    tid = str(body.task_uuid) if body.task_uuid else None
    pid = str(body.parent_task_uuid) if body.parent_task_uuid else None
    try:
        doc = store.create_task(
            task_uuid=tid,
            task_type=body.task_type,
            parent_task_uuid=pid,
            initial_context=body.initial_context,
            status="PENDING",
        )
    except ParentNotFoundError:
        return PlainTextResponse(f"Parent task not found: {pid}", status_code=404)
    except TaskConflictError as e:
        return PlainTextResponse(str(e), status_code=409)
    return {"taskUuid": doc["taskUuid"], "type": doc["type"], "status": doc["status"]}


@router.get("/tasks/{taskUuid}", response_model=None)
def get_task(
    store: StoreDep,
    taskUuid: UUID,
) -> dict | PlainTextResponse:
    doc = store.get_raw(str(taskUuid))
    if doc is None:
        return PlainTextResponse(f"Task not found: {taskUuid}", status_code=404)
    return store.to_view(doc)


@router.post("/tasks/{taskUuid}/progress", response_model=None)
def post_task_progress(
    store: StoreDep,
    taskUuid: UUID,
    body: OrchestrationTaskProgress = Body(...),
) -> Response | PlainTextResponse:
    tid = str(taskUuid)
    if store.get_raw(tid) is None:
        return PlainTextResponse(f"Task not found: {tid}", status_code=404)
    try:
        store.record_progress(
            tid,
            recorded_at=body.recorded_at,
            execution_log_fragment=body.execution_log_fragment,
            detail=body.detail,
        )
    except TaskConflictError:
        return PlainTextResponse("Task is already in a terminal status", status_code=409)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/tasks/{taskUuid}/completion", response_model=None)
def post_task_completion(
    store: StoreDep,
    taskUuid: UUID,
    body: OrchestrationTaskCompletion = Body(...),
) -> Response | PlainTextResponse:
    tid = str(taskUuid)
    if store.get_raw(tid) is None:
        return PlainTextResponse(f"Task not found: {tid}", status_code=404)
    if body.terminal_status not in _TERMINAL:
        return PlainTextResponse(f"Invalid terminalStatus: {body.terminal_status}", status_code=400)
    try:
        store.record_completion(
            tid,
            terminal_status=body.terminal_status,
            execution_log=body.execution_log,
            result=body.result,
            error_message=body.error_message,
        )
    except TaskConflictError:
        return PlainTextResponse("Task was already completed", status_code=409)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
