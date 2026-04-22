from __future__ import annotations

from celery.exceptions import OperationalError
from fastapi import APIRouter, Body, status
from fastapi.responses import PlainTextResponse, Response

from celery_orchestrator.api.deps import CeleryDep, StoreDep
from celery_orchestrator.api.schemas import (
    CollectionBatchEvent,
    CollectionQueryCompleteEvent,
    CollectionQueryEvent,
    CollectionQueryProgressEvent,
    EvaluationCompleteEvent,
    EvaluationEnqueueEvent,
    NotificationEvent,
)
from celery_orchestrator.storage.redis_orchestration import TaskNotFoundError

router = APIRouter(tags=["queue-broker"])


def _enqueue(celery_app, name: str, kwargs: dict, *, queue: str = "celery") -> None:
    celery_app.send_task(name, kwargs=kwargs, queue=queue)


@router.post("/queue-broker/events/collection-batch", response_model=None)
def post_collection_batch(
    celery_app: CeleryDep,
    body: CollectionBatchEvent | None = Body(None),
) -> Response | PlainTextResponse:
    kwargs: dict = {}
    if body:
        if body.triggered_at is not None:
            kwargs["triggered_at"] = body.triggered_at
        if body.correlation_id is not None:
            kwargs["correlation_id"] = str(body.correlation_id)
    try:
        _enqueue(celery_app, "tasks.collection_batch", kwargs)
    except OperationalError as e:
        return PlainTextResponse(str(e), status_code=500)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/queue-broker/events/collection-query", response_model=None)
def post_collection_query(
    store: StoreDep,
    celery_app: CeleryDep,
    body: CollectionQueryEvent = Body(...),
) -> Response | PlainTextResponse:
    pid = str(body.parent_batch_task_uuid)
    try:
        store.require_type(pid, "COLLECTION_BATCH")
    except TaskNotFoundError:
        return PlainTextResponse(f"Parent task not found: {pid}", status_code=404)
    kwargs = {
        "search_query": body.search_query,
        "parent_batch_task_uuid": pid,
    }
    try:
        _enqueue(celery_app, "tasks.collection_query", kwargs)
    except OperationalError as e:
        return PlainTextResponse(str(e), status_code=500)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/queue-broker/events/collection-query-progress", response_model=None)
def post_collection_query_progress(
    store: StoreDep,
    celery_app: CeleryDep,
    body: CollectionQueryProgressEvent = Body(...),
) -> Response | PlainTextResponse:
    tid = str(body.collection_query_task_uuid)
    try:
        store.require_type(tid, "COLLECTION_QUERY")
    except TaskNotFoundError:
        return PlainTextResponse(f"Orchestration task not found: {tid}", status_code=404)
    kwargs = {
        "collection_query_task_uuid": tid,
        "current_page": body.current_page,
        "total_pages": body.total_pages,
        "execution_log_fragment": body.execution_log_fragment,
        "detail": body.detail,
    }
    try:
        _enqueue(celery_app, "tasks.collection_query_progress", kwargs)
    except OperationalError as e:
        return PlainTextResponse(str(e), status_code=500)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/queue-broker/events/collection-query-complete", response_model=None)
def post_collection_query_complete(
    store: StoreDep,
    celery_app: CeleryDep,
    body: CollectionQueryCompleteEvent = Body(...),
) -> Response | PlainTextResponse:
    tid = str(body.collection_query_task_uuid)
    try:
        store.require_type(tid, "COLLECTION_QUERY")
    except TaskNotFoundError:
        return PlainTextResponse(f"Orchestration task not found: {tid}", status_code=404)
    kwargs = {
        "collection_query_task_uuid": tid,
        "execution_log": body.execution_log,
        "result": body.result,
    }
    try:
        _enqueue(celery_app, "tasks.collection_query_complete", kwargs)
    except OperationalError as e:
        return PlainTextResponse(str(e), status_code=500)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/queue-broker/events/evaluation", response_model=None)
def post_evaluation(
    store: StoreDep,
    celery_app: CeleryDep,
    body: EvaluationEnqueueEvent = Body(...),
) -> Response | PlainTextResponse:
    if body.parent_task_uuid is not None:
        pid = str(body.parent_task_uuid)
        if store.get_raw(pid) is None:
            return PlainTextResponse(f"Parent task not found: {pid}", status_code=404)
    kwargs: dict = {"job_posting_uuid": str(body.job_posting_uuid)}
    if body.evaluation_task_uuid is not None:
        kwargs["evaluation_task_uuid"] = str(body.evaluation_task_uuid)
    if body.parent_task_uuid is not None:
        kwargs["parent_task_uuid"] = str(body.parent_task_uuid)
    try:
        _enqueue(celery_app, "tasks.evaluation", kwargs)
    except OperationalError as e:
        return PlainTextResponse(str(e), status_code=500)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/queue-broker/events/evaluation-complete", response_model=None)
def post_evaluation_complete(
    store: StoreDep,
    celery_app: CeleryDep,
    body: EvaluationCompleteEvent = Body(...),
) -> Response | PlainTextResponse:
    tid = str(body.evaluation_task_uuid)
    try:
        store.require_type(tid, "EVALUATION")
    except TaskNotFoundError:
        return PlainTextResponse(f"Orchestration task not found: {tid}", status_code=404)
    kwargs = {
        "evaluation_task_uuid": tid,
        "execution_log": body.execution_log,
        "result": body.result,
    }
    try:
        _enqueue(celery_app, "tasks.evaluation_complete", kwargs)
    except OperationalError as e:
        return PlainTextResponse(str(e), status_code=500)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/queue-broker/events/notification", response_model=None)
def post_notification(
    celery_app: CeleryDep,
    body: NotificationEvent = Body(...),
) -> Response | PlainTextResponse:
    kwargs: dict = {"job_posting_uuid": str(body.job_posting_uuid)}
    if body.relevance_score is not None:
        kwargs["relevance_score"] = body.relevance_score
    if body.notification_threshold is not None:
        kwargs["notification_threshold"] = body.notification_threshold
    if body.message is not None:
        kwargs["message"] = body.message
    if body.evaluation_task_uuid is not None:
        kwargs["evaluation_task_uuid"] = str(body.evaluation_task_uuid)
    try:
        _enqueue(celery_app, "tasks.notification", kwargs)
    except OperationalError as e:
        return PlainTextResponse(str(e), status_code=500)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
