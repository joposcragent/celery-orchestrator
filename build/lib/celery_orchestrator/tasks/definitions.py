from __future__ import annotations

import contextlib
import logging
import uuid
from typing import Any

import httpx
import redis

from celery_orchestrator.celery_app import celery_app
from celery_orchestrator.config import get_settings
from celery_orchestrator.services import http_integration
from celery_orchestrator.services.wait_signals import (
    signal_collection_query_complete,
    signal_evaluation_complete,
    wait_collection_query_complete,
    wait_evaluation_complete,
)
from celery_orchestrator.storage.redis_orchestration import (
    OrchestrationStore,
    TaskConflictError,
    TaskNotFoundError,
)

logger = logging.getLogger(__name__)


def _redis_client() -> redis.Redis:
    return redis.from_url(get_settings().redis_url, decode_responses=True)


def _store() -> OrchestrationStore:
    s = get_settings()
    return OrchestrationStore(_redis_client(), s.orch_key_prefix)


@celery_app.task(name="tasks.collection_batch")
def collection_batch(
    triggered_at: str | None = None,
    correlation_id: str | None = None,
    **_extra: Any,
) -> None:
    settings = get_settings()
    store = _store()
    batch_uuid = str(uuid.uuid4())
    ctx: dict[str, Any] = {}
    if triggered_at:
        ctx["triggeredAt"] = triggered_at
    if correlation_id:
        ctx["correlationId"] = correlation_id
    store.create_task(
        task_uuid=batch_uuid,
        task_type="COLLECTION_BATCH",
        parent_task_uuid=None,
        initial_context=ctx,
        status="RUNNING",
    )
    try:
        queries = http_integration.fetch_search_queries(settings)
    except Exception as e:
        logger.exception("settings-manager unavailable")
        store.record_completion(
            batch_uuid,
            terminal_status="FAILED",
            execution_log=str(e),
            result=None,
            error_message=str(e),
        )
        raise
    if not queries:
        store.record_completion(
            batch_uuid,
            terminal_status="FAILED",
            execution_log="No search queries configured",
            result=None,
            error_message="Empty search query list",
        )
        return
    store.merge_initial_context(batch_uuid, {"spawnedQueryCount": len(queries)})
    for q in queries:
        cq_uuid = str(uuid.uuid4())
        store.create_task(
            task_uuid=cq_uuid,
            task_type="COLLECTION_QUERY",
            parent_task_uuid=batch_uuid,
            initial_context={"searchQuery": q},
            status="RUNNING",
        )
        celery_app.send_task(
            "tasks.collection_query",
            kwargs={
                "search_query": q,
                "parent_batch_task_uuid": batch_uuid,
                "collection_query_task_uuid": cq_uuid,
            },
        )


@celery_app.task(name="tasks.collection_query")
def collection_query(
    search_query: str,
    parent_batch_task_uuid: str,
    collection_query_task_uuid: str | None = None,
    **_extra: Any,
) -> None:
    settings = get_settings()
    store = _store()
    r = _redis_client()
    try:
        store.require_type(parent_batch_task_uuid, "COLLECTION_BATCH")
    except TaskNotFoundError:
        if collection_query_task_uuid and store.get_raw(collection_query_task_uuid):
            with contextlib.suppress(TaskNotFoundError, TaskConflictError):
                store.record_completion(
                    collection_query_task_uuid,
                    terminal_status="FAILED",
                    execution_log="Parent COLLECTION_BATCH not found",
                    result=None,
                    error_message="Parent task not found",
                )
        raise
    cq_uuid = collection_query_task_uuid or str(uuid.uuid4())
    if store.get_raw(cq_uuid) is None:
        store.create_task(
            task_uuid=cq_uuid,
            task_type="COLLECTION_QUERY",
            parent_task_uuid=parent_batch_task_uuid,
            initial_context={"searchQuery": search_query},
            status="RUNNING",
        )
    try:
        http_integration.start_crawler(settings, search_query=search_query, correlation_id=cq_uuid)
    except httpx.HTTPStatusError as e:
        logger.warning("crawler HTTP error: %s", e)
        store.record_completion(
            cq_uuid,
            terminal_status="FAILED",
            execution_log=str(e),
            result={"statusCode": e.response.status_code},
            error_message=str(e),
        )
        raise
    except Exception as e:
        logger.exception("crawler call failed")
        store.record_completion(
            cq_uuid,
            terminal_status="FAILED",
            execution_log=str(e),
            result=None,
            error_message=str(e),
        )
        raise
    sig = wait_collection_query_complete(
        r,
        settings.orch_key_prefix,
        cq_uuid,
        settings.collection_query_wait_seconds,
    )
    if sig is None:
        store.record_completion(
            cq_uuid,
            terminal_status="FAILED",
            execution_log="Timeout waiting for collection-query-complete",
            result=None,
            error_message="Timeout",
        )
        raise TimeoutError("collection_query wait timeout")


@celery_app.task(name="tasks.collection_query_progress")
def collection_query_progress(
    collection_query_task_uuid: str,
    current_page: int | None = None,
    total_pages: int | None = None,
    execution_log_fragment: str | None = None,
    detail: dict[str, Any] | None = None,
    **_: Any,
) -> None:
    store = _store()
    try:
        doc = store.require_type(collection_query_task_uuid, "COLLECTION_QUERY")
    except TaskNotFoundError:
        logger.error("COLLECTION_QUERY not found: %s", collection_query_task_uuid)
        return
    if doc.get("status") in {"SUCCEEDED", "FAILED", "CANCELLED"}:
        logger.warning("Ignoring progress for terminal task %s", collection_query_task_uuid)
        return
    d: dict[str, Any] = {}
    if current_page is not None:
        d["currentPage"] = current_page
    if total_pages is not None:
        d["totalPages"] = total_pages
    try:
        store.record_progress(
            collection_query_task_uuid,
            recorded_at=None,
            execution_log_fragment=execution_log_fragment,
            detail={**(detail or {}), **{k: v for k, v in d.items() if v is not None}},
        )
    except TaskConflictError:
        logger.warning("Progress rejected (terminal): %s", collection_query_task_uuid)


@celery_app.task(name="tasks.collection_query_complete")
def collection_query_complete(
    collection_query_task_uuid: str,
    execution_log: Any,
    result: dict[str, Any],
    **_: Any,
) -> None:
    settings = get_settings()
    store = _store()
    r = _redis_client()
    try:
        store.require_type(collection_query_task_uuid, "COLLECTION_QUERY")
    except TaskNotFoundError:
        logger.error("COLLECTION_QUERY not found for complete: %s", collection_query_task_uuid)
        return
    doc = store.get_raw(collection_query_task_uuid)
    if doc and doc.get("status") in {"SUCCEEDED", "FAILED", "CANCELLED"}:
        logger.info("Idempotent complete for %s", collection_query_task_uuid)
        signal_collection_query_complete(r, settings.orch_key_prefix, collection_query_task_uuid, {"duplicate": True})
        return
    failed = bool(result.get("failed")) if isinstance(result, dict) else False
    try:
        store.record_completion(
            collection_query_task_uuid,
            terminal_status="FAILED" if failed else "SUCCEEDED",
            execution_log=execution_log,
            result=result,
            error_message=str(result.get("error")) if failed and isinstance(result, dict) else None,
        )
    except TaskConflictError:
        logger.info("Completion idempotent skip for %s", collection_query_task_uuid)
    signal_collection_query_complete(r, settings.orch_key_prefix, collection_query_task_uuid, {})


@celery_app.task(name="tasks.evaluation")
def evaluation(
    job_posting_uuid: str,
    evaluation_task_uuid: str | None = None,
    parent_task_uuid: str | None = None,
    **_: Any,
) -> None:
    settings = get_settings()
    store = _store()
    r = _redis_client()
    ev_uuid = evaluation_task_uuid or str(uuid.uuid4())
    if store.get_raw(ev_uuid) is None:
        store.create_task(
            task_uuid=ev_uuid,
            task_type="EVALUATION",
            parent_task_uuid=parent_task_uuid,
            initial_context={"jobPostingUuid": job_posting_uuid},
            status="RUNNING",
        )
    try:
        http_integration.start_evaluator_async(
            settings,
            job_posting_uuid=job_posting_uuid,
            evaluation_task_uuid=ev_uuid,
            parent_task_uuid=parent_task_uuid,
        )
    except httpx.HTTPStatusError as e:
        store.record_completion(
            ev_uuid,
            terminal_status="FAILED",
            execution_log=str(e),
            result={"statusCode": e.response.status_code},
            error_message=str(e),
        )
        raise
    except Exception as e:
        logger.exception("evaluator call failed")
        store.record_completion(
            ev_uuid,
            terminal_status="FAILED",
            execution_log=str(e),
            result=None,
            error_message=str(e),
        )
        raise
    sig = wait_evaluation_complete(
        r,
        settings.orch_key_prefix,
        ev_uuid,
        settings.evaluation_wait_seconds,
    )
    if sig is None:
        store.record_completion(
            ev_uuid,
            terminal_status="FAILED",
            execution_log="Timeout waiting for evaluation-complete",
            result=None,
            error_message="Timeout",
        )
        raise TimeoutError("evaluation wait timeout")


@celery_app.task(name="tasks.evaluation_complete")
def evaluation_complete(
    evaluation_task_uuid: str,
    execution_log: Any,
    result: dict[str, Any],
    **_: Any,
) -> None:
    settings = get_settings()
    store = _store()
    r = _redis_client()
    try:
        store.require_type(evaluation_task_uuid, "EVALUATION")
    except TaskNotFoundError:
        logger.error("EVALUATION not found: %s", evaluation_task_uuid)
        return
    doc = store.get_raw(evaluation_task_uuid)
    failed = bool(result.get("failed")) if isinstance(result, dict) else False
    if doc and doc.get("status") not in {"SUCCEEDED", "FAILED", "CANCELLED"}:
        with contextlib.suppress(TaskConflictError):
            store.record_completion(
                evaluation_task_uuid,
                terminal_status="FAILED" if failed else "SUCCEEDED",
                execution_log=execution_log,
                result=result,
                error_message=str(result.get("error")) if failed and isinstance(result, dict) else None,
            )
    signal_evaluation_complete(r, settings.orch_key_prefix, evaluation_task_uuid, {})


@celery_app.task(name="tasks.notification")
def notification(job_posting_uuid: str, **kwargs: Any) -> None:
    logger.info("notification stub for job %s kwargs=%s", job_posting_uuid, kwargs)
    raise RuntimeError("Not implemented")
