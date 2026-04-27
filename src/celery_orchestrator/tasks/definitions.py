from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx
from celery.utils.log import get_task_logger

from celery_orchestrator.celery_app import app
from celery_orchestrator.config import get_settings
from celery_orchestrator.orchestration_wait import wait_until_orch_not_running
from celery_orchestrator.storage.redis_store import RedisTaskStorage, get_redis_client, utc_now_iso

log = get_task_logger(__name__)


def _storage() -> RedisTaskStorage:
    s = get_settings()
    return RedisTaskStorage(get_redis_client(s), s.orch_redis_prefix)


def _corr(kwargs: dict[str, Any]) -> str | None:
    return kwargs.get("correlationId") or kwargs.get("correlation_id")


def _mark_started(task_id: str, worker: str | None) -> None:
    st = _storage()
    st.update_task(
        task_id,
        started=utc_now_iso(),
        state="STARTED",
        worker=worker,
    )


def _flatten_query_list(payload: Any) -> list[dict[str, Any]]:
    """Normalize settings-manager /search-query/list JSON (array or nested array)."""
    if payload is None:
        return []
    if isinstance(payload, list):
        if not payload:
            return []
        if isinstance(payload[0], list):
            return [x for group in payload for x in group if isinstance(x, dict)]
        if isinstance(payload[0], dict):
            return payload  # type: ignore[return-value]
    return []


def _send_task(task_name: str, *, task_id: str, kwargs: dict[str, Any]) -> None:
    q = get_settings().celery_default_queue
    app.send_task(task_name, task_id=task_id, kwargs=kwargs, queue=q)


def _map_finish_status_to_state(status: str) -> str:
    if status == "SUCCEEDED":
        return "SUCCESS"
    if status == "FAILED":
        return "FAILURE"
    if status == "CANCELLED":
        return "REVOKED"
    return "FAILURE"


@app.task(name="task.beat-hourly-collection-batch")
def beat_hourly_collection_batch() -> None:
    now = datetime.now(timezone.utc).isoformat()
    tid = str(uuid.uuid4())
    kwargs: dict[str, Any] = {
        "createdAt": now,
    }
    st = _storage()
    st.init_task(tid, name="task.collection-batch", kwargs=kwargs)
    _send_task("task.collection-batch", task_id=tid, kwargs=kwargs)
    log.info(
        "beat_hourly_collection_batch enqueued task_id=%s correlationId=%s",
        tid,
        _corr(kwargs) or "none",
    )


@app.task(name="task.collection-batch", bind=True)
def collection_batch(self, **kwargs: Any) -> None:
    task_id = self.request.id
    worker = getattr(self.request, "hostname", None)
    _mark_started(task_id, worker)
    st = _storage()
    s = get_settings()
    url = f"{s.settings_manager_base_url.rstrip('/')}/search-query/list"
    log.info(
        "collection_batch start task_id=%s settings_url=%s",
        task_id,
        url,
    )
    try:
        with httpx.Client(timeout=s.http_timeout_seconds) as client:
            r = client.get(url)
    except Exception as exc:  # noqa: BLE001
        log.exception("collection_batch httpx error task_id=%s", task_id)
        st.update_task(task_id, state="FAILURE", exception=str(exc), result=str(exc))
        raise
    if r.status_code != 200:
        log.warning(
            "collection_batch settings-manager not ok task_id=%s status=%s response_len=%d",
            task_id,
            r.status_code,
            len(r.text or ""),
        )
        msg = f"settings-manager HTTP {r.status_code} {r.text}"
        st.update_task(task_id, state="FAILURE", result=msg, exception=msg)
        return
    try:
        payload = r.json()
    except json.JSONDecodeError:
        payload = []
    rows = _flatten_query_list(payload)
    if not rows:
        log.warning("collection_batch no search queries after flatten task_id=%s", task_id)
        st.update_task(
            task_id,
            state="REVOKED",
            result="Поисковые запросы не настроены",
        )
        return
    count = 0
    for row in rows:
        q = row.get("query")
        if not q:
            continue
        child_id = str(uuid.uuid4())
        child_kwargs: dict[str, Any] = {
            "searchQuery": str(q),
            "parentId": task_id
        }
        st.init_task(
            child_id,
            name="task.collection-query",
            kwargs=child_kwargs,
            parent_id=task_id,
        )
        _send_task("task.collection-query", task_id=child_id, kwargs=child_kwargs)
        count += 1
    st.update_task(
        task_id,
        state="SUCCESS",
        result=f"Запущено {count} асинхронных процессов сбора вакансий",
    )
    log.info("collection_batch success task_id=%s spawned_child_tasks=%d", task_id, count)


@app.task(name="task.collection-query", bind=True)
def collection_query(self, **kwargs: Any) -> None:
    task_id = self.request.id
    worker = getattr(self.request, "hostname", None)
    _mark_started(task_id, worker)
    st = _storage()
    log.info(
        "collection_query start task_id=%s",
        task_id,
    )
    raw_sq = kwargs.get("searchQuery")
    if isinstance(raw_sq, list):
        log.warning("collection_query searchQuery is list task_id=%s", task_id)
        st.update_task(
            task_id,
            state="FAILURE",
            result="Поисковый запрос должен быть строкой, не массивом",
        )
        return
    query_str = str(raw_sq).strip() if raw_sq is not None else ""
    if not query_str:
        log.warning("collection_query empty searchQuery task_id=%s", task_id)
        st.update_task(task_id, state="FAILURE", result="Поисковый запрос пустой")
        return
    s = get_settings()
    body = {"query": query_str}
    headers = {"X-Joposcragent-correlationId": task_id}
    url = f"{s.crawler_base_url.rstrip('/')}/crawler/start"
    try:
        with httpx.Client(timeout=s.http_timeout_seconds) as client:
            r = client.post(url, json=body, headers=headers)
    except Exception as exc:  # noqa: BLE001
        log.exception("collection_query httpx error task_id=%s crawler_url=%s", task_id, url)
        st.update_task(task_id, state="FAILURE", exception=str(exc), result=str(exc))
        raise
    if r.status_code != 200:
        log.warning(
            "collection_query crawler not ok task_id=%s status=%s response_len=%d",
            task_id,
            r.status_code,
            len(r.text or ""),
        )
        msg = f"Не удалось запустить сбор: {r.status_code} {r.text}"
        st.update_task(task_id, state="FAILURE", result=msg, exception=msg)
        return
    # RUNNING: ждём task.finish с correlationId = uuid этой задачи (async.md п.4).
    st.update_task(task_id, state="RUNNING", result=None)
    log.info("collection_query state=RUNNING waiting for task.finish task_id=%s", task_id)
    # Воркер не должен завершить Celery-задачу SUCCESS до finish: блокируемся на Redis.
    # apply() в тестах — request.is_eager=True, ожидание пропускаем (иначе deadlock).
    if not self.request.is_eager:
        wait_until_orch_not_running(task_id)


@app.task(name="task.evaluation", bind=True)
def evaluation(self, **kwargs: Any) -> None:
    task_id = self.request.id
    worker = getattr(self.request, "hostname", None)
    _mark_started(task_id, worker)
    st = _storage()
    job_uuid = kwargs.get("jobPostingUuid")
    log.info(
        "evaluation start task_id=%s jobPostingUuid=%s correlationId=%s",
        task_id,
        job_uuid or "none",
        _corr(kwargs) or "none",
    )
    if not job_uuid:
        log.warning("evaluation missing jobPostingUuid task_id=%s", task_id)
        st.update_task(task_id, state="FAILURE", result="Отсутствует UUID вакансии")
        return
    s = get_settings()
    headers = {"X-Joposcragent-correlationId": task_id}
    url = f"{s.evaluator_base_url.rstrip('/')}/evaluate/async/{job_uuid}"
    try:
        with httpx.Client(timeout=s.http_timeout_seconds) as client:
            r = client.post(url, headers=headers)
    except Exception as exc:  # noqa: BLE001
        log.exception("evaluation httpx error task_id=%s evaluator_url=%s", task_id, url)
        st.update_task(task_id, state="FAILURE", exception=str(exc), result=str(exc))
        raise
    if r.status_code != 200:
        log.warning(
            "evaluation not ok task_id=%s status=%s response_len=%d",
            task_id,
            r.status_code,
            len(r.text or ""),
        )
        msg = f"Не удалось запустить оценку: {r.status_code} {r.text}"
        st.update_task(task_id, state="FAILURE", result=msg, exception=msg)
        return
    # RUNNING: ждём task.finish с correlationId = uuid этой задачи (async.md).
    st.update_task(task_id, state="RUNNING", result=None)
    log.info("evaluation state=RUNNING waiting for task.finish task_id=%s", task_id)
    if not self.request.is_eager:
        wait_until_orch_not_running(task_id)


@app.task(name="task.notification", bind=True)
def notification(self, **kwargs: Any) -> None:  # noqa: ARG001
    task_id = self.request.id
    worker = getattr(self.request, "hostname", None)
    _mark_started(task_id, worker)
    st = _storage()
    log.info("notification not implemented task_id=%s", task_id)
    st.update_task(task_id, state="REVOKED", result="Not implemented")


@app.task(name="task.progress", bind=True)
def progress(self, **kwargs: Any) -> None:
    task_id = self.request.id
    worker = getattr(self.request, "hostname", None)
    _mark_started(task_id, worker)
    st = _storage()
    parent = _corr(kwargs)
    if not parent or not st.exists(parent):
        log.warning("progress parent not found task_id=%s parent_id=%s", task_id, parent)
        msg = f"Не удалось найти родительскую задачу {parent}"
        st.update_task(task_id, state="FAILURE", result=msg)
        return
    log.info("progress success task_id=%s parent_id=%s", task_id, parent)
    st.update_task(task_id, state="SUCCESS")


@app.task(name="task.finish", bind=True)
def finish(self, **kwargs: Any) -> None:
    task_id = self.request.id
    worker = getattr(self.request, "hostname", None)
    _mark_started(task_id, worker)
    st = _storage()
    log.info("finish start task_id=%s correlationId=%s", task_id, _corr(kwargs) or "none")
    doc = st.get_raw(task_id) or {}
    parent_result = doc.get("result")
    parent_status = doc.get("finishEventStatus")
    if parent_result is None or parent_status is None:
        log.warning(
            "finish missing parent snapshot fields task_id=%s has_result=%s has_finishEventStatus=%s",
            task_id,
            parent_result is not None,
            parent_status is not None,
        )
        st.update_task(
            task_id,
            state="FAILURE",
            result="Отсутствуют результат и статус родительской задачи",
        )
        return
    parent_uuid = _corr(kwargs)
    if not parent_uuid:
        log.warning("finish missing parent correlation in kwargs task_id=%s", task_id)
        st.update_task(
            task_id,
            state="FAILURE",
            result="Отсутствуют correlation_id родительской задачи",
        )
        return
    if not st.exists(parent_uuid):
        log.warning("finish parent task not in redis task_id=%s parent_uuid=%s", task_id, parent_uuid)
        msg = f"Не удалось найти родительскую задачу {parent_uuid}"
        st.update_task(task_id, state="FAILURE", result=msg)
        return
    mapped = _map_finish_status_to_state(str(parent_status))
    st.update_task(parent_uuid, state=mapped, result=parent_result)
    st.update_task(task_id, state="SUCCESS")
    log.info("finish success task_id=%s parent_uuid=%s mapped_state=%s", task_id, parent_uuid, mapped)
