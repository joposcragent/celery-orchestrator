from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx

from celery_orchestrator.celery_app import app
from celery_orchestrator.config import get_settings
from celery_orchestrator.orchestration_wait import wait_until_orch_not_running
from celery_orchestrator.storage.redis_store import RedisTaskStorage, get_redis_client, utc_now_iso


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
        "correlationId": str(uuid.uuid4()),
        "createdAt": now,
    }
    st = _storage()
    st.init_task(tid, name="task.collection-batch", kwargs=kwargs)
    _send_task("task.collection-batch", task_id=tid, kwargs=kwargs)


@app.task(name="task.collection-batch", bind=True)
def collection_batch(self, **kwargs: Any) -> None:
    task_id = self.request.id
    worker = getattr(self.request, "hostname", None)
    _mark_started(task_id, worker)
    st = _storage()
    s = get_settings()
    url = f"{s.settings_manager_base_url.rstrip('/')}/search-query/list"
    try:
        with httpx.Client(timeout=s.http_timeout_seconds) as client:
            r = client.get(url)
    except Exception as exc:  # noqa: BLE001
        st.update_task(task_id, state="FAILURE", exception=str(exc), result=str(exc))
        raise
    if r.status_code != 200:
        msg = f"settings-manager HTTP {r.status_code} {r.text}"
        st.update_task(task_id, state="FAILURE", result=msg, exception=msg)
        return
    try:
        payload = r.json()
    except json.JSONDecodeError:
        payload = []
    rows = _flatten_query_list(payload)
    if not rows:
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
            "parentId": task_id,
            "correlationId": task_id,
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


@app.task(name="task.collection-query", bind=True)
def collection_query(self, **kwargs: Any) -> None:
    task_id = self.request.id
    worker = getattr(self.request, "hostname", None)
    _mark_started(task_id, worker)
    st = _storage()
    raw_sq = kwargs.get("searchQuery")
    if isinstance(raw_sq, list):
        st.update_task(
            task_id,
            state="FAILURE",
            result="Поисковый запрос должен быть строкой, не массивом",
        )
        return
    query_str = str(raw_sq).strip() if raw_sq is not None else ""
    if not query_str:
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
        st.update_task(task_id, state="FAILURE", exception=str(exc), result=str(exc))
        raise
    if r.status_code != 200:
        msg = f"Не удалось запустить сбор: {r.status_code} {r.text}"
        st.update_task(task_id, state="FAILURE", result=msg, exception=msg)
        return
    # RUNNING: ждём task.finish с correlationId = uuid этой задачи (async.md п.4).
    st.update_task(task_id, state="RUNNING", result=None)
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
    if not job_uuid:
        st.update_task(task_id, state="FAILURE", result="Отсутствует UUID вакансии")
        return
    s = get_settings()
    headers = {"X-Joposcragent-correlationId": task_id}
    url = f"{s.evaluator_base_url.rstrip('/')}/evaluate/async/{job_uuid}"
    try:
        with httpx.Client(timeout=s.http_timeout_seconds) as client:
            r = client.post(url, headers=headers)
    except Exception as exc:  # noqa: BLE001
        st.update_task(task_id, state="FAILURE", exception=str(exc), result=str(exc))
        raise
    if r.status_code != 200:
        msg = f"Не удалось запустить оценку: {r.status_code} {r.text}"
        st.update_task(task_id, state="FAILURE", result=msg, exception=msg)
        return
    # RUNNING: ждём task.finish с correlationId = uuid этой задачи (async.md).
    st.update_task(task_id, state="RUNNING", result=None)
    if not self.request.is_eager:
        wait_until_orch_not_running(task_id)


@app.task(name="task.notification", bind=True)
def notification(self, **kwargs: Any) -> None:  # noqa: ARG001
    task_id = self.request.id
    worker = getattr(self.request, "hostname", None)
    _mark_started(task_id, worker)
    st = _storage()
    st.update_task(task_id, state="REVOKED", result="Not implemented")


@app.task(name="task.progress", bind=True)
def progress(self, **kwargs: Any) -> None:
    task_id = self.request.id
    worker = getattr(self.request, "hostname", None)
    _mark_started(task_id, worker)
    st = _storage()
    parent = _corr(kwargs)
    if not parent or not st.exists(parent):
        msg = f"Не удалось найти родительскую задачу {parent}"
        st.update_task(task_id, state="FAILURE", result=msg)
        return
    st.update_task(task_id, state="SUCCESS")


@app.task(name="task.finish", bind=True)
def finish(self, **kwargs: Any) -> None:
    task_id = self.request.id
    worker = getattr(self.request, "hostname", None)
    _mark_started(task_id, worker)
    st = _storage()
    parent_result = kwargs.get("parentTaskResult")
    parent_status = kwargs.get("parentTaskStatus")
    if parent_result is None or parent_status is None:
        st.update_task(
            task_id,
            state="FAILURE",
            result="Отсутствуют результат и статус родительской задачи",
        )
        return
    parent_uuid = _corr(kwargs)
    if not parent_uuid:
        st.update_task(
            task_id,
            state="FAILURE",
            result="Отсутствуют correlation_id родительской задачи",
        )
        return
    if not st.exists(parent_uuid):
        msg = f"Не удалось найти родительскую задачу {parent_uuid}"
        st.update_task(task_id, state="FAILURE", result=msg)
        return
    mapped = _map_finish_status_to_state(str(parent_status))
    st.update_task(parent_uuid, state=mapped, result=parent_result)
    st.update_task(task_id, state="SUCCESS")
