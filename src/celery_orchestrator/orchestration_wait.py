"""Wait until orchestration state leaves RUNNING (after task.finish updates Redis)."""

from __future__ import annotations

import logging
import time

from celery_orchestrator.config import Settings, get_settings
from celery_orchestrator.storage.redis_store import RedisTaskStorage, get_redis_client

_log = logging.getLogger("celery_orchestrator.orchestration_wait")


def wait_until_orch_not_running(task_id: str, settings: Settings | None = None) -> None:
    """
    Poll orchestration snapshot until state is no longer RUNNING (terminal set by task.finish)
    or timeout. Raises TimeoutError on timeout (Celery task should then fail).
    """
    s = settings or get_settings()
    st = RedisTaskStorage(get_redis_client(s), s.orch_redis_prefix)
    deadline = time.monotonic() + s.orchestration_finish_wait_timeout_seconds
    interval = s.orchestration_finish_poll_interval_seconds
    _log.info(
        "wait_until_orch_not_running start task_id=%s timeout_s=%s poll_s=%s",
        task_id,
        s.orchestration_finish_wait_timeout_seconds,
        interval,
    )
    while time.monotonic() < deadline:
        doc = st.get_raw(task_id)
        if doc is None:
            return
        if doc.get("state") != "RUNNING":
            return
        time.sleep(interval)
    msg = "Timeout waiting for orchestration task.finish"
    _log.error("wait_until_orch_not_running timeout task_id=%s", task_id)
    st.update_task(task_id, state="FAILURE", result=msg, exception=msg)
    raise TimeoutError(msg)
