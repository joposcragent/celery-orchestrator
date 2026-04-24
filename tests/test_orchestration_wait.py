import threading
import time

import pytest

from celery_orchestrator.config import Settings
from celery_orchestrator.orchestration_wait import wait_until_orch_not_running
from celery_orchestrator.storage.redis_store import RedisTaskStorage


def test_wait_until_orch_not_running_releases(fake_redis):
    s = Settings(
        redis_url="redis://localhost:6379/0",
        orchestration_finish_wait_timeout_seconds=3.0,
        orchestration_finish_poll_interval_seconds=0.05,
    )
    st = RedisTaskStorage(fake_redis, s.orch_redis_prefix)
    tid = "550e8400-e29b-41d4-a716-4466554400c0"
    st.init_task(tid, name="task.collection-query", kwargs={})
    st.update_task(tid, state="RUNNING")

    def flip() -> None:
        time.sleep(0.12)
        st.update_task(tid, state="SUCCESS", result="ok")

    threading.Thread(target=flip, daemon=True).start()
    wait_until_orch_not_running(tid, settings=s)
    assert st.get_raw(tid)["state"] == "SUCCESS"


def test_wait_until_orch_timeout(fake_redis):
    s = Settings(
        redis_url="redis://localhost:6379/0",
        orchestration_finish_wait_timeout_seconds=0.2,
        orchestration_finish_poll_interval_seconds=0.05,
    )
    st = RedisTaskStorage(fake_redis, s.orch_redis_prefix)
    tid = "550e8400-e29b-41d4-a716-4466554400c1"
    st.init_task(tid, name="task.collection-query", kwargs={})
    st.update_task(tid, state="RUNNING")
    with pytest.raises(TimeoutError):
        wait_until_orch_not_running(tid, settings=s)
    assert st.get_raw(tid)["state"] == "FAILURE"
