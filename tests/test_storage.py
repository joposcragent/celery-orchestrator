import json

import pytest

from celery_orchestrator.storage.redis_store import RedisTaskStorage


def test_init_and_get_roundtrip(fake_redis):
    st = RedisTaskStorage(fake_redis, "t:")
    st.init_task("u1", name="task.x", kwargs={"a": 1}, parent_id=None)
    doc = st.get_raw("u1")
    assert doc["name"] == "task.x"
    assert doc["kwargs"] == {"a": 1}
    assert doc["state"] == "PENDING"


def test_children_index(fake_redis):
    st = RedisTaskStorage(fake_redis, "t:")
    st.init_task("p", name="parent", kwargs={}, parent_id=None)
    st.init_task("c1", name="child", kwargs={}, parent_id="p")
    st.init_task("c2", name="child", kwargs={}, parent_id="p")
    assert set(st.list_child_ids("p")) == {"c1", "c2"}


def test_update_task(fake_redis):
    st = RedisTaskStorage(fake_redis, "t:")
    st.init_task("u", name="n", kwargs={})
    st.update_task("u", state="SUCCESS", result={"ok": True})
    doc = st.get_raw("u")
    assert doc["state"] == "SUCCESS"
    assert doc["result"] == {"ok": True}


def test_json_serializes_nested(fake_redis):
    st = RedisTaskStorage(fake_redis, "t:")
    st.init_task("u", name="n", kwargs={"d": {"nested": [1, 2]}})
    raw = fake_redis.get("t:task:u")
    assert json.loads(raw)["kwargs"]["d"]["nested"] == [1, 2]
