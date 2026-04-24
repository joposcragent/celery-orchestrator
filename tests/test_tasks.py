import json
from unittest.mock import MagicMock

import httpx
import pytest
import respx

from celery_orchestrator.config import Settings
from celery_orchestrator.storage.redis_store import RedisTaskStorage


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch) -> Settings:
    s = Settings(
        redis_url="redis://localhost:6379/0",
        settings_manager_base_url="http://settings.test",
        crawler_base_url="http://crawler.test",
        evaluator_base_url="http://eval.test",
    )
    monkeypatch.setattr("celery_orchestrator.tasks.definitions.get_settings", lambda: s)
    return s


@respx.mock
def test_collection_batch_empty_list(fake_redis, settings):
    from celery_orchestrator.tasks.definitions import collection_batch

    respx.get("http://settings.test/search-query/list").mock(return_value=httpx.Response(200, json=[]))
    tid = "550e8400-e29b-41d4-a716-446655440030"
    collection_batch.apply(kwargs={"correlationId": "x", "createdAt": "2026-01-01T00:00:00Z"}, task_id=tid).get()
    st = RedisTaskStorage(fake_redis, settings.orch_redis_prefix)
    doc = st.get_raw(tid)
    assert doc["state"] == "REVOKED"
    assert "не настроены" in doc["result"]


@respx.mock
def test_collection_batch_spawns_children(fake_redis, settings, monkeypatch: pytest.MonkeyPatch):
    from celery_orchestrator.tasks.definitions import collection_batch

    payload = [[{"uuid": "550e8400-e29b-41d4-a716-446655440031", "query": "https://hh.example"}]]
    respx.get("http://settings.test/search-query/list").mock(return_value=httpx.Response(200, json=payload))
    send = MagicMock()
    monkeypatch.setattr("celery_orchestrator.tasks.definitions.app.send_task", send)
    tid = "550e8400-e29b-41d4-a716-446655440032"
    collection_batch.apply(kwargs={"correlationId": "x", "createdAt": "2026-01-01T00:00:00Z"}, task_id=tid).get()
    assert send.call_count == 1
    args, kwargs = send.call_args
    assert args[0] == "task.collection-query"
    assert kwargs.get("queue") == "celery"
    assert kwargs["kwargs"]["searchQuery"] == "https://hh.example"
    assert kwargs["kwargs"]["parentId"] == tid


@respx.mock
def test_collection_query_success_then_running_until_finish(fake_redis, settings):
    from celery_orchestrator.tasks.definitions import collection_query, finish

    captured: dict[str, object] = {}

    def on_request(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content.decode()) if request.content else {}
        return httpx.Response(200)

    respx.post("http://crawler.test/crawler/start").mock(side_effect=on_request)
    tid = "550e8400-e29b-41d4-a716-446655440033"
    collection_query.apply(
        kwargs={"searchQuery": "https://q", "parentId": tid, "correlationId": tid},
        task_id=tid,
    ).get()
    assert captured.get("body") == {"query": "https://q"}
    st = RedisTaskStorage(fake_redis, settings.orch_redis_prefix)
    assert st.get_raw(tid)["state"] == "RUNNING"
    finish.apply(
        kwargs={
            "correlationId": tid,
            "parentTaskResult": "done",
            "parentTaskStatus": "SUCCEEDED",
        },
        task_id="550e8400-e29b-41d4-a716-4466554400aa",
    ).get()
    assert st.get_raw(tid)["state"] == "SUCCESS"
    assert st.get_raw(tid)["result"] == "done"
    assert st.get_raw("550e8400-e29b-41d4-a716-4466554400aa")["state"] == "SUCCESS"


@respx.mock
def test_collection_query_http_error(fake_redis, settings):
    from celery_orchestrator.tasks.definitions import collection_query

    respx.post("http://crawler.test/crawler/start").mock(return_value=httpx.Response(502, text="bad"))
    tid = "550e8400-e29b-41d4-a716-446655440034"
    collection_query.apply(kwargs={"searchQuery": "x"}, task_id=tid).get()
    st = RedisTaskStorage(fake_redis, settings.orch_redis_prefix)
    assert st.get_raw(tid)["state"] == "FAILURE"
    assert "502" in st.get_raw(tid)["result"]


def test_collection_query_empty_search(fake_redis, settings):
    from celery_orchestrator.tasks.definitions import collection_query

    tid = "550e8400-e29b-41d4-a716-446655440035"
    collection_query.apply(kwargs={"searchQuery": ""}, task_id=tid).get()
    st = RedisTaskStorage(fake_redis, settings.orch_redis_prefix)
    assert st.get_raw(tid)["state"] == "FAILURE"


def test_collection_query_rejects_list_search_query(fake_redis, settings):
    from celery_orchestrator.tasks.definitions import collection_query

    tid = "550e8400-e29b-41d4-a716-446655440038"
    collection_query.apply(kwargs={"searchQuery": ["legacy"]}, task_id=tid).get()
    st = RedisTaskStorage(fake_redis, settings.orch_redis_prefix)
    assert st.get_raw(tid)["state"] == "FAILURE"
    assert "строкой" in st.get_raw(tid)["result"]


@respx.mock
def test_evaluation_success_then_running_until_finish(fake_redis, settings):
    from celery_orchestrator.tasks.definitions import evaluation, finish

    jid = "550e8400-e29b-41d4-a716-446655440036"
    respx.post(f"http://eval.test/evaluate/async/{jid}").mock(return_value=httpx.Response(200))
    tid = "550e8400-e29b-41d4-a716-446655440037"
    evaluation.apply(kwargs={"jobPostingUuid": jid}, task_id=tid).get()
    st = RedisTaskStorage(fake_redis, settings.orch_redis_prefix)
    assert st.get_raw(tid)["state"] == "RUNNING"
    finish.apply(
        kwargs={
            "correlationId": tid,
            "parentTaskResult": {"eval": "ok"},
            "parentTaskStatus": "SUCCEEDED",
        },
        task_id="550e8400-e29b-41d4-a716-4466554400bb",
    ).get()
    assert st.get_raw(tid)["state"] == "SUCCESS"
    assert st.get_raw(tid)["result"] == {"eval": "ok"}


def test_evaluation_missing_uuid(fake_redis, settings):
    from celery_orchestrator.tasks.definitions import evaluation

    tid = "550e8400-e29b-41d4-a716-446655440038"
    evaluation.apply(kwargs={}, task_id=tid).get()
    st = RedisTaskStorage(fake_redis, settings.orch_redis_prefix)
    assert st.get_raw(tid)["state"] == "FAILURE"


def test_progress_missing_parent(fake_redis, settings):
    from celery_orchestrator.tasks.definitions import progress

    tid = "550e8400-e29b-41d4-a716-446655440039"
    progress.apply(kwargs={"correlationId": "550e8400-e29b-41d4-a716-446655440099"}, task_id=tid).get()
    st = RedisTaskStorage(fake_redis, settings.orch_redis_prefix)
    assert st.get_raw(tid)["state"] == "FAILURE"


def test_progress_success(fake_redis, settings):
    from celery_orchestrator.tasks.definitions import progress

    parent = "550e8400-e29b-41d4-a716-446655440040"
    st = RedisTaskStorage(fake_redis, settings.orch_redis_prefix)
    st.init_task(parent, name="p", kwargs={})
    tid = "550e8400-e29b-41d4-a716-446655440041"
    progress.apply(kwargs={"correlationId": parent}, task_id=tid).get()
    assert st.get_raw(tid)["state"] == "SUCCESS"


def test_finish_updates_parent(fake_redis, settings):
    from celery_orchestrator.tasks.definitions import finish

    parent = "550e8400-e29b-41d4-a716-446655440042"
    st = RedisTaskStorage(fake_redis, settings.orch_redis_prefix)
    st.init_task(parent, name="p", kwargs={})
    tid = "550e8400-e29b-41d4-a716-446655440043"
    finish.apply(
        kwargs={
            "correlationId": parent,
            "parentTaskResult": {"done": True},
            "parentTaskStatus": "SUCCEEDED",
        },
        task_id=tid,
    ).get()
    assert st.get_raw(parent)["state"] == "SUCCESS"
    assert st.get_raw(parent)["result"] == {"done": True}
    assert st.get_raw(tid)["state"] == "SUCCESS"


def test_finish_missing_parent_result(fake_redis, settings):
    from celery_orchestrator.tasks.definitions import finish

    tid = "550e8400-e29b-41d4-a716-446655440044"
    finish.apply(kwargs={"correlationId": "550e8400-e29b-41d4-a716-446655440042"}, task_id=tid).get()
    st = RedisTaskStorage(fake_redis, settings.orch_redis_prefix)
    assert st.get_raw(tid)["state"] == "FAILURE"


def test_notification_task(fake_redis, settings):
    from celery_orchestrator.tasks.definitions import notification

    tid = "550e8400-e29b-41d4-a716-446655440045"
    notification.apply(kwargs={}, task_id=tid).get()
    st = RedisTaskStorage(fake_redis, settings.orch_redis_prefix)
    assert st.get_raw(tid)["state"] == "REVOKED"


def test_beat_enqueues_batch(fake_redis, settings, monkeypatch: pytest.MonkeyPatch):
    from celery_orchestrator.tasks.definitions import beat_hourly_collection_batch

    send = MagicMock()
    monkeypatch.setattr("celery_orchestrator.tasks.definitions.app.send_task", send)
    beat_hourly_collection_batch.apply().get()
    assert send.call_count == 1
    args, _kwargs = send.call_args
    assert args[0] == "task.collection-batch"
