from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from celery_orchestrator.main import app


@pytest.fixture
def client(fake_redis, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setattr("celery_orchestrator.api.routes.celery_app.send_task", MagicMock())
    return TestClient(app)


def test_enqueue_passes_queue_to_send_task(fake_redis, monkeypatch):
    mock_send = MagicMock()
    monkeypatch.setattr("celery_orchestrator.api.routes.celery_app.send_task", mock_send)
    c = TestClient(app)
    c.post(
        "/events-queue/progress",
        json={"correlationId": "550e8400-e29b-41d4-a716-446655440000", "createdAt": "2026-01-01T00:00:00Z"},
    )
    assert mock_send.call_count == 1
    assert mock_send.call_args.kwargs.get("queue") == "celery"


def test_post_progress_204(client):
    r = client.post(
        "/events-queue/progress",
        json={"correlationId": "550e8400-e29b-41d4-a716-446655440000", "createdAt": "2026-01-01T00:00:00Z"},
    )
    assert r.status_code == 204


def test_post_progress_accepts_extra_fields_204(client):
    r = client.post(
        "/events-queue/progress",
        json={
            "correlationId": "550e8400-e29b-41d4-a716-446655440000",
            "createdAt": "2026-01-01T00:00:00Z",
            "jobPostingUuid": "550e8400-e29b-41d4-a716-446655440002",
            "vacancyUrl": "https://example.com/v",
            "executionLog": "x",
            "status": "FAILED",
            "futureField": {"nested": True},
        },
    )
    assert r.status_code == 204


def test_post_progress_empty_body_422(client):
    r = client.post("/events-queue/progress", content=b"")
    assert r.status_code == 422
    assert "request body is required" in r.json()["detail"]


def test_post_finish_204(client):
    r = client.post(
        "/events-queue/finish",
        json={
            "correlationId": "550e8400-e29b-41d4-a716-446655440000",
            "createdAt": "2026-01-01T00:00:00Z",
            "status": "SUCCEEDED",
            "result": {"x": 1},
        },
    )
    assert r.status_code == 204


def test_post_finish_reserved_fields_in_snapshot_not_celery_kwargs(fake_redis, monkeypatch):
    from celery_orchestrator.storage.redis_store import RedisTaskStorage

    mock_send = MagicMock()
    monkeypatch.setattr("celery_orchestrator.api.routes.celery_app.send_task", mock_send)
    c = TestClient(app)
    cid = "550e8400-e29b-41d4-a716-4466554400cc"
    r = c.post(
        "/events-queue/finish",
        json={
            "correlationId": cid,
            "createdAt": "2026-01-01T00:00:00Z",
            "status": "SUCCEEDED",
            "result": {"x": 1},
            "executionLog": "done",
        },
    )
    assert r.status_code == 204
    celery_kwargs = mock_send.call_args.kwargs["kwargs"]
    assert "result" not in celery_kwargs
    assert "status" not in celery_kwargs
    assert "executionLog" not in celery_kwargs
    assert celery_kwargs["correlationId"] == cid

    task_id = mock_send.call_args.kwargs["task_id"]
    st = RedisTaskStorage(fake_redis, "orch:")
    doc = st.get_raw(task_id)
    assert doc is not None
    assert doc["result"] == {"x": 1}
    assert doc["executionLog"] == "done"
    assert doc["finishEventStatus"] == "SUCCEEDED"


def test_post_event_unknown_returns_422(client):
    r = client.post(
        "/events-queue/unknown-event",
        json={"correlationId": "550e8400-e29b-41d4-a716-446655440001", "createdAt": "2026-01-01T00:00:00Z"},
    )
    assert r.status_code == 422


def test_post_event_collection_batch_204(client):
    r = client.post(
        "/events-queue/collection-batch",
        json={"correlationId": "550e8400-e29b-41d4-a716-446655440001", "createdAt": "2026-01-01T00:00:00Z"},
    )
    assert r.status_code == 204


def test_get_task_404(client):
    r = client.get("/tasks/550e8400-e29b-41d4-a716-446655440099")
    assert r.status_code == 404
    assert r.text == "not found"


def test_get_task_200(client, fake_redis):
    from celery_orchestrator.storage.redis_store import RedisTaskStorage

    st = RedisTaskStorage(fake_redis, "orch:")
    uid = "550e8400-e29b-41d4-a716-446655440010"
    st.init_task(uid, name="task.x", kwargs={"k": 1})
    r = client.get(f"/tasks/{uid}")
    assert r.status_code == 200
    body = r.json()
    assert body["uuid"] == uid
    assert body["name"] == "task.x"


def test_get_children_404(client):
    r = client.get("/tasks/550e8400-e29b-41d4-a716-446655440011/children")
    assert r.status_code == 404


def test_get_children_200(client, fake_redis):
    from celery_orchestrator.storage.redis_store import RedisTaskStorage

    st = RedisTaskStorage(fake_redis, "orch:")
    p = "550e8400-e29b-41d4-a716-446655440020"
    c = "550e8400-e29b-41d4-a716-446655440021"
    st.init_task(p, name="parent", kwargs={})
    st.init_task(c, name="child", kwargs={}, parent_id=p)
    r = client.get(f"/tasks/{p}/children")
    assert r.status_code == 200
    assert len(r.json()) == 1
    assert r.json()[0]["uuid"] == c


def test_send_task_failure_returns_500(fake_redis, monkeypatch):
    def boom(*_a, **_k):
        raise ConnectionError("redis down")

    monkeypatch.setattr("celery_orchestrator.api.routes.celery_app.send_task", boom)
    c = TestClient(app, raise_server_exceptions=False)
    r = c.post(
        "/events-queue/progress",
        json={"correlationId": "550e8400-e29b-41d4-a716-446655440000", "createdAt": "2026-01-01T00:00:00Z"},
    )
    assert r.status_code == 500
    assert "redis down" in r.text
