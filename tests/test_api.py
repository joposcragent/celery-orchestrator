from __future__ import annotations

import uuid

from fastapi.testclient import TestClient


def test_create_and_get_task(client: TestClient) -> None:
    r = client.post(
        "/tasks",
        json={"type": "COLLECTION_BATCH", "initialContext": {"k": 1}},
    )
    assert r.status_code == 201
    data = r.json()
    assert data["type"] == "COLLECTION_BATCH"
    assert data["status"] == "PENDING"
    tid = data["taskUuid"]
    g = client.get(f"/tasks/{tid}")
    assert g.status_code == 200
    body = g.json()
    assert body["taskUuid"] == tid
    assert body["initialContext"] == {"k": 1}


def test_create_task_duplicate_uuid_409(client: TestClient) -> None:
    tid = str(uuid.uuid4())
    r1 = client.post("/tasks", json={"taskUuid": tid, "type": "EVALUATION"})
    assert r1.status_code == 201
    r2 = client.post("/tasks", json={"taskUuid": tid, "type": "EVALUATION"})
    assert r2.status_code == 409


def test_create_task_parent_missing_404(client: TestClient) -> None:
    missing = str(uuid.uuid4())
    r = client.post(
        "/tasks",
        json={"type": "COLLECTION_QUERY", "parentTaskUuid": missing},
    )
    assert r.status_code == 404
    assert "Parent" in r.text


def test_progress_after_completion_409(client: TestClient) -> None:
    r = client.post("/tasks", json={"type": "EVALUATION"})
    tid = r.json()["taskUuid"]
    c1 = client.post(
        f"/tasks/{tid}/completion",
        json={"terminalStatus": "SUCCEEDED", "executionLog": "done"},
    )
    assert c1.status_code == 204
    p = client.post(
        f"/tasks/{tid}/progress",
        json={"detail": {"x": 1}},
    )
    assert p.status_code == 409


def test_queue_collection_query_parent_not_found(client: TestClient) -> None:
    r = client.post(
        "/queue-broker/events/collection-query",
        json={
            "searchQuery": "python",
            "parentBatchTaskUuid": str(uuid.uuid4()),
        },
    )
    assert r.status_code == 404


def test_queue_collection_batch_enqueues(client: TestClient, captured_tasks: list) -> None:
    cid = str(uuid.uuid4())
    r = client.post(
        "/queue-broker/events/collection-batch",
        json={"correlationId": cid, "reason": "manual"},
    )
    assert r.status_code == 204
    assert len(captured_tasks) == 1
    assert captured_tasks[0][0] == "tasks.collection_batch"
    assert captured_tasks[0][1]["correlation_id"] == cid
    assert captured_tasks[0][1]["reason"] == "manual"


def test_queue_evaluation_with_parent_missing(client: TestClient) -> None:
    r = client.post(
        "/queue-broker/events/evaluation",
        json={
            "jobPostingUuid": str(uuid.uuid4()),
            "parentTaskUuid": str(uuid.uuid4()),
        },
    )
    assert r.status_code == 404
