from __future__ import annotations

from typing import Any


def orchestration_task_view(doc: dict[str, Any]) -> dict[str, Any]:
    """Shape stored document into OrchestrationTaskView (OpenAPI)."""
    raw_state = doc.get("state", "PENDING")
    # Внутренний RUNNING (ожидание task.finish) в OpenAPI отображаем как STARTED.
    api_state = "STARTED" if raw_state == "RUNNING" else raw_state
    view: dict[str, Any] = {
        "name": doc.get("name", ""),
        "uuid": doc.get("uuid", ""),
        "state": api_state,
        "args": doc.get("args", []),
        "kwargs": doc.get("kwargs", {}),
        "result": doc.get("result"),
        "received": doc.get("received"),
        "started": doc.get("started"),
        "failed": doc.get("failed"),
        "retries": doc.get("retries", 0),
        "worker": doc.get("worker"),
        "exception": doc.get("exception"),
        "timestamp": doc.get("timestamp"),
        "traceback": doc.get("traceback"),
        "clock": doc.get("clock"),
        "root": doc.get("root"),
        "rootId": doc.get("rootId"),
        "parent": doc.get("parent"),
        "parentId": doc.get("parentId") or doc.get("parent_id"),
        "children": doc.get("children"),
    }
    if "executionLog" in doc:
        view["executionLog"] = doc["executionLog"]
    return view
