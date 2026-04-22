from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

import redis

TERMINAL_STATUSES = frozenset({"SUCCEEDED", "FAILED", "CANCELLED"})


class TaskNotFoundError(Exception):
    pass


class ParentNotFoundError(Exception):
    pass


class TaskConflictError(Exception):
    pass


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class OrchestrationStore:
    """Orchestration task documents in Redis (same instance as Celery broker)."""

    def __init__(self, client: redis.Redis, key_prefix: str = "orch") -> None:
        self._r = client
        self._p = key_prefix.rstrip(":")

    def _task_key(self, task_uuid: str) -> str:
        return f"{self._p}:task:{task_uuid}"

    def _parent_children_key(self, parent_uuid: str) -> str:
        return f"{self._p}:parent:{parent_uuid}:children"

    def get_raw(self, task_uuid: str) -> dict[str, Any] | None:
        raw = self._r.get(self._task_key(task_uuid))
        if raw is None:
            return None
        return json.loads(raw)

    def require(self, task_uuid: str) -> dict[str, Any]:
        doc = self.get_raw(task_uuid)
        if doc is None:
            raise TaskNotFoundError(task_uuid)
        return doc

    def require_type(self, task_uuid: str, expected: str) -> dict[str, Any]:
        doc = self.require(task_uuid)
        if doc.get("type") != expected:
            raise TaskNotFoundError(task_uuid)
        return doc

    def _would_create_cycle(self, task_uuid: str, parent_uuid: str) -> bool:
        current: str | None = parent_uuid
        seen: set[str] = set()
        while current is not None:
            if current == task_uuid:
                return True
            if current in seen:
                return True
            seen.add(current)
            parent = self.get_raw(current)
            if parent is None:
                break
            raw_parent = parent.get("parentTaskUuid")
            current = raw_parent if raw_parent else None
        return False

    def create_task(
        self,
        *,
        task_uuid: str | None,
        task_type: str,
        parent_task_uuid: str | None,
        initial_context: dict[str, Any] | None,
        status: str = "PENDING",
    ) -> dict[str, Any]:
        tid = task_uuid or str(uuid.uuid4())
        if self._r.exists(self._task_key(tid)):
            raise TaskConflictError(f"Task already exists: {tid}")
        if parent_task_uuid:
            if self.get_raw(parent_task_uuid) is None:
                raise ParentNotFoundError(parent_task_uuid)
            if self._would_create_cycle(tid, parent_task_uuid):
                raise TaskConflictError("Cycle in parent_task_uuid chain")

        now = _now_iso()
        doc: dict[str, Any] = {
            "taskUuid": tid,
            "type": task_type,
            "status": status,
            "createdAt": now,
            "updatedAt": now,
            "initialContext": initial_context or {},
            "lastProgress": None,
            "completion": None,
        }
        if parent_task_uuid:
            doc["parentTaskUuid"] = parent_task_uuid
        self._r.set(self._task_key(tid), json.dumps(doc, default=str))
        if parent_task_uuid:
            self._r.sadd(self._parent_children_key(parent_task_uuid), tid)
        return doc

    def update_task_document(self, task_uuid: str, doc: dict[str, Any]) -> None:
        doc["updatedAt"] = _now_iso()
        self._r.set(self._task_key(task_uuid), json.dumps(doc, default=str))

    def merge_initial_context(self, task_uuid: str, patch: dict[str, Any]) -> dict[str, Any]:
        doc = self.require(task_uuid)
        ctx = dict(doc.get("initialContext") or {})
        ctx.update(patch)
        doc["initialContext"] = ctx
        self.update_task_document(task_uuid, doc)
        return doc

    def record_progress(
        self,
        task_uuid: str,
        *,
        recorded_at: str | None,
        execution_log_fragment: str | None,
        detail: dict[str, Any] | None,
    ) -> None:
        doc = self.require(task_uuid)
        if doc.get("status") in TERMINAL_STATUSES:
            raise TaskConflictError("Task is already in a terminal status")
        progress: dict[str, Any] = {}
        if recorded_at:
            progress["recordedAt"] = recorded_at
        if execution_log_fragment is not None:
            progress["executionLogFragment"] = execution_log_fragment
        if detail is not None:
            progress["detail"] = detail
        if not progress:
            progress["recordedAt"] = _now_iso()
        doc["lastProgress"] = progress
        if doc.get("status") == "PENDING":
            doc["status"] = "RUNNING"
        self.update_task_document(task_uuid, doc)

    def record_completion(
        self,
        task_uuid: str,
        *,
        terminal_status: str,
        execution_log: Any | None,
        result: dict[str, Any] | None,
        error_message: str | None,
    ) -> None:
        doc = self.require(task_uuid)
        if doc.get("status") in TERMINAL_STATUSES:
            raise TaskConflictError("Task is already completed")
        completion: dict[str, Any] = {"terminalStatus": terminal_status}
        if execution_log is not None:
            completion["executionLog"] = execution_log
        if result is not None:
            completion["result"] = result
        if error_message is not None:
            completion["errorMessage"] = error_message
        doc["completion"] = completion
        doc["status"] = terminal_status
        self.update_task_document(task_uuid, doc)
        self._maybe_aggregate_parent(doc)

    def set_status_only(self, task_uuid: str, status: str) -> None:
        doc = self.require(task_uuid)
        doc["status"] = status
        self.update_task_document(task_uuid, doc)

    def _child_uuids(self, parent_uuid: str) -> set[str]:
        members = self._r.smembers(self._parent_children_key(parent_uuid))
        return {m.decode() if isinstance(m, bytes) else str(m) for m in members}

    def _maybe_aggregate_parent(self, completed_child: dict[str, Any]) -> None:
        parent = completed_child.get("parentTaskUuid")
        if not parent:
            return
        parent_doc = self.get_raw(parent)
        if parent_doc is None or parent_doc.get("type") != "COLLECTION_BATCH":
            return
        if parent_doc.get("status") in TERMINAL_STATUSES:
            return
        children = self._child_uuids(parent)
        if not children:
            return
        states: list[str] = []
        for cid in children:
            cdoc = self.get_raw(cid)
            if cdoc is None:
                continue
            states.append(cdoc.get("status", "PENDING"))
        if not states:
            return
        if any(s not in TERMINAL_STATUSES for s in states):
            return
        try:
            if any(s == "FAILED" for s in states):
                self.record_completion(
                    parent,
                    terminal_status="FAILED",
                    execution_log="One or more COLLECTION_QUERY tasks failed",
                    result=None,
                    error_message="Child task failed",
                )
            else:
                self.record_completion(
                    parent,
                    terminal_status="SUCCEEDED",
                    execution_log="All COLLECTION_QUERY tasks finished",
                    result={"childCount": len(children)},
                    error_message=None,
                )
        except TaskConflictError:
            pass

    def to_view(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Return API-shaped OrchestrationTaskView."""
        view: dict[str, Any] = {
            "taskUuid": doc["taskUuid"],
            "type": doc["type"],
            "status": doc["status"],
            "createdAt": doc.get("createdAt"),
            "updatedAt": doc.get("updatedAt"),
            "initialContext": doc.get("initialContext") or {},
            "lastProgress": doc.get("lastProgress"),
            "completion": doc.get("completion"),
        }
        if doc.get("parentTaskUuid"):
            view["parentTaskUuid"] = doc["parentTaskUuid"]
        else:
            view["parentTaskUuid"] = None
        return view
