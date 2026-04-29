from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import redis

from celery_orchestrator.config import Settings, get_settings


def get_redis_client(settings: Settings | None = None) -> redis.Redis:
    s = settings or get_settings()
    return redis.Redis.from_url(s.redis_url, decode_responses=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class RedisTaskStorage:
    """Persists orchestration task snapshots and parent/child links."""

    def __init__(
        self,
        client: redis.Redis,
        prefix: str,
        *,
        record_ttl_seconds: int | None = None,
    ) -> None:
        self._r = client
        self._prefix = prefix
        self._ttl = (
            record_ttl_seconds
            if record_ttl_seconds is not None
            else get_settings().orch_redis_record_ttl_seconds
        )

    def _task_key(self, task_uuid: str) -> str:
        return f"{self._prefix}task:{task_uuid}"

    def _children_key(self, parent_uuid: str) -> str:
        return f"{self._prefix}children:{parent_uuid}"

    def init_task(
        self,
        task_uuid: str,
        *,
        name: str,
        kwargs: dict[str, Any],
        args: list[Any] | None = None,
        parent_id: str | None = None,
        state: str = "PENDING",
        snapshot_result: Any | None = None,
        snapshot_execution_log: Any | None = None,
        finish_event_status: str | None = None,
    ) -> None:
        now = utc_now_iso()
        doc: dict[str, Any] = {
            "uuid": task_uuid,
            "name": name,
            "state": state,
            "args": args if args is not None else [],
            "kwargs": kwargs,
            "received": now,
            "started": None,
            "failed": None,
            "retries": 0,
            "worker": None,
            "result": None,
            "exception": None,
            "traceback": None,
            "timestamp": now,
            "parent_id": parent_id,
        }
        if snapshot_result is not None:
            doc["result"] = snapshot_result
        if snapshot_execution_log is not None:
            doc["executionLog"] = snapshot_execution_log
        if finish_event_status is not None:
            doc["finishEventStatus"] = finish_event_status
        self._r.set(self._task_key(task_uuid), json.dumps(doc, default=str), ex=self._ttl)
        if parent_id:
            ck = self._children_key(parent_id)
            self._r.sadd(ck, task_uuid)
            self._r.expire(ck, self._ttl)

    def exists(self, task_uuid: str) -> bool:
        return bool(self._r.exists(self._task_key(task_uuid)))

    def get_raw(self, task_uuid: str) -> dict[str, Any] | None:
        raw = self._r.get(self._task_key(task_uuid))
        if raw is None:
            return None
        return json.loads(raw)

    def update_task(self, task_uuid: str, **fields: Any) -> None:
        doc = self.get_raw(task_uuid)
        if doc is None:
            doc = {"uuid": task_uuid, "args": [], "kwargs": {}, "received": utc_now_iso()}
        doc.update(fields)
        doc["timestamp"] = utc_now_iso()
        self._r.set(self._task_key(task_uuid), json.dumps(doc, default=str), ex=self._ttl)

    def list_child_ids(self, parent_uuid: str) -> list[str]:
        members = self._r.smembers(self._children_key(parent_uuid))
        return sorted(members) if members else []

    def get_children_views(self, parent_uuid: str) -> list[dict[str, Any]]:
        return [self.get_raw(cid) for cid in self.list_child_ids(parent_uuid) if self.get_raw(cid)]
