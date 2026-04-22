from __future__ import annotations

import json
import math
from typing import Any

import redis


def _cq_signal_key(prefix: str, collection_query_task_uuid: str) -> str:
    return f"{prefix.rstrip(':')}:signal:cq:{collection_query_task_uuid}"


def _ev_signal_key(prefix: str, evaluation_task_uuid: str) -> str:
    return f"{prefix.rstrip(':')}:signal:ev:{evaluation_task_uuid}"


def signal_collection_query_complete(
    client: redis.Redis,
    key_prefix: str,
    collection_query_task_uuid: str,
    payload: dict[str, Any] | None = None,
) -> None:
    body = json.dumps(payload or {}, default=str)
    client.lpush(_cq_signal_key(key_prefix, collection_query_task_uuid), body)


def wait_collection_query_complete(
    client: redis.Redis,
    key_prefix: str,
    collection_query_task_uuid: str,
    timeout_seconds: float,
) -> dict[str, Any] | None:
    key = _cq_signal_key(key_prefix, collection_query_task_uuid)
    total = int(math.ceil(timeout_seconds))
    remaining = total
    while remaining > 0:
        block = min(remaining, 86400)
        item = client.brpop(key, timeout=block)
        if item is not None:
            _, raw = item
            data = raw.decode() if isinstance(raw, bytes) else raw
            return json.loads(data) if data else {}
        remaining -= block
    return None


def signal_evaluation_complete(
    client: redis.Redis,
    key_prefix: str,
    evaluation_task_uuid: str,
    payload: dict[str, Any] | None = None,
) -> None:
    body = json.dumps(payload or {}, default=str)
    client.lpush(_ev_signal_key(key_prefix, evaluation_task_uuid), body)


def wait_evaluation_complete(
    client: redis.Redis,
    key_prefix: str,
    evaluation_task_uuid: str,
    timeout_seconds: float,
) -> dict[str, Any] | None:
    key = _ev_signal_key(key_prefix, evaluation_task_uuid)
    total = int(math.ceil(timeout_seconds))
    remaining = total
    while remaining > 0:
        block = min(remaining, 86400)
        item = client.brpop(key, timeout=block)
        if item is not None:
            _, raw = item
            data = raw.decode() if isinstance(raw, bytes) else raw
            return json.loads(data) if data else {}
        remaining -= block
    return None
