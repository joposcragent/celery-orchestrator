from __future__ import annotations

import logging
from typing import Any

import httpx

from celery_orchestrator.config import Settings

logger = logging.getLogger(__name__)


def flatten_search_queries(payload: Any) -> list[str]:
    """Normalize settings-manager GET /search-query/list body (nested pages of items)."""
    if payload is None:
        return []
    if not isinstance(payload, list):
        return []
    out: list[str] = []
    for page in payload:
        if isinstance(page, list):
            for item in page:
                if isinstance(item, dict) and "query" in item:
                    q = item.get("query")
                    if isinstance(q, str) and q.strip():
                        out.append(q)
        elif isinstance(page, dict) and "query" in page:
            q = page.get("query")
            if isinstance(q, str) and q.strip():
                out.append(q)
    return out


def fetch_search_queries(settings: Settings) -> list[str]:
    url = settings.settings_manager_base_url.rstrip("/") + "/search-query/list"
    with httpx.Client(timeout=settings.http_timeout_seconds) as client:
        r = client.get(url)
        r.raise_for_status()
        data = r.json()
    return flatten_search_queries(data)


def start_crawler(settings: Settings, *, search_query: str, correlation_id: str) -> None:
    url = settings.crawler_base_url.rstrip("/") + "/crawler/start"
    body = {"list": [search_query], "correlationId": correlation_id}
    with httpx.Client(timeout=settings.http_timeout_seconds) as client:
        r = client.post(url, json=body)
        r.raise_for_status()


def start_evaluator_async(
    settings: Settings,
    *,
    job_posting_uuid: str,
    evaluation_task_uuid: str | None,
    parent_task_uuid: str | None,
) -> None:
    url = settings.evaluator_base_url.rstrip("/") + f"/evaluate/async/{job_posting_uuid}"
    opts: dict[str, Any] = {}
    if evaluation_task_uuid:
        opts["evaluationTaskUuid"] = evaluation_task_uuid
    if parent_task_uuid:
        opts["parentTaskUuid"] = parent_task_uuid
    body: dict[str, Any] = dict(opts) if opts else {}
    with httpx.Client(timeout=settings.http_timeout_seconds) as client:
        r = client.post(url, json=body)
        r.raise_for_status()
