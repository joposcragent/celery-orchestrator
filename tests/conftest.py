from __future__ import annotations

from collections.abc import Generator
from typing import Any

import fakeredis
import pytest
from fastapi.testclient import TestClient

from celery_orchestrator.celery_app import celery_app as celery_module_app
from celery_orchestrator.main import app


@pytest.fixture
def captured_tasks(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, Any]]]:
    sent: list[tuple[str, dict[str, Any]]] = []

    def fake_send_task(
        name: str,
        args: tuple | None = None,
        kwargs: dict[str, Any] | None = None,
        **kw: Any,
    ) -> None:
        sent.append((name, dict(kwargs or {})))

    monkeypatch.setattr(celery_module_app, "send_task", fake_send_task)
    return sent


@pytest.fixture
def client(
    monkeypatch: pytest.MonkeyPatch,
    captured_tasks: list[tuple[str, dict[str, Any]]],
) -> Generator[TestClient, None, None]:
    fake = fakeredis.FakeRedis(decode_responses=True)

    def _from_url(_url: str, **kwargs: Any) -> fakeredis.FakeRedis:
        return fake

    monkeypatch.setattr("celery_orchestrator.main.Redis.from_url", _from_url)
    with TestClient(app) as c:
        yield c
