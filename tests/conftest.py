import pytest
import fakeredis
from unittest.mock import MagicMock

from celery_orchestrator.celery_app import app
from celery_orchestrator.config import get_settings


@pytest.fixture(autouse=True)
def _clear_settings_cache() -> None:
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def fake_redis(monkeypatch: pytest.MonkeyPatch) -> fakeredis.FakeStrictRedis:
    r = fakeredis.FakeStrictRedis(decode_responses=True)

    def _client(settings=None):  # noqa: ARG001
        return r

    monkeypatch.setattr("celery_orchestrator.storage.redis_store.get_redis_client", _client)
    monkeypatch.setattr("celery_orchestrator.api.routes.get_redis_client", _client)
    monkeypatch.setattr("celery_orchestrator.tasks.definitions.get_redis_client", _client)
    monkeypatch.setattr("celery_orchestrator.orchestration_wait.get_redis_client", _client)
    # Orchestration Redis is faked; avoid hitting real Redis for Celery result backend in task tests.
    monkeypatch.setattr(app.backend, "store_result", MagicMock(return_value=None))
    return r
