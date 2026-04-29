"""TTL для Redis result backend (терминальные vs промежуточные состояния)."""

from datetime import timedelta

from celery import Celery, states

from celery_orchestrator.redis_result_backend import TieredExpiryRedisBackend


def test_tiered_backend_uses_shorter_ttl_for_ready_states() -> None:
    app = Celery("t", broker="memory://")
    app.conf.result_expires = timedelta(seconds=500)
    app.conf.result_ready_expires = timedelta(seconds=50)

    backend = TieredExpiryRedisBackend(app=app, url="redis://localhost:6379/0")
    observed: list[int] = []

    def capture_set(key, value, **kwargs):  # noqa: ARG001
        observed.append(int(backend.expires))

    backend.set = capture_set

    backend._set_with_state("k", "{}", states.STARTED)
    backend._set_with_state("k", "{}", states.SUCCESS)

    assert observed == [500, 50]
