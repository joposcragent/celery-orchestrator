"""Celery Redis result backend with separate TTL for in-flight vs terminal task meta."""

from __future__ import annotations

from datetime import timedelta

from celery import states
from celery.backends.redis import RedisBackend


class TieredExpiryRedisBackend(RedisBackend):
    """TTL для метаданных задачи: до завершения — `result_expires`, после — `result_ready_expires`."""

    def _expires_for_ready(self) -> int:
        conf = self.app.conf
        raw = getattr(conf, "result_ready_expires", None)
        if raw is None:
            raw = timedelta(days=1)
        sec = self.prepare_expires(raw, int)
        return int(sec) if sec is not None else 86400

    def _expires_for_progress(self) -> int:
        sec = self.expires
        return int(sec) if sec is not None else 432000

    def _set_with_state(self, key, value, state):
        previous = self.expires
        try:
            self.expires = (
                self._expires_for_ready()
                if state in states.READY_STATES
                else self._expires_for_progress()
            )
            return self.set(key, value)
        finally:
            self.expires = previous
