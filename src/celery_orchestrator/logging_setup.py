"""Application logging for celery-orchestrator (stdlib, single namespace)."""

from __future__ import annotations

import logging
from typing import Final

from celery_orchestrator.config import get_settings

_LOG_NAMESPACE: Final = "celery_orchestrator"
_configured: bool = False
_FORMAT: Final = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def _parse_level(name: str) -> int:
    n = (name or "INFO").strip().upper()
    if hasattr(logging, n) and isinstance(getattr(logging, n), int):
        return int(getattr(logging, n))
    return logging.INFO


def configure_logging() -> None:
    """Idempotent: attach one StreamHandler to `celery_orchestrator` and set level from Settings."""
    global _configured
    if _configured:
        return
    s = get_settings()
    level = _parse_level(s.log_level)
    log = logging.getLogger(_LOG_NAMESPACE)
    log.setLevel(level)
    if not log.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter(_FORMAT))
        log.addHandler(h)
    log.propagate = False
    _configured = True
