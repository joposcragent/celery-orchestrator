"""Celery orchestrator package."""

from __future__ import annotations

import tomllib
from pathlib import Path

__all__ = ["__version__"]


def _read_version() -> str:
    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    with pyproject.open("rb") as fh:
        data = tomllib.load(fh)
    return str(data["project"]["version"])


__version__ = _read_version()
