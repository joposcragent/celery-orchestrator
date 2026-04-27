#!/usr/bin/env python
"""Build Docker image joposcragent/celery-orchestrator:<version> and tag :latest."""

from __future__ import annotations

import os
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _project_version() -> str:
    with (ROOT / "pyproject.toml").open("rb") as f:
        data = tomllib.load(f)
    return str(data["project"]["version"])


def main() -> int:
    image_name = os.environ.get("DOCKER_IMAGE_NAME", "joposcragent/celery-orchestrator")
    image_version = os.environ.get("DOCKER_IMAGE_VERSION", _project_version())
    ref = f"{image_name}:{image_version}"
    latest_ref = f"{image_name}:latest"

    print(f"[build:image] docker build -t {ref} .")
    r1 = subprocess.run(
        ["docker", "build", "-t", ref, "."],
        cwd=ROOT,
        check=False,
    )
    if r1.returncode != 0:
        return r1.returncode

    print(f"[build:image] docker tag {ref} {latest_ref}")
    r2 = subprocess.run(
        ["docker", "tag", ref, latest_ref],
        cwd=ROOT,
        check=False,
    )
    return r2.returncode if r2.returncode != 0 else 0


if __name__ == "__main__":
    sys.exit(main())
