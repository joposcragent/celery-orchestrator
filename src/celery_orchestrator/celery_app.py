from __future__ import annotations

from celery import Celery
from celery.schedules import crontab

from celery_orchestrator.config import get_settings
from celery_orchestrator.logging_setup import configure_logging

settings = get_settings()
configure_logging()

app = Celery("celery_orchestrator")
app.conf.broker_url = settings.redis_url
app.conf.result_backend = settings.redis_url
app.conf.task_default_queue = settings.celery_default_queue
app.conf.task_create_missing_queues = True
app.conf.task_track_started = True
app.conf.task_serializer = "json"
app.conf.result_serializer = "json"
app.conf.accept_content = ["json"]
app.conf.timezone = "UTC"
app.conf.beat_schedule = {
    "hourly-collection-batch": {
        "task": "task.beat-hourly-collection-batch",
        "schedule": crontab(minute=0),
    },
}

# Eager imports so worker registers tasks
import celery_orchestrator.tasks.definitions  # noqa: E402, F401
