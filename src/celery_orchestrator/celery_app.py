from celery import Celery

from celery_orchestrator.config import get_settings

settings = get_settings()

celery_app = Celery(
    "celery_orchestrator",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["celery_orchestrator.tasks.definitions"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    broker_connection_retry_on_startup=True,
)

if settings.celery_beat_enabled:
    celery_app.conf.beat_schedule = {
        "hourly-collection-batch": {
            "task": "tasks.collection_batch",
            "schedule": 3600.0,
            "kwargs": {},
        },
    }
