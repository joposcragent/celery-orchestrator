FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

RUN pip install --no-cache-dir --upgrade pip

COPY pyproject.toml readme.md /app/
COPY src /app/src

RUN pip install --no-cache-dir /app

# Default matches celery-orchestrator-api in compose (override entrypoint/command there)
CMD ["python", "-m", "uvicorn", "celery_orchestrator.main:app", "--host", "0.0.0.0", "--port", "8000"]
