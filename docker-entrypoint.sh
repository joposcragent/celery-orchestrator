#!/bin/sh
set -e
export PYTHONPATH="${PYTHONPATH:-/app/src}"
API_PORT="${API_PORT:-8000}"
FLOWER_PORT="${FLOWER_PORT:-5555}"

uvicorn celery_orchestrator.main:app --host "${API_HOST:-0.0.0.0}" --port "${API_PORT}" &
UV_PID=$!

celery -A celery_orchestrator.celery_app worker --loglevel="${CELERY_LOG_LEVEL:-info}" &
WK_PID=$!

if [ "${CELERY_BEAT_ENABLED:-0}" = "1" ] || [ "${CELERY_BEAT_ENABLED:-false}" = "true" ]; then
  celery -A celery_orchestrator.celery_app beat --loglevel="${CELERY_LOG_LEVEL:-info}" &
  BT_PID=$!
fi

celery -A celery_orchestrator.celery_app flower --port="${FLOWER_PORT}" &
FL_PID=$!

_term() {
  kill "$UV_PID" "$WK_PID" "$FL_PID" 2>/dev/null || true
  if [ -n "${BT_PID:-}" ]; then
    kill "$BT_PID" 2>/dev/null || true
  fi
  wait || true
}
trap _term TERM INT

wait "$UV_PID"
EXIT=$?
_term
exit "$EXIT"
