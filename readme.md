# celery-orchestrator

Python-сервис: **FastAPI** (постановка задач и чтение снимков оркестрации), **Celery worker**, **Flower**, **Celery Beat** (ежечасный `collection-batch`). Брокер и хранилище снимков задач — **Redis**.

Спецификации: каталог `specifications/services/celery-orchestrator/` (OpenAPI, `rest.md`, `async.md`).

## Требования

- Python **3.12+**
- Redis 7+ (локально или в Docker)

## Переменные окружения

| Переменная | Назначение | По умолчанию |
|------------|------------|----------------|
| `REDIS_URL` | Брокер Celery + result backend + снимки задач | `redis://localhost:6379/0` |
| `SETTINGS_MANAGER_BASE_URL` | HTTP settings-manager | `http://localhost:8080` |
| `CRAWLER_BASE_URL` | HTTP crawler-headhunter | `http://localhost:3000` |
| `EVALUATOR_BASE_URL` | HTTP job-postings-evaluator | `http://localhost:8082` |
| `ORCH_REDIS_PREFIX` | Префикс ключей снимков в Redis | `orch:` |
| `CELERY_DEFAULT_QUEUE` | Очередь для `send_task` (совпадайте с `celery worker -Q …`) | `celery` |
| `ORCHESTRATION_FINISH_WAIT_TIMEOUT_SECONDS` | Сколько секунд ждать `task.finish` после RUNNING (`collection-query`, `evaluation`) | `604800` |
| `ORCHESTRATION_FINISH_POLL_INTERVAL_SECONDS` | Пауза между опросами Redis при ожидании finish | `0.25` |

**Celery и очередь:** отсутствие аргумента `queue` у сообщения не делает задачу «синхронной» — по умолчанию Celery всё равно шлёт в очередь `celery`. Сейчас очередь задаётся явно (`CELERY_DEFAULT_QUEUE` и `task_default_queue` в приложении), чтобы совпадать с воркером.

**Ожидание `task.finish`:** после успешного ответа crawler/evaluator воркер **блокируется** в опросе Redis, пока `task.finish` не сменит состояние с `RUNNING`. Пока функция задачи не вернулась, Celery/Flower не переводят её в SUCCESS. Нужен **другой свободный слот воркера** для выполнения `task.finish`; при `--concurrency 1` в одном процессе возможен взаимный **deadlock** — в compose для worker задано `--concurrency 6`.

**Два слоя состояния:** снимок оркестрации для `GET /tasks/{uuid}` хранится под префиксом `ORCH_REDIS_PREFIX` (отдельные ключи в Redis). Result backend Celery использует тот же `REDIS_URL`, но другие ключи. `POST /events-queue/finish` при постановке `task.finish` передаёт в брокер `parent_id` и `root_id` из тела `correlationId`; воркер `task.finish` определяет uuid родителя по **`request.parent_id`**, обновляет снимок родителя в оркестрации и вызывает `store_result` для того же uuid, чтобы итог в backend совпадал с оркестрацией. После снятия ожидания `collection-query` / `evaluation` завершают Celery-задачу с результатом или исключением в соответствии с финальным состоянием в снимке.

Для снимка finish в Redis обязателен **`status`** в теле запроса (в `finishEventStatus`). Поле **`result`** может отсутствовать (так шлёт job-postings-evaluator) — тогда у родителя в снимке остаётся `result: null`.

Порт HTTP API задаётся аргументом `uvicorn` (в `infra/docker-compose` — **8000** внутри контейнера, снаружи по умолчанию **8001**).

В OpenAPI путь задан как `/events-queue/{eventName}`; в реализации FastAPI параметр называется `event_name` (то же положение в URL, то же поведение).

## Локальная разработка

```bash
cd app/celery-orchestrator
python -m venv .venv
. .venv/Scripts/activate   # Windows
# source .venv/bin/activate  # Linux/macOS
pip install -e ".[dev]"
export REDIS_URL=redis://localhost:6379/0
```

Терминалы:

1. **Redis** (если нет локально): `docker run --rm -p 6379:6379 redis:7-alpine`
2. **API**: `python -m uvicorn celery_orchestrator.main:app --host 0.0.0.0 --port 8000`
3. **Worker**: `python -m celery -A celery_orchestrator.celery_app worker --loglevel=info`
4. **Flower**: `python -m celery -A celery_orchestrator.celery_app flower --port=5555`
5. **Beat**: `python -m celery -A celery_orchestrator.celery_app beat --loglevel=info`

## Тесты и покрытие

```bash
cd app/celery-orchestrator
pip install -e ".[dev]"
pytest
```

Порог покрытия **60%** задан в `pyproject.toml` (`--cov-fail-under=60`).

## Скрипт локальной сборки Docker-образа

Из каталога `app/celery-orchestrator` (нужны установленные **Docker** и **Python 3.12+**, как у сервиса; для чтения версии используется стандартный `tomllib`):

```bash
python scripts/build_image.py
```

Скрипт:

1. Читает версию из `[project].version` в `pyproject.toml`.
2. Выполняет `docker build` с тегом `joposcragent/celery-orchestrator:<версия>`.
3. Перевешивает тег `joposcragent/celery-orchestrator:latest` на только что собранный образ (`docker tag`).

Переопределение без правки файлов (опционально):

| Переменная окружения | Назначение |
|----------------------|------------|
| `DOCKER_IMAGE_NAME` | Имя образа без тега (по умолчанию `joposcragent/celery-orchestrator`) |
| `DOCKER_IMAGE_VERSION` | Тег версии вместо значения из `pyproject.toml` |

## Сборка и запуск Docker (один образ)

Из каталога `app/celery-orchestrator`:

```bash
python scripts/build_image.py
docker run --rm -e REDIS_URL=redis://host.docker.internal:6379/0 -p 8000:8000 joposcragent/celery-orchestrator:latest
```

Для worker / flower / beat переопределите `CMD` или используйте `docker compose` (см. ниже).

## Docker Compose (репозиторий `infra`)

Сборка образа с тегом, ожидаемым compose (версионный тег из `pyproject.toml` плюс `latest`):

```bash
cd app/celery-orchestrator
python scripts/build_image.py
```

Запуск стека оркестратора и Redis:

```bash
cd infra
docker compose up -d redis celery-orchestrator-api celery-orchestrator-worker celery-orchestrator-flower celery-orchestrator-beat
```

Порты по умолчанию см. в [`docker-compose.yaml`](../infra/docker-compose.yaml): `CELERY_ORCHESTRATOR_HTTP_PORT`, `CELERY_ORCHESTRATOR_FLOWER_PORT`, `REDIS_PORT`.
