from celery_orchestrator.view_builder import orchestration_task_view


def test_orchestration_task_view_minimal():
    doc = {
        "name": "task.x",
        "uuid": "u",
        "state": "PENDING",
        "args": [],
        "kwargs": {},
        "received": "t1",
        "started": None,
        "failed": None,
        "retries": 0,
        "worker": None,
        "result": None,
        "exception": None,
        "traceback": None,
        "timestamp": "t2",
    }
    v = orchestration_task_view(doc)
    assert v["name"] == "task.x"
    assert v["uuid"] == "u"
    assert v["state"] == "PENDING"


def test_orchestration_task_view_running_maps_to_started():
    doc = {
        "name": "task.collection-query",
        "uuid": "u",
        "state": "RUNNING",
        "args": [],
        "kwargs": {},
        "received": "t1",
        "started": "t0",
        "failed": None,
        "retries": 0,
        "worker": None,
        "result": None,
        "exception": None,
        "traceback": None,
        "timestamp": "t2",
    }
    v = orchestration_task_view(doc)
    assert v["state"] == "STARTED"
