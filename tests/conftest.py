import os

import pytest

pytest_plugins = ("celery.contrib.pytest",)


@pytest.fixture(scope="session")
def celery_config():
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:16379")
    return {
        "broker_url": redis_url,
        "result_backend": redis_url,
        "worker_send_task_events": True,
    }


@pytest.fixture(scope="session")
def celery_worker_parameters():
    return {"without_heartbeat": False}
