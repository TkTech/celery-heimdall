import pytest

pytest_plugins = ('celery.contrib.pytest', )


@pytest.fixture(scope='session')
def celery_config():
    return {
        'broker_url': 'redis://',
        'result_backend': 'redis://',
        'worker_send_task_events': True
    }


@pytest.fixture(scope='session')
def celery_worker_parameters():
    return {'without_heartbeat': False}