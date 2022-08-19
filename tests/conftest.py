import pytest

pytest_plugins = ('celery.contrib.pytest', )


@pytest.fixture(scope='session')
def celery_config():
    return {
        'broker_url': 'redis://',
        'result_backend': 'redis://'
    }
