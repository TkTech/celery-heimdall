import time

import celery.result
from celery import shared_task

from celery_heimdall import HeimdallTask


@shared_task(
    base=HeimdallTask,
    heimdall={
        'times': 2,
        'per': 10
    }
)
def default_rate_limit_task():
    pass


def test_default_rate_limit(celery_session_worker):
    """
    Ensure a unique task with no other configuration "just works".
    """
    start = time.time()
    # Immediate
    task1 = default_rate_limit_task.apply_async()
    # Immediate
    task2 = default_rate_limit_task.apply_async()
    # After at least 10 seconds
    task3 = default_rate_limit_task.apply_async()
    # After at least 10 seconds
    task4 = default_rate_limit_task.apply_async()
    # After at least 20 seconds
    task5 = default_rate_limit_task.apply_async()
    # After at least 20 seconds
    task6 = default_rate_limit_task.apply_async()

    task1.get()
    task2.get()

    elapsed = time.time() - start
    assert elapsed < 2

    task3.get()
    task4.get()

    elapsed = time.time() - start
    assert 10 < elapsed < 20

    task5.get()
    task6.get()

    elapsed = time.time() - start
    assert 20 < elapsed < 30
