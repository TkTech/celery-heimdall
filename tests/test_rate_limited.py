import time

import pytest
from celery import shared_task

from celery_heimdall import HeimdallTask, RateLimit


@shared_task(base=HeimdallTask, heimdall={"times": 2, "per": 10})
def default_rate_limit_task():
    pass


@shared_task(base=HeimdallTask, heimdall={"rate_limit": RateLimit((2, 10))})
def tuple_rate_limit_task():
    pass


@shared_task(
    base=HeimdallTask, heimdall={"rate_limit": RateLimit(lambda key: (2, 10))}
)
def callable_rate_limit_task():
    pass


@pytest.mark.parametrize(
    "func",
    [default_rate_limit_task, tuple_rate_limit_task, callable_rate_limit_task],
)
def test_default_rate_limit(celery_session_worker, func):
    """
    Ensure a unique task with no other configuration "just works".
    """
    start = time.time()
    # Immediate
    task1 = func.apply_async()
    # Immediate
    task2 = func.apply_async()
    # After at least 10 seconds
    task3 = func.apply_async()
    # After at least 10 seconds
    task4 = func.apply_async()
    # After at least 20 seconds
    task5 = func.apply_async()
    # After at least 20 seconds
    task6 = func.apply_async()

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
