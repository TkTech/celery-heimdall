import threading
import time

import pytest
from celery import shared_task
from kombu.utils import uuid

from celery_heimdall import HeimdallTask, RateLimit
from celery_heimdall.task import rate_limited_countdown


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


@shared_task(base=HeimdallTask, heimdall={"times": 5, "per": 60})
def concurrent_rate_limit_task():
    pass


def _race_for_slots(task, key, workers):
    """
    Have `workers` threads ask for a slot under `key` at the same instant,
    and return the delay each of them was told to wait.
    """
    redis = task.heimdall_redis
    barrier = threading.Barrier(workers)
    delays = []
    errors = []

    def worker():
        try:
            # Make sure each thread has an open connection, so the only thing
            # left to do after the barrier is to ask for a slot.
            redis.ping()
            barrier.wait(timeout=30)
            delays.append(rate_limited_countdown(task, key, (), {}))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    return delays


def test_rate_limit_is_atomic(celery_session_app):
    """
    Ensure workers asking for a slot at the same instant are given exactly as
    many slots as the rate limit allows, never more.
    """
    task = concurrent_rate_limit_task
    times = task.heimdall["times"]
    redis = task.heimdall_redis

    for _ in range(5):
        key = f"h-rate:test-atomic-{uuid()}"
        try:
            delays = _race_for_slots(task, key, workers=40)
        finally:
            redis.delete(key, f"{key}.schedule")

        immediate = [d for d in delays if d == 0]
        assert len(delays) == 40
        assert len(immediate) == times, (
            f"{len(immediate)} of 40 simultaneous callers were allowed to run "
            f"immediately, but the rate limit is {times} per window."
        )


def test_rate_limit_slots_in_a_window(celery_session_app):
    """
    Ensure exactly `times` callers get a slot per window, and the rest are
    told to wait for at most the rest of the window.
    """
    task = concurrent_rate_limit_task
    times, per = task.heimdall["times"], task.heimdall["per"]
    redis = task.heimdall_redis
    key = f"h-rate:test-slots-{uuid()}"

    try:
        for _ in range(times):
            assert rate_limited_countdown(task, key, (), {}) == 0

        delay = rate_limited_countdown(task, key, (), {})
        assert 0 < delay <= per
    finally:
        redis.delete(key, f"{key}.schedule")
