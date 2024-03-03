"""
Tests for unique tasks.
"""

import time

import celery
import pytest
from celery.result import AsyncResult

from celery_heimdall import HeimdallTask, AlreadyQueuedError
from celery_heimdall.task import HeimdallConfig


@celery.shared_task(base=HeimdallTask, heimdall=HeimdallConfig(unique=True))
def default_unique_task(dummy_arg=None):
    time.sleep(4)


@celery.shared_task(
    base=HeimdallTask, heimdall=HeimdallConfig(unique=True, unique_raises=True)
)
def unique_raises_task():
    time.sleep(4)


@celery.shared_task(
    base=HeimdallTask, heimdall=HeimdallConfig(unique=True, key="MyTaskKey")
)
def explicit_key_task():
    time.sleep(2)


@celery.shared_task(
    base=HeimdallTask,
    heimdall=HeimdallConfig(unique=True, key=lambda args, kwargs: "MyTaskKey"),
)
def explicit_key_callable_task():
    time.sleep(2)


@celery.shared_task(
    base=HeimdallTask,
    bind=True,
    heimdall=HeimdallConfig(unique=True, lock_prefix="new-prefix"),
)
def task_with_override_config(task: HeimdallTask):
    time.sleep(2)
    return task.bifrost().config.get_lock_prefix()


@celery.shared_task(
    base=HeimdallTask,
    heimdall=HeimdallConfig(unique=True, unique_early=False, unique_late=True),
)
def task_with_late_lock():
    time.sleep(5)


def test_default_unique(celery_session_worker):
    """
    Ensure a unique task with no other configuration "just works".
    """
    task_1: AsyncResult = default_unique_task.apply_async()
    task_2: AsyncResult = default_unique_task.apply_async()
    assert task_1.id == task_2.id

    # Ensure the key gets erased after the task finishes, and we can queue
    # again.
    task_1.get()
    task_3: AsyncResult = default_unique_task.apply_async()
    assert task_3.id != task_1.id


def test_raises_unique(celery_session_worker):
    """
    Ensure a unique task raises an exception on conflicts.
    """
    task_1: AsyncResult = unique_raises_task.apply_async()
    with pytest.raises(AlreadyQueuedError) as exc_info:
        unique_raises_task.apply_async()

    # Ensure we populate the ID of the task most likely holding onto the lock
    # preventing us from running.
    assert exc_info.value.likely_culprit == task_1.id
    assert task_1.id in repr(exc_info.value)


def test_unique_explicit_key(celery_session_worker):
    """
    Ensure a unique task with an explicitly provided key works.
    """
    task_1: AsyncResult = explicit_key_task.apply_async()
    task_2: AsyncResult = explicit_key_task.apply_async()
    assert task_1.id == task_2.id

    # Ensure the key gets erased after the task finishes, and we can queue
    # again.
    task_1.get()
    task_3 = explicit_key_task.apply_async()
    assert task_3.id != task_1.id


def test_unique_explicit_callable_key(celery_session_worker):
    """
    Ensure a unique task with an explicitly provided key works.
    """
    task_1: AsyncResult = explicit_key_callable_task.apply_async()
    task_2: AsyncResult = explicit_key_callable_task.apply_async()
    assert task_1.id == task_2.id

    # Ensure the key gets erased after the task finishes, and we can queue
    # again.
    task_1.get()
    task_3 = explicit_key_callable_task.apply_async()
    assert task_3.id != task_1.id


def test_different_keys(celery_session_worker):
    """
    Ensure tasks enqueued with different args (and thus different auto keys)
    works as expected.
    """
    task_1 = default_unique_task.delay("Task1")
    task_2 = default_unique_task.delay("Task2")

    assert task_1.id != task_2.id


def test_task_with_override_config(celery_session_worker):
    """
    Ensure we can override Config values from the `heimdall` task argument.
    """
    task_1: AsyncResult = task_with_override_config.apply_async()
    task_2: AsyncResult = task_with_override_config.apply_async()

    assert task_1.id == task_2.id
    assert task_1.get() == "new-prefix"


def test_task_with_late_lock(celery_session_worker):
    """
    Ensure that tasks that only acquire their lock on call() work.

    This is hard to properly test with the pytest celery integration, since
    it only runs one task at a time.
    """
    task_1: AsyncResult = task_with_late_lock.apply_async()
    task_2: AsyncResult = task_with_late_lock.apply_async()

    assert task_1.id != task_2.id
