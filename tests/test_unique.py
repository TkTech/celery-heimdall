"""
Tests for unique tasks.
"""
import time

import celery
import pytest

from celery_heimdall import HeimdallTask, AlreadyQueuedError


@celery.shared_task(base=HeimdallTask, heimdall={'unique': True})
def default_unique_task():
    time.sleep(2)


@celery.shared_task(
    base=HeimdallTask,
    heimdall={
        'unique': True,
        'key': lambda _, __: 'MyTaskKey'
    }
)
def explicit_key_task():
    time.sleep(2)


@celery.shared_task(base=HeimdallTask, heimdall={'unique': True})
def auto_key_task(dummy_arg):
    time.sleep(2)


def test_default_unique(celery_session_worker):
    """
    Ensure a unique task with no other configuration "just works".
    """
    task1 = default_unique_task.apply_async()
    with pytest.raises(AlreadyQueuedError):
        default_unique_task.apply_async()

    # Ensure the key gets erased after the task finishes, and we can queue
    # again.
    task1.get()
    default_unique_task.apply_async()


def test_unique_explicit_key(celery_session_worker):
    """
    Ensure a unique task with an explicitly provided key works.
    """
    task1 = explicit_key_task.apply_async()
    with pytest.raises(AlreadyQueuedError):
        explicit_key_task.apply_async()

    # Ensure the key gets erased after the task finishes, and we can queue
    # again.
    task1.get()
    explicit_key_task.apply_async()


def test_different_keys(celery_session_worker):
    """
    Ensure tasks enqueued with different args (and thus different auto keys)
    works as expected.
    """
    auto_key_task.delay('Task1')
    auto_key_task.delay('Task2')
