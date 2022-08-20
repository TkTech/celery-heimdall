"""
Tests for unique tasks.
"""
import time

import celery
import pytest

from celery_heimdall import HeimdallTask, AlreadyQueuedError


@celery.shared_task(base=HeimdallTask, heimdall={'unique': True})
def default_unique_task(dummy_arg=None):
    time.sleep(4)


@celery.shared_task(
    base=HeimdallTask,
    heimdall={
        'unique': True,
        'key': lambda _, __: 'MyTaskKey'
    }
)
def explicit_key_task():
    time.sleep(2)


@celery.shared_task(
    base=HeimdallTask,
    bind=True,
    heimdall={
        'unique': True,
        'lock_prefix': 'new-prefix:'
    }
)
def task_with_override_config(task: HeimdallTask):
    time.sleep(2)
    return task.heimdall_config.lock_prefix


def test_default_unique(celery_session_worker):
    """
    Ensure a unique task with no other configuration "just works".
    """
    task1 = default_unique_task.apply_async()
    with pytest.raises(AlreadyQueuedError) as exc_info:
        default_unique_task.apply_async()

    # Ensure we populate the ID of the task most likely holding onto the lock
    # preventing us from running.
    assert exc_info.value.likely_culprit == task1.id
    # 60 * 60 is the default Heimdall task timeout.
    assert 0 < exc_info.value.expires_in <= 60 * 60
    assert task1.id in repr(exc_info.value)

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
    default_unique_task.delay('Task1')
    default_unique_task.delay('Task2')


def test_task_with_override_config(celery_session_worker):
    """
    Ensure we can override Config values from the `heimdall` task argument.
    """
    task1 = task_with_override_config.apply_async()
    with pytest.raises(AlreadyQueuedError):
        task_with_override_config.apply_async()

    assert task1.get() == 'new-prefix:'
