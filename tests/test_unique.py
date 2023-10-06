"""
Tests for unique tasks.
"""
import celery
import pytest
from celery.result import AsyncResult

from celery_heimdall import HeimdallTask, AlreadyQueuedError
from celery_heimdall.task import release_lock, unique_key_for_task


@celery.shared_task(
    base=HeimdallTask,
    heimdall={"unique": True},
)
def default_unique_task(dummy_arg=None):
    return


@celery.shared_task(
    base=HeimdallTask,
    heimdall={
        "unique": True,
        "unique_wait_for_expiry": True,
    },
    name="wait_for_task",
    bind=True,
)
def wait_for_task(self, dummy_arg=None):
    return self.request.id


@celery.shared_task(
    base=HeimdallTask, heimdall={"unique": True, "unique_raises": True}
)
def unique_raises_task():
    return


@celery.shared_task(
    base=HeimdallTask,
    heimdall={"unique": True, "key": lambda _, __: "MyTaskKey"},
)
def explicit_key_task():
    return


@celery.shared_task(
    base=HeimdallTask,
    heimdall={"unique": True, "key": "MyTaskKeyStr"},
)
def explicit_key_task_str():
    return


@celery.shared_task(
    base=HeimdallTask,
    bind=True,
    heimdall={"unique": True, "lock_prefix": "new-prefix:"},
)
def task_with_override_config(task: HeimdallTask):
    return task.heimdall_config.lock_prefix


def test_default_unique(celery_session_worker):
    """
    Ensure a unique task with no other configuration "just works".
    """
    task1: AsyncResult = default_unique_task.apply_async()
    result: AsyncResult = default_unique_task.apply_async()
    assert result.id == task1.id

    # Ensure the key gets erased after the task finishes, and we can queue
    # again.
    task1.get()
    default_unique_task.apply_async()


def test_raises_unique(celery_session_worker):
    """
    Ensure a unique task raises an exception on conflicts.
    """
    task1: AsyncResult = unique_raises_task.apply_async()
    with pytest.raises(AlreadyQueuedError) as exc_info:
        result: AsyncResult = unique_raises_task.apply_async()

    # Ensure we populate the ID of the task most likely holding onto the lock
    # preventing us from running.
    assert exc_info.value.likely_culprit == task1.id
    # 60 * 60 is the default Heimdall task timeout.
    assert 0 < exc_info.value.expires_in <= 60 * 60
    assert task1.id in repr(exc_info.value)


def test_unique_explicit_key(celery_session_worker):
    """
    Ensure a unique task with an explicitly provided key works.
    """
    task1: AsyncResult = explicit_key_task.apply_async()
    result: AsyncResult = explicit_key_task.apply_async()
    assert task1.id == result.id

    # Ensure the key gets erased after the task finishes, and we can queue
    # again.
    task1.get()
    explicit_key_task.apply_async()

    # Ensure we can use a simple string instead of a lambda.
    task1: AsyncResult = explicit_key_task_str.apply_async()
    result: AsyncResult = explicit_key_task_str.apply_async()
    assert task1.id == result.id


def test_different_keys(celery_session_worker):
    """
    Ensure tasks enqueued with different args (and thus different auto keys)
    works as expected.
    """
    default_unique_task.delay("Task1")
    default_unique_task.delay("Task2")


def test_task_with_override_config(celery_session_worker):
    """
    Ensure we can override Config values from the `heimdall` task argument.
    """
    task1: AsyncResult = task_with_override_config.apply_async()
    result: AsyncResult = task_with_override_config.apply_async()

    assert task1.id == result.id
    assert task1.get() == "new-prefix:"


def test_send_task(celery_session_app, celery_session_worker):
    """
    Ensure that tasks triggered with send_task (like celery beat) will also
    be unique.
    """
    # This celery pytest plugin doesn't appear to run more than 1 worker at
    # a time, even when configured for higher concurrency and a prefork model,
    # so we use a unique task that doesn't clear its lock until the timeout to
    # test the 2nd task.

    # First we clear any locks that might be hanging around from a previous
    # test run.
    task = celery_session_app.tasks["wait_for_task"]
    release_lock(
        task,
        unique_key_for_task(
            task, (), {}, prefix=task.heimdall_config.lock_prefix
        ),
    )

    # Then we queue up the task, which will complete almost immediately but
    # leave a lock behind because of unique_wait_for_expiry.
    task1: AsyncResult = celery_session_app.send_task("wait_for_task")
    # Then we queue up a second task, bypassing `apply_async()`, which will
    # check the lock at runtime.
    task2: AsyncResult = celery_session_app.send_task("wait_for_task")

    assert task1.get() == task1.id
    assert task2.get() is None
