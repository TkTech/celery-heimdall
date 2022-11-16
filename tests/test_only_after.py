import time

from celery import shared_task
from celery.result import AsyncResult

from celery_heimdall import HeimdallTask


@shared_task(base=HeimdallTask, bind=True)
def task_with_block(self: HeimdallTask):
    if self.only_after('only_after', 5):
        return True
    return False


def test_only_after(celery_session_worker):
    """
    Ensure that the blocks protected by `only_after()` only run after X
    seconds.
    """
    task1: AsyncResult = task_with_block.apply_async()
    assert task1.get() is True
    task2: AsyncResult = task_with_block.apply_async()
    assert task2.get() is False
    time.sleep(10)
    task3: AsyncResult = task_with_block.apply_async()
    assert task3.get() is True
