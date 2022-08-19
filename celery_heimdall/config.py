from celery import Celery
from celery.app import app_or_default


class Config:
    def __init__(self, app: Celery, *, task=None):
        self.app = app_or_default(app)
        self.task = task

    def _from_task_or_app(self, key, default):
        key = f'heimdall_{key}'

        if self.task:
            v = getattr(self.task, 'heimdall', {}).get(key)
            if v is not None:
                return v

        return self.app.conf.get(key, default)

    @property
    def unique_lock_timeout(self):
        return self._from_task_or_app('unique_lock_timeout', 1)

    @property
    def unique_timeout(self):
        return self._from_task_or_app('unique_timeout', 60 * 60)

    @property
    def lock_prefix(self):
        return self._from_task_or_app('lock_prefix', 'h-lock:')

    @property
    def rate_limit_prefix(self):
        return self._from_task_or_app('rate_limit_prefix', 'h-rate:')
