import hashlib
import random
import datetime
from abc import ABC

import redis
import redis.lock
import celery
from kombu import serialization
from kombu.utils import uuid

from celery_heimdall.config import Config
from celery_heimdall.errors import AlreadyQueuedError


def acquire_lock(task: 'HeimdallTask', key: str, timeout: int, *, task_id: str):
    acquired = redis.lock.Lock(
        task.heimdall_redis,
        key,
        timeout=timeout,
        blocking=task.heimdall_config.unique_lock_blocking,
        blocking_timeout=task.heimdall_config.unique_lock_timeout,
    ).acquire(token=task_id)

    if not acquired:
        pipe = task.heimdall_redis.pipeline()
        pipe.get(key)
        pipe.ttl(key)
        task_id, ttl = pipe.execute()

        raise AlreadyQueuedError(
            # TTL may be -1 or -2 if the key didn't exist, depending on the
            # version of Redis.
            expires_in=max(0, ttl),
            likely_culprit=task_id.decode('utf-8') if task_id else None
        )

    return acquired


def release_lock(task: 'HeimdallTask', key: str):
    task.heimdall_redis.delete(key)


def unique_key_for_task(task: 'HeimdallTask', args, kwargs, *,
                        prefix='') -> str:
    """
    Given a task and its arguments, generate a unique key which can be used
    to identify it.
    """
    h = getattr(task, 'heimdall', {})

    # When Celery deserializes the arguments for a job, args and kwargs will
    # be `[]` or `{}`, even if they were `None` when serialized. Ensure we
    # do the same here or the hashes will never match when arguments are empty.
    args = args or []
    kwargs = kwargs or {}

    # User specified an explicit key function.
    if 'key' in h:
        return prefix + h['key'](args, kwargs)

    # Try to generate a unique key from the arguments given to the task.
    # Most of the cases where this will fail are also cases where Celery
    # will be unable to serialize the job, so we're not too concerned with
    # validation.
    _, _, data = serialization.dumps(
        (args, kwargs),
        # TODO: We should _probably_ use the same serializer as the task.
        'json'
    )

    h = hashlib.md5()
    h.update(task.name.encode('utf-8'))
    h.update(data.encode('utf-8'))
    return f'{prefix}{h.hexdigest()}'


def rate_limited_countdown(task: 'HeimdallTask', key):
    # Based on improvements to Vigrond's original implementation by mlissner
    # on stack overflow.
    h = getattr(task, 'heimdall', {})
    r = task.heimdall_redis

    times, per = h['times'], h['per']

    number_of_running_tasks = r.get(key)
    if number_of_running_tasks is None:
        r.set(key, 1, ex=per)
        return 0

    if int(number_of_running_tasks) < times:
        if r.incr(key, 1) == 1:
            r.expire(key, per)
        return 0

    schedule_key = f'{key}.schedule'
    now = datetime.datetime.now(datetime.timezone.utc)

    delay = r.get(schedule_key)
    if delay is None or int(delay) < now.timestamp():
        # Either not scheduled, or scheduled in the past.
        ttl = r.ttl(key)
        if ttl < 0:
            return 0

        r.set(
            f'{key}.schedule',
            int((now + datetime.timedelta(seconds=ttl)).timestamp())
        )
        return ttl

    new_time = (
        datetime.datetime.fromtimestamp(
            int(delay),
            datetime.timezone.utc
        ) + datetime.timedelta(seconds=per // times)
    )
    r.set(f'{key}.schedule)', int(new_time.timestamp()))
    return int((new_time - now).total_seconds())


class HeimdallTask(celery.Task, ABC):
    """
    An all-seeing base task for Celery, it provides useful global utilities
    for common Celery behaviors, such as global rate limiting and singleton
    (only one at a time) tasks.
    """
    abstract = True

    def __init__(self):
        super().__init__()
        self._heimdall_config = None
        self._heimdall_redis = None

    @property
    def heimdall_config(self) -> Config:
        if not self._heimdall_config:
            self._heimdall_config = Config(self.app, task=self)
        return self._heimdall_config

    @property
    def heimdall_redis(self) -> redis.Redis:
        if not self._heimdall_redis:
            self._heimdall_redis = self.setup_redis()
        return self._heimdall_redis

    def setup_redis(self) -> redis.Redis:
        """
        Sets up the Redis connection. By default, it'll use any Redis instance
        it can find (in order):

            - the Celery result backend
            - the Celery broker

        If nothing can be found, or if you want to explicitly specify a Redis
        connection you'll need to implement this method yourself, ex:

        .. code::

            from redis import Redis
            from celery_heimdall import HeimdallTask

            class MyHeimdallTask(HeimdallTask):
                def setup_redis(self):
                    return Redis.from_url('redis://')
        """
        # Try to use the Celery result backend, if it's configured for redis.
        backend = self.app.conf.get('result_backend') or ''
        if backend.startswith('redis://'):
            return redis.Redis.from_url(backend)

        # If not the backend, try the broker....
        broker = self.app.conf.get('broker_url') or ''
        if broker.startswith('redis://'):
            return redis.Redis.from_url(broker)

        # Nope, we can't find a usable redis, user will need to implement
        # setup_redis() themselves.
        raise NotImplementedError()

    def apply_async(self, args=None, kwargs=None, task_id=None, **options):
        h = getattr(self, 'heimdall', {})
        if h and 'unique' in h:
            task_id = task_id or uuid()

            # Task has been configured to be globally unique, so we check for
            # the presence of a global lock before allowing it to be queued.
            acquire_lock(
                self,
                unique_key_for_task(
                    self,
                    args,
                    kwargs,
                    prefix=self.heimdall_config.lock_prefix
                ),
                h.get(
                    'unique_timeout',
                    self.heimdall_config.unique_timeout
                ),
                task_id=task_id
            )

        # TODO: If we kept track of queued, but not running, tasks, we should
        #       be able to estimate _when_ it would be okay to run a
        #       rate-limited task, rather then just checking when it runs.

        return super().apply_async(
            args=args,
            kwargs=kwargs,
            task_id=task_id,
            **options
        )

    def __call__(self, *args, **kwargs):
        h = getattr(self, 'heimdall', {})
        if h and 'per' in h and 'times' in h:
            delay = rate_limited_countdown(
                self,
                unique_key_for_task(
                    self,
                    args,
                    kwargs,
                    prefix=self.heimdall_config.rate_limit_prefix
                )
            )
            if delay > 0:
                # We don't want our rescheduling retry to count against
                # any normal retry limits the user might have set on the
                # task or globally.
                self.request.retries -= 1
                raise self.retry(countdown=delay)

        return self.run(*args, **kwargs)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        # Handles post-task cleanup, when a task exits cleanly. This will be
        # called if a task raises an exception (stored in `einfo`), but not
        # if a worker straight up dies (say, because of running out of memory)
        h = getattr(self, 'heimdall', {})

        # Cleanup the unique task lock when the task finishes, unless the user
        # told us to wait for the remaining interval.
        if h and 'unique' in h and not h.get('unique_wait_for_expiry'):
            release_lock(
                self,
                unique_key_for_task(
                    self,
                    args,
                    kwargs,
                    prefix=self.heimdall_config.lock_prefix
                )
            )

        super().after_return(status, retval, task_id, args, kwargs, einfo)
