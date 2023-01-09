# celery-heimdall

[![codecov](https://codecov.io/gh/TkTech/celery-heimdall/branch/main/graph/badge.svg?token=1A2CVHQ25Q)](https://codecov.io/gh/TkTech/celery-heimdall)
![GitHub](https://img.shields.io/github/license/tktech/celery-heimdall)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/celery-heimdall)

Celery Heimdall is a set of common utilities useful for the Celery background
worker framework, built on top of Redis. It's not trying to handle every use
case, but to be an easy, modern, and maintainable drop-in solution for 90% of
projects.

## Features

- Globally unique tasks, allowing only 1 copy of a task to execute at a time, or
  within a time period (ex: "Don't allow queuing until an hour has passed")
- Global rate limiting. Celery has built-in rate limiting, but it's a rate limit
  _per worker_, making it unsuitable for purposes such as limiting requests to
  an API.

## Installation

`pip install celery-heimdall`

## Usage

### Unique Tasks

Imagine you have a task that starts when a user presses a button. This task
takes a long time and a lot of resources to generate a report. You don't want
the user to press the button 10 times and start 10 tasks. In this case, you
want what Heimdall calls a unique task:

```python
from celery import shared_task
from celery_heimdall import HeimdallTask

@shared_task(base=HeimdallTask, heimdall={'unique': True})
def generate_report(customer_id):
    pass
```

All we've done here is change the base Task class that Celery will use to run
the task, and passed in some options for Heimdall to use. This task is now
unique - for the given arguments, only 1 will ever run at the same time.

#### Expiry

What happens if our task dies, or something goes wrong? We might end up in a
situation where our lock never gets cleared, called [deadlock][]. To work around
this, we add a maximum time before the task is allowed to be queued again:


```python
from celery import shared_task
from celery_heimdall import HeimdallTask

@shared_task(
  base=HeimdallTask,
  heimdall={
    'unique': True,
    'unique_timeout': 60 * 60
  }
)
def generate_report(customer_id):
  pass
```

Now, `generate_report` will be allowed to run again in an hour even if the
task got stuck, the worker ran out of memory, the machine burst into flames,
etc...

#### Custom Keys

By default, a hash of the task name and its arguments is used as the lock key.
But this often might not be what you want. What if you only want one report at
a time, even for different customers? Ex:

```python
from celery import shared_task
from celery_heimdall import HeimdallTask

@shared_task(
  base=HeimdallTask,
  heimdall={
    'unique': True,
    'key': lambda args, kwargs: 'generate_report'
  }
)
def generate_report(customer_id):
  pass
```
By specifying our own key function, we can completely customize how we determine
if a task is unique.

#### The Existing Task

By default, if you try to queue up a unique task that is already running,
Heimdall will return the existing task's `AsyncResult`. This lets you write
simple code that doesn't need to care if a task is unique or not. Imagine a
simple API endpoint that starts a report when it's hit, but we only want it
to run one at a time. The below is all you need:

```python
import time
from celery import shared_task
from celery_heimdall import HeimdallTask

@shared_task(base=HeimdallTask, heimdall={'unique': True})
def generate_report(customer_id):
  time.sleep(10)

def my_api_call(customer_id: int):
  return {
    'status': 'RUNNING',
    'task_id': generate_report.delay(customer_id).id
  }
```

Everytime `my_api_call` is called with the same `customer_id`, the same
`task_id` will be returned by `generate_report.delay()` until the original task
has completed.

Sometimes you'll want to catch that the task was already running when you tried
to queue it again. We can tell Heimdall to raise an exception in this case:

```python
import time
from celery import shared_task
from celery_heimdall import HeimdallTask, AlreadyQueuedError


@shared_task(
  base=HeimdallTask,
  heimdall={
    'unique': True,
    'unique_raises': True
  }
)
def generate_report(customer_id):
  time.sleep(10)


def my_api_call(customer_id: int):
  try:
    task = generate_report.delay(customer_id)
    return {'status': 'STARTED', 'task_id': task.id}
  except AlreadyQueuedError as exc:
    return {'status': 'ALREADY_RUNNING', 'task_id': exc.likely_culprit}
```

By setting `unique_raises` to `True` when we define our task, an
`AlreadyQueuedError` will be raised when you try to queue up a unique task
twice. The `AlreadyQueuedError` has two properties:

- `likely_culprit`, which contains the task ID of the already-running task,
- `expires_in`, which is the time remaining (in seconds) before the 
  already-running task is considered to be expired.

#### Unique Interval Task

What if we want the task to only run once in an hour, even if it's finished?
In those cases, we want it to run, but not clear the lock when it's finished:

```python
from celery import shared_task
from celery_heimdall import HeimdallTask

@shared_task(
  base=HeimdallTask,
  heimdall={
    'unique': True,
    'unique_timeout': 60 * 60,
    'unique_wait_for_expiry': True
  }
)
def generate_report(customer_id):
  pass
```

By setting `unique_wait_for_expiry` to `True`, the task will finish, and won't
allow another `generate_report()` to be queued until `unique_timeout` has
passed.

### Rate Limiting

Celery offers rate limiting out of the box. However, this rate limiting applies
on a per-worker basis. There's no reliable way to rate limit a task across all
your workers. Heimdall makes this easy:

```python
from celery import shared_task
from celery_heimdall import HeimdallTask, RateLimit

@shared_task(
  base=HeimdallTask,
  heimdall={
    'rate_limit': RateLimit((2, 60))
  }
)
def download_report_from_amazon(customer_id):
  pass
```

This says "every 60 seconds, only allow this task to run 2 times". If a task
can't be run because it would violate the rate limit, it'll be rescheduled.

It's important to note this does not guarantee that your task will run _exactly_
twice a second, just that it won't run _more_ than twice a second. Tasks are
rescheduled with a random jitter to prevent the [thundering herd][] problem.


#### Dynamic Rate Limiting

Just like you can dynamically provide a key for a task, you can also
dynamically provide a rate limit based off that key.


```python
from celery import shared_task
from celery_heimdall import HeimdallTask, RateLimit


@shared_task(
  base=HeimdallTask,
  heimdall={
    # Provide a lower rate limit for the customer with the ID 10, for everyone
    # else provide a higher rate limit.
    'rate_limit': RateLimit(lambda args: (1, 30) if args[0] == 10 else (2, 30)),
    'key': lambda args, kwargs: f'customer_{args[0]}'
  }
)
def download_report_from_amazon(customer_id):
  pass
```


## Inspirations

These are more mature projects which inspired this library, and which may
support older versions of Celery & Python then this project.

- [celery_once][], which is unfortunately abandoned and the reason this project
  exists.
- [celery_singleton][]
- [This snippet][snip] by Vigrond, and subsequent improvements by various
  contributors.


[celery_once]: https://github.com/cameronmaske/celery-once
[celery_singleton]: https://github.com/steinitzu/celery-singleton
[deadlock]: https://en.wikipedia.org/wiki/Deadlock
[thundering herd]: https://en.wikipedia.org/wiki/Thundering_herd_problem
[snip]: https://gist.github.com/Vigrond/2bbea9be6413415e5479998e79a1b11a