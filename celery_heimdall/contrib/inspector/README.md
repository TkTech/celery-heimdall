# Inspector

**Note:** This tool is in beta, and currently only tested against SQLite as a
data store.

The Inspector is a minimal debugging tool for working with Celery queues and
tasks. It is an optional component of celery-heimdall and not installed by
default.

It runs a monitor, which populates any SQLAlchemy-compatible database with the
state of your Celery cluster.

## Why?

This tool is used to assist in debugging, generate graphs of queues for
documentation, to verify the final state of Celery after tests, etc...

Flower deprecated their graphs page, and now require you to use prometheus and
grafana, which is overkill when you just want to see what's been running.

## Installation

```
pip install celery-heimdall[inspector]
```