from pathlib import Path

import click
from celery import Celery

from celery_heimdall.contrib.inspector.monitor import monitor


@click.group()
def cli():
    """
    heimdall-inspector provides tools for introspecting a live Celery cluster.
    """


@cli.command('monitor')
@click.argument('broker_url')
@click.option(
    '--enable-events',
    default=False,
    is_flag=True,
    help=(
        'Sends a command-and-control message to all Celery workers to start'
        ' emitting worker events before starting the server.'
    )
)
@click.option(
    '--db',
    default='heimdall.db',
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    help=(
        'Use the provided path to store our sqlite database.'
    )
)
def monitor_command(broker_url: str, enable_events: bool, db: Path):
    """
    Starts a monitor to watch for Celery events and records them to an SQLite
    database.

    Optionally enables event monitoring on a live cluster if --enable-events is
    provided. Note that it will not stop events when finished.
    """
    if enable_events:
        celery_app = Celery(broker=broker_url)
        celery_app.control.enable_events()

    monitor(broker=broker_url, db=db)


if __name__ == '__main__':
    cli()
