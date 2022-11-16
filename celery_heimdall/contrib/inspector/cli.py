from celery import Celery

try:
    import click
    import quart
except ImportError:
    print(
        'heimdall-inspector is not installed - install it with\n'
        '\tpip install celery-heimdall[inspector]'
    )
    raise SystemExit()


from celery_heimdall.contrib.inspector.monitor import monitor


@click.group()
def cli():
    pass


@cli.command('monitor')
@click.argument('broker')
@click.option(
    '--enable-events',
    default=False,
    is_flag=True,
    help=(
        'Sends a command-and-control message to all Celery workers to start'
        ' emitting worker events before starting the server.'
    )
)
def monitor_command(broker, enable_events):
    """
    Starts the monitor to record Celery events.
    """
    if enable_events:
        celery_app = Celery(broker=broker)
        celery_app.control.enable_events()

    monitor(broker=broker)


if __name__ == '__main__':
    cli()
