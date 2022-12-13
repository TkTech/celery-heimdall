import datetime
from pathlib import Path

from celery import Celery
from sqlalchemy import create_engine, insert, func, update
from sqlalchemy.dialects.sqlite import insert

from celery_heimdall.contrib.inspector import models


def task_received(event):
    with models.Session() as session:
        session.execute(
            insert(models.TaskInstance.__table__).values(
                uuid=event['uuid'],
                name=event['name'],
                status=models.TaskStatus.RECEIVED,
                hostname=event['hostname'],
                args=event['args'],
                kwargs=event['kwargs'],
                received=datetime.datetime.fromtimestamp(
                    event['timestamp']
                )
            )
        )
        session.commit()


def task_started(event):
    with models.Session() as session:
        session.execute(
            update(
                models.TaskInstance.__table__
            ).where(
                models.TaskInstance.uuid == event['uuid']
            ).values(
                runtime=event.get('runtime', 0),
                status=models.TaskStatus.STARTED,
                started=datetime.datetime.fromtimestamp(
                    event['timestamp']
                ),
                last_seen=func.now()
            )
        )
        session.commit()


def task_succeeded(event):
    with models.Session() as session:
        session.execute(
            update(
                models.TaskInstance.__table__
            ).where(
                models.TaskInstance.uuid == event['uuid']
            ).values(
                runtime=event.get('runtime', 0),
                status=models.TaskStatus.SUCCEEDED,
                succeeded=datetime.datetime.fromtimestamp(
                    event['timestamp']
                ),
                last_seen=func.now()
            )
        )
        session.commit()


def task_retried(event):
    with models.Session() as session:
        session.execute(
            update(
                models.TaskInstance.__table__
            ).where(
                models.TaskInstance.uuid == event['uuid']
            ).values(
                runtime=event.get('runtime', 0),
                status=models.TaskStatus.RETRIED,
                retries=models.TaskInstance.retries + 1,
                last_seen=func.now()
            )
        )
        session.commit()


def task_failed(event):
    with models.Session() as session:
        session.execute(
            update(
                models.TaskInstance.__table__
            ).where(
                models.TaskInstance.uuid == event['uuid']
            ).values(
                runtime=event.get('runtime', 0),
                status=models.TaskStatus.FAILED,
                failed=datetime.datetime.fromtimestamp(
                    event['timestamp']
                ),
                last_seen=func.now()
            )
        )
        session.commit()


def task_rejected(event):
    with models.Session() as session:
        session.execute(
            update(
                models.TaskInstance.__table__
            ).where(
                models.TaskInstance.uuid == event['uuid']
            ).values(
                runtime=event.get('runtime', 0),
                status=models.TaskStatus.REJECTED,
                rejected=datetime.datetime.fromtimestamp(
                    event['timestamp']
                ),
                last_seen=func.now()
            )
        )
        session.commit()


def worker_event(event):
    field_mapping = {
        'freq': models.Worker.frequency,
        'sw_ident': models.Worker.sw_identity,
        'sw_ver': models.Worker.sw_version,
        'sw_sys': models.Worker.sw_system,
        'active': models.Worker.active,
        'processed': models.Worker.processed
    }

    payload = {
        'last_seen': func.now(),
        'status': {
            'worker-heartbeat': models.WorkerStatus.ALIVE,
            'worker-online': models.WorkerStatus.ALIVE,
            'worker-offline': models.WorkerStatus.OFFLINE,
        }.get(event['type'], models.WorkerStatus.LOST)
    }
    for k, v in field_mapping.items():
        if k in event:
            payload[v] = event[k]

    # FIXME: Support postgres / MySQL
    with models.Session() as session:
        session.execute(
            insert(models.Worker.__table__).values({
                'id': event['hostname'],
                **payload
            }).on_conflict_do_update(
                index_elements=['id'],
                set_=payload
            )
        )
        session.commit()


def monitor(*, broker: str, db: Path):
    """
    A real-time Celery event monitor which captures events and populates a
    supported SQLAlchemy database.
    """
    app = Celery(broker=broker)

    engine = create_engine(f'sqlite:///{db}')
    models.Session.configure(bind=engine)
    models.Base.metadata.create_all(engine)

    with app.connection() as connection:
        recv = app.events.Receiver(
            connection,
            handlers={
                # '*': state.event,
                'task-started': task_started,
                'task-rejected': task_rejected,
                'task-failed': task_failed,
                'task-received': task_received,
                'task-succeeded': task_succeeded,
                'task-retried': task_retried,
                'worker-online': worker_event,
                'worker-heartbeat': worker_event,
                'worker-offline': worker_event
            }
        )
        recv.capture(limit=None, timeout=None, wakeup=True)
