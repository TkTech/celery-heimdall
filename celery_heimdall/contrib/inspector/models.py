import enum

from sqlalchemy import (
    Column,
    TIMESTAMP,
    Integer,
    String,
    func,
    BigInteger,
    text,
    Enum
)
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()
Session = sessionmaker()


class WorkerStatus(enum.Enum):
    #: Worker is answering heartbeats.
    ALIVE = 0
    #: We didn't get an offline event, but we're not getting heartbeats.
    LOST = 10
    #: We got a shutdown event for the worker.
    OFFLINE = 20


class TaskStatus(enum.Enum):
    RECEIVED = 0
    STARTED = 10
    SUCCEEDED = 20
    FAILED = 30
    REJECTED = 40
    REVOKED = 50
    RETRIED = 60


class TaskInstance(Base):
    __tablename__ = 'task_instance'

    uuid = Column(String, primary_key=True)
    name = Column(String)
    status = Column(Enum(TaskStatus))
    hostname = Column(String)
    args = Column(String)
    kwargs = Column(String)

    runtime = Column(Integer)

    received = Column(TIMESTAMP, server_default=func.now())
    started = Column(TIMESTAMP, nullable=True)
    failed = Column(TIMESTAMP, nullable=True)
    rejected = Column(TIMESTAMP, nullable=True)
    succeeded = Column(TIMESTAMP, nullable=True)

    retries = Column(Integer, server_default=text('0'))
    last_seen = Column(TIMESTAMP, server_onupdate=func.now())


class Worker(Base):
    __tablename__ = 'worker'

    #: The hostname of a worker is used as its ID.
    id = Column(String, primary_key=True)

    #: How often the worker is configured to send heartbeats.
    frequency = Column(Integer, server_default=text('0'))
    #: Name of the worker software
    sw_identity = Column(String, nullable=True)
    #: Version of the worker software.
    sw_version = Column(String, nullable=True)
    #: Host operating system of the worker.
    sw_system = Column(String, nullable=True)

    #: Number of currently executing tasks.
    active = Column(BigInteger, server_default=text('0'))
    #: Number of processed tasks.
    processed = Column(BigInteger, server_default=text('0'))

    #: Last known status of the worker.
    status = Column(Enum(WorkerStatus))

    first_seen = Column(TIMESTAMP, server_default=func.now())
    last_seen = Column(TIMESTAMP, server_onupdate=func.now())
