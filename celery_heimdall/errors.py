from typing import Optional


class AlreadyQueuedError(Exception):
    """
    Raised when a task has already been enqueued and may not be added to the
    queue.

    The exception may have the property `likely_culprit` set. If it is, this
    is the Celery Task ID of the task _most likely_ holding onto the lock.

    `likely_culprit` is here to assist in debugging deadlocks. Retrieving this
    value is not atomic, and thus should not be relied upon.
    """
    def __init__(self, *, expires_in: Optional[int] = None,
                 likely_culprit: Optional[str] = None):
        super().__init__()
        self.likely_culprit = likely_culprit
        self.expires_in = expires_in

    def __repr__(self):
        return (
            '<AlreadyQueuedError('
            f'likely_culprit={self.likely_culprit!r},'
            f' expires_in={self.expires_in!r})>'
        )
