class AlreadyQueuedError(Exception):
    """
    Raised when a task has already been enqueued and may not be added to the
    queue.
    """
