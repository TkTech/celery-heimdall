"""
Based off the redis-py lock implementation, this module provides a lock
implementation better suited to our uses in Celery.

A stateful lock is pointless to us, as we almost exclusively lock on one
machine and release on another (when using early locks), and we require
better access to the `token` to know if our task is the one that should
hold the lock.

It has been further simplified to remove support for blocking locks, as
using a blocking lock in Celery is a recipe for disaster.
"""

from redis.client import StrictRedis

# KEYS[1] - lock name
# ARGV[1] - token
# return 1 if the lock was released, otherwise 0
LUA_RELEASE_SCRIPT = """
    local token = redis.call('get', KEYS[1])
    if not token or token ~= ARGV[1] then
        return 0
    end
    redis.call('del', KEYS[1])
    return 1
"""

# KEYS[1] - lock name
# ARGV[1] - token
# ARGV[2] - additional milliseconds
# ARGV[3] - "0" if the additional time should be added to the lock's
#           existing ttl or "1" if the existing ttl should be replaced
# return 1 if the locks time was extended, otherwise 0
LUA_EXTEND_SCRIPT = """
    local token = redis.call('get', KEYS[1])
    if not token or token ~= ARGV[1] then
        return 0
    end
    local expiration = redis.call('pttl', KEYS[1])
    if not expiration then
        expiration = 0
    end
    if expiration < 0 then
        return 0
    end

    local newttl = ARGV[2]
    if ARGV[3] == "0" then
        newttl = ARGV[2] + expiration
    end
    redis.call('pexpire', KEYS[1], newttl)
    return 1
"""

# KEYS[1] - lock name
# ARGV[1] - token
# ARGV[2] - milliseconds
# return 1 if the locks time was reacquired, otherwise 0
LUA_REACQUIRE_SCRIPT = """
    local token = redis.call('get', KEYS[1])
    if not token or token ~= ARGV[1] then
        return 0
    end
    redis.call('pexpire', KEYS[1], ARGV[2])
    return 1
"""

RELEASE = None
EXTEND = None
REACQUIRE = None


def _do_release(client: StrictRedis, key: bytes, token: bytes):
    """
    Releases the lock with the given key and token.
    """
    global RELEASE
    if RELEASE is None:
        RELEASE = client.register_script(LUA_RELEASE_SCRIPT)
    return RELEASE(keys=[key], args=[token])


def _do_extend(
    client: StrictRedis,
    key: bytes,
    token: bytes,
    milliseconds: int,
    replace: bool,
):
    """
    Extends the lock with the given key and token by the given number of
    milliseconds.
    """
    global EXTEND
    if EXTEND is None:
        EXTEND = client.register_script(LUA_EXTEND_SCRIPT)
    return EXTEND(
        keys=[key], args=[token, milliseconds, "1" if replace else "0"]
    )


def _do_reacquire(
    client: StrictRedis, key: bytes, token: bytes, milliseconds: int
):
    """
    Reacquires the lock with the given key and token, extending it by the
    given number of milliseconds.
    """
    global REACQUIRE
    if REACQUIRE is None:
        REACQUIRE = client.register_script(LUA_REACQUIRE_SCRIPT)
    return REACQUIRE(keys=[key], args=[token, milliseconds])


def lock(
    client: StrictRedis, key: bytes, token: bytes, *, expiry: int
) -> bytes:
    """
    Acquires a lock with the given key and token, and an expiry in
    milliseconds.

    If the lock is already held, the token of the current holder is
    returned, otherwise the token that was passed in is returned.
    """
    old = client.set(key, token, nx=True, px=expiry, get=True)
    return token if old is None else old


def release(client: StrictRedis, key: bytes, token: bytes) -> bool:
    """
    Releases the lock with the given key and token.

    If the lock is not held, this function returns False, otherwise it
    returns True.

    If the token is not the current holder of the lock, this function
    returns False.
    """
    return bool(_do_release(client, key, token))


def extend(
    client: StrictRedis,
    key: bytes,
    token: bytes,
    milliseconds: int,
    replace: bool,
) -> bool:
    """
    Extends the lock with the given key and token by the given number of
    milliseconds.

    If the lock is not held, this function returns False, otherwise it
    returns True.

    If the token is not the current holder of the lock, this function
    returns False.
    """
    return bool(_do_extend(client, key, token, milliseconds, replace))
