"""Pool module: contains `CypherGraphDBPool` implementation separated from cyphergraphdb.

Provides a lightweight, thread-safe pool with optional TTL for `CypherGraphDB` instances.
"""

from __future__ import annotations

import contextlib
import threading
import time
from collections import deque
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from loguru import logger  # noqa: F401

from .backend import CypherBackend
from .backendprovider import backend_provider

if TYPE_CHECKING:  # pragma: no cover
    from .cyphergraphdb import CypherGraphDB


@dataclass(slots=True)
class PoolStats:
    created: int = 0  # total created db instances
    acquired: int = 0  # total successful get()
    released: int = 0  # total releases
    expired: int = 0  # expired idle instances purged
    hard_exhausted: int = 0  # times pool was exhausted (non-blocking)
    blocked_acquires: int = 0  # times a blocking wait occurred

    def snapshot(self) -> PoolStats:  # return shallow copy
        return PoolStats(
            created=self.created,
            acquired=self.acquired,
            released=self.released,
            expired=self.expired,
            hard_exhausted=self.hard_exhausted,
            blocked_acquires=self.blocked_acquires,
        )


class CypherGraphDBPool:
    """Lightweight pool for `CypherGraphDB` instances.

    Features:
    - Maximum size (`pool_size`).
    - Optional TTL (seconds) for idle instances (expired ones are closed).
    - Thread-safe acquisition / release via internal lock.
    - Context manager support to auto-close pool.

    Notes:
    Uses only stdlib primitives (`deque`, `Lock`, `time.monotonic`).
        TTL support is minimal: idle resources are purged opportunistically
        (on acquisition) and timestamped on release; no background thread.
        A private factory `_create()` centralizes construction & optional
        auto-connect so warm-up and runtime paths stay identical.
        Blocking acquisition re-computes remaining timeout to honor the
        total requested wait duration.
    """

    def __init__(
        self,
        backend: CypherBackend | str,
        connect_params: dict | None = None,
        pool_size: int = 5,
        min_size: int = 0,
        ttl: float | None = None,
        auto_connect: bool = True,
    ):
        """Initialize a new pool.

        Args:
            backend: Backend id or instance registered in `backend_provider`.
            connect_params: Keyword arguments passed to `db.connect()` if
                `auto_connect` is True.
            pool_size: Maximum simultaneously in-use connections.
            min_size: Warm start number of idle connections to pre-create.
            ttl: Idle lifetime in seconds; expired connections are discarded
                lazily. `None` disables expiration.
            auto_connect: If True, new instances are connected automatically.
        Raises:
            ValueError: If `min_size` > `pool_size`.
        """
        self._backend = backend_provider.check_and_resolve(backend, True)
        assert self._backend

        self._connect_params = connect_params
        self._pool_size = max(1, int(pool_size))
        self._min_size = max(0, int(min_size))
        if self._min_size > self._pool_size:
            raise ValueError("min_size cannot exceed pool_size")
        self._ttl = ttl  # seconds, None means infinite
        self._auto_connect = auto_connect

        # Idle entries store (expiry_ts, db_instance) if ttl enabled
        # else (last_used_ts, db)
        self._idle = deque()  # list[tuple[float, CypherGraphDB]]
        self._in_use: set[CypherGraphDB] = set()
        self._lock = threading.RLock()
        self._not_empty = threading.Condition(self._lock)
        self._closed = False
        self._clock = time.monotonic
        self._stats = PoolStats()

        # Pre-create min_size idle connections (warm pool) if requested.
        if self._min_size:
            logger.debug(
                "Warm start backend={} min={} ttl={} auto_connect={} size={}",
                getattr(self._backend, "name", self._backend),
                self._min_size,
                self._ttl,
                self._auto_connect,
                self._pool_size,
            )
            for idx in range(self._min_size):
                db = self._create()
                expiry = self._expiry()
                self._idle.append((expiry, db))
                logger.trace(
                    "Warm conn idx={} id={} expiry={} (0=no-ttl)",
                    idx,
                    id(db),
                    expiry,
                )
        else:
            logger.debug(
                "Pool init backend={} pool_size={} ttl={} auto_connect={}",
                getattr(self._backend, "name", self._backend),
                self._pool_size,
                self._ttl,
                self._auto_connect,
            )

    def prune(self) -> int:
        """Remove expired idle connections.

        Returns:
            int: Number of expired connections purged during this call.
        """
        with self._lock:
            before = self._stats.expired
            self._purge_expired()
            return self._stats.expired - before

    def __len__(self) -> int:  # number of total tracked (idle + in-use)
        """Return total number of pooled instances (idle + in-use)."""
        with self._lock:
            return len(self._in_use) + len(self._idle)

    @property
    def stats(self) -> PoolStats:
        """Snapshot of pool statistics (counters since creation)."""
        return self._stats.snapshot()

    @property
    def closed(self) -> bool:
        """Whether the pool has been closed (no further acquisition)."""
        return self._closed

    def get(
        self,
        block: bool = False,
        timeout: float | None = None,
    ) -> CypherGraphDB:
        """Acquire a connection from the pool.

        Args:
            block: If True, block until a connection becomes available or the
                timeout elapses.
            timeout: Maximum seconds to wait in blocking mode; `None` means
                wait indefinitely.
        Returns:
            CypherGraphDB: A pooled database instance.
        Raises:
            RuntimeError: If the pool is closed, exhausted (non-blocking), or
                the timeout expires while waiting.
        """
        with self._lock:
            if self._closed:
                raise RuntimeError("Pool is closed")
            deadline = None if timeout is None else self._clock() + timeout
            logger.trace(
                "Acquire start blk={} to={} in_use={} idle={} created={}",
                block,
                timeout,
                len(self._in_use),
                len(self._idle),
                self._stats.created,
            )
            while True:
                db = self._next_idle()
                if db is not None:
                    self._in_use.add(db)
                    self._stats.acquired += 1
                    logger.trace(
                        "Reuse id={} in_use={} idle={} acquired_total={}",
                        id(db),
                        len(self._in_use),
                        len(self._idle),
                        self._stats.acquired,
                    )
                    return db
                if len(self._in_use) < self._pool_size:
                    db = self._create()
                    self._in_use.add(db)
                    self._stats.acquired += 1
                    logger.trace(
                        "Create id={} in_use={} idle={} acquired_total={}",
                        id(db),
                        len(self._in_use),
                        len(self._idle),
                        self._stats.acquired,
                    )
                    return db
                if not block:
                    self._stats.hard_exhausted += 1
                    logger.debug(
                        "Acquire exhausted in_use={} pool_size={} created={}",
                        len(self._in_use),
                        self._pool_size,
                        self._stats.created,
                    )
                    raise RuntimeError("Pool exhausted (all instances in use)")
                self._stats.blocked_acquires += 1
                remaining = None
                if deadline is not None:
                    remaining = deadline - self._clock()
                    if remaining <= 0:
                        logger.debug("Acquire timeout pre-wait remaining<=0")
                        raise RuntimeError("Timeout waiting for connection from pool")
                waited = self._not_empty.wait(remaining)
                logger.trace(
                    "Wait wake waited={} remaining={} in_use={} idle={}",
                    waited,
                    remaining,
                    len(self._in_use),
                    len(self._idle),
                )
                if not waited:
                    logger.debug("Acquire timeout post-wait")
                    raise RuntimeError("Timeout waiting for connection from pool")

    def release(self, db: CypherGraphDB):
        """Return a previously acquired connection to the pool.

        Silently ignores connections not tracked by this pool. If the pool
        has been closed the connection is disconnected instead of re-queued.
        """
        if db is None:
            return
        with self._lock:
            if db not in self._in_use:
                logger.trace("Release ignore id={} not in use", id(db))
                return
            self._in_use.remove(db)
            if self._closed:
                with contextlib.suppress(Exception):
                    db.disconnect()
                logger.trace("Release disconnect (closed) id={}", id(db))
                return
            # compute and append expiry (0.0 sentinel if no TTL)
            self._idle.append((self._expiry(), db))
            self._stats.released += 1
            logger.trace(
                "Release id={} in_use={} idle={} released_total={}",
                id(db),
                len(self._in_use),
                len(self._idle),
                self._stats.released,
            )
            # notify one waiter if any
            self._not_empty.notify()

    def close(self):
        """Close the pool.

        Idempotent. All idle connections are disconnected. Threads blocked in
        `get(block=True)` are awakened and will receive a RuntimeError on
        their next iteration. In-use connections are NOT forcibly closed; the
        caller should release them which will then disconnect immediately.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            logger.debug(
                "Closing pool in_use={} idle={} crt={} exp={} acq={} rel={}",
                len(self._in_use),
                len(self._idle),
                self._stats.created,
                self._stats.expired,
                self._stats.acquired,
                self._stats.released,
            )
            while self._idle:
                _, db = self._idle.popleft()
                with contextlib.suppress(Exception):
                    db.disconnect()
            # notify all waiters so they error out
            self._not_empty.notify_all()

    def __enter__(self):  # pragma: no cover - trivial
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # pragma: no cover - trivial
        self.close()
        return False

    @property
    def backend(self) -> CypherBackend:
        """Return resolved backend instance used for new connections."""
        return self._backend

    @property
    def connect_params(self) -> dict | None:
        """Return connection parameter mapping used for auto-connect."""
        return self._connect_params

    # Convenience context acquisition
    @contextmanager
    def acquire(self, **get_kwargs) -> Iterator[CypherGraphDB]:
        """Context-managed acquisition.

        Usage:
            with pool.acquire() as db:
                db.run(...)

        Accepts the same keyword arguments as `get()` (e.g. block, timeout).
        """
        db = self.get(**get_kwargs)
        try:
            yield db
        finally:
            with contextlib.suppress(Exception):
                self.release(db)

    def _create(self):  # returns new CypherGraphDB instance
        from .cyphergraphdb import CypherGraphDB  # local import to avoid cycle

        db = CypherGraphDB(
            self._backend,
            connect_params=None,
        )
        if self._auto_connect and self._connect_params:
            with contextlib.suppress(Exception):
                db.connect(**self._connect_params)
        self._stats.created += 1
        return db

    def _expiry(self) -> float:
        """Compute expiry timestamp or 0.0 if no TTL (sentinel)."""
        if self._ttl is None:
            return 0.0
        return self._clock() + self._ttl

    def _next_idle(self):  # returns valid idle or None
        if not self._idle:
            return None
        now = self._clock()
        while self._idle:
            exp_ts, db = self._idle[0]
            if self._ttl is None or exp_ts == 0.0 or exp_ts > now:
                self._idle.popleft()
                return db
            self._idle.popleft()
            self._stats.expired += 1
            logger.trace("Expire idle id={} exp_ts={}", id(db), exp_ts)
            with contextlib.suppress(Exception):
                db.disconnect()
        return None

    def _purge_expired(self) -> None:
        if self._ttl is None or not self._idle:
            return
        now = self._clock()
        while self._idle:
            exp_ts, db = self._idle[0]
            if exp_ts > now:  # still valid
                break
            self._idle.popleft()
            self._stats.expired += 1
            logger.trace("Purge expired idle id={} exp_ts={}", id(db), exp_ts)
            with contextlib.suppress(Exception):
                db.disconnect()


__all__ = ["CypherGraphDBPool"]
