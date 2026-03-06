# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
import asyncio
import contextlib
import logging
import random
import time
import uuid
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import cast

from nucliadb.common.maindb.exceptions import ConflictError
from nucliadb_telemetry.metrics import Counter, Gauge, Histogram

from .maindb.pg import PGDriver, PGTransaction
from .maindb.utils import get_driver

logger = logging.getLogger(__name__)

NEW_SHARD_LOCK = "new-shard-{kbid}"
RESOURCE_LOCK = "resource-{kbid}-{resource_id}"
RESOURCE_CREATION_SLUG_LOCK = "resource-creation-{kbid}-{resource_slug}"
KB_SHARDS_LOCK = "shards-kb-{kbid}"
MIGRATIONS_LOCK = "migration"
KB_MIGRATIONS_LOCK = "migration-{kbid}"


# Metrics
lock_acquired_counter = Counter(
    "nucliadb_lock_acquired_total",
    labels={"lock_type": ""},
)

lock_miss_counter = Counter(
    "nucliadb_lock_miss_total",
    labels={"lock_type": ""},
)

lock_timeout_counter = Counter(
    "nucliadb_lock_timeout_total",
    labels={"lock_type": ""},
)

locks_active_gauge = Gauge(
    "nucliadb_locks_active",
    labels={"lock_type": ""},
)

lock_wait_duration_histogram = Histogram(
    "nucliadb_lock_wait_duration_seconds",
    labels={"lock_type": ""},
    buckets=[
        0.001,  # 1ms
        0.005,  # 5ms
        0.010,  # 10ms
        0.025,  # 25ms
        0.050,  # 50ms
        0.100,  # 100ms
        0.250,  # 250ms
        0.500,  # 500ms
        1.0,  # 1s
        2.5,  # 2.5s
        5.0,  # 5s
        10.0,  # 10s
        30.0,  # 30s
        60.0,  # 60s
        float("inf"),
    ],
)

lock_held_duration_histogram = Histogram(
    "nucliadb_lock_held_duration_seconds",
    labels={"lock_type": ""},
    buckets=[
        0.010,  # 10ms
        0.050,  # 50ms
        0.100,  # 100ms
        0.250,  # 250ms
        0.500,  # 500ms
        1.0,  # 1s
        2.5,  # 2.5s
        5.0,  # 5s
        10.0,  # 10s
        30.0,  # 30s
        60.0,  # 60s
        120.0,  # 2min
        300.0,  # 5min
        float("inf"),
    ],
)


def _get_lock_type(key: str) -> str:
    """Extract the lock type from the lock key for metrics labeling."""
    if key.startswith("new-shard-"):
        return NEW_SHARD_LOCK
    elif key.startswith("resource-creation-"):
        return RESOURCE_CREATION_SLUG_LOCK
    elif key.startswith("resource-"):
        return RESOURCE_LOCK
    elif key.startswith("shards-kb-"):
        return KB_SHARDS_LOCK
    elif key.startswith("migration-"):
        return KB_MIGRATIONS_LOCK
    elif key == "migration":
        return MIGRATIONS_LOCK
    else:
        return "other"


class ResourceLocked(Exception):
    def __init__(self, key: str):
        self.key = key
        super().__init__(f"{key} is locked")


@dataclass
class LockValue:
    value: str
    expires_at: float


class _Lock:
    """PostgreSQL table-based distributed lock implementation."""

    task: asyncio.Task

    def __init__(
        self,
        key: str,
        *,
        lock_timeout: float,
        expire_timeout: float,
        refresh_timeout: float,
    ):
        self.user_key = key
        self.lock_timeout = lock_timeout
        self.expire_timeout = expire_timeout
        self.refresh_timeout = refresh_timeout
        self.value = uuid.uuid4().hex
        self.lock_type = _get_lock_type(self.user_key)
        self.acquired_at: float | None = None
        self.driver = cast(PGDriver, get_driver())

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncGenerator[PGTransaction, None]:
        async with self.driver._transaction(read_only=False) as txn:
            txn = cast(PGTransaction, txn)
            yield txn

    async def _cleanup_expired_locks(self) -> None:
        """Clean up expired locks older than 1 day."""
        async with self.transaction() as txn:
            async with txn.connection.cursor() as cur:
                await cur.execute(
                    "DELETE FROM distributed_locks "
                    "WHERE expires_at < EXTRACT(EPOCH FROM NOW() - INTERVAL '1 day')::DOUBLE PRECISION"
                )
            await txn.commit()

    async def _maybe_cleanup_expired_locks(self) -> None:
        # Probabilistically run cleanup (1% chance) to distribute cleanup load
        # without adding overhead on every lock acquisition
        if random.random() < 0.01:
            try:
                await self._cleanup_expired_locks()
            except Exception:
                # If cleanup fails, log but don't block lock acquisition
                logger.warning("Failed to cleanup expired locks", exc_info=True)

    async def _get_lock_data(self) -> LockValue | None:
        async with self.transaction() as txn:
            async with txn.connection.cursor() as cur:
                await cur.execute(
                    "SELECT lock_value, expires_at FROM distributed_locks WHERE lock_key = %s FOR UPDATE",
                    (self.user_key,),
                )
                row = await cur.fetchone()
                if row is None:
                    return None
                else:
                    return LockValue(value=row[0], expires_at=row[1])

    async def _set_lock_value(self) -> None:
        async with self.transaction() as txn:
            async with txn.connection.cursor() as cur:
                try:
                    await cur.execute(
                        "INSERT INTO distributed_locks (lock_key, lock_value, expires_at) VALUES (%s, %s, %s)",
                        (self.user_key, self.value, time.time() + self.expire_timeout),
                    )
                except Exception as e:
                    # If there's a unique constraint violation, it means the lock already exists
                    if "duplicate key value" in str(e).lower() or "unique constraint" in str(e).lower():
                        raise ConflictError() from e
                    raise
            await txn.commit()

    async def _update_lock_value(self) -> None:
        async with self.transaction() as txn:
            async with txn.connection.cursor() as cur:
                await cur.execute(
                    "UPDATE distributed_locks SET lock_value = %s, expires_at = %s WHERE lock_key = %s",
                    (self.value, time.time() + self.expire_timeout, self.user_key),
                )
            await txn.commit()

    async def _delete_lock(self) -> None:
        async with self.transaction() as txn:
            async with txn.connection.cursor() as cur:
                await cur.execute("DELETE FROM distributed_locks WHERE lock_key = %s", (self.user_key,))
            await txn.commit()

    async def __aenter__(self) -> "_Lock":
        await self._maybe_cleanup_expired_locks()

        start = time.monotonic()
        while True:
            try:
                lock_data = await self._get_lock_data()
                if lock_data is None:
                    await self._set_lock_value()
                    break
                else:
                    lock_miss_counter.inc(labels={"lock_type": self.lock_type})

                    if time.time() > lock_data.expires_at:
                        # if current time is greater than when it expires, take it over
                        await self._update_lock_value()
                        break

                    if time.monotonic() > start + self.lock_timeout:
                        # if current time > start time + lock timeout
                        # we've waited too long, raise exception that, we can't get the lock
                        lock_timeout_counter.inc(labels={"lock_type": self.lock_type})
                        raise ResourceLocked(key=self.user_key)
            except ConflictError:
                # if we get a conflict error, retry
                pass
            await asyncio.sleep(0.1)  # sleep before trying again

        # Record metrics after successful acquisition
        self.acquired_at = time.monotonic()
        wait_duration = self.acquired_at - start
        lock_wait_duration_histogram.observe(wait_duration, labels={"lock_type": self.lock_type})
        lock_acquired_counter.inc(labels={"lock_type": self.lock_type})
        locks_active_gauge.inc(1, labels={"lock_type": self.lock_type})

        self.task = asyncio.create_task(self._refresh_task())
        return self

    async def _refresh_task(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.refresh_timeout)
                await self._update_lock_value()
            except (asyncio.CancelledError, RuntimeError):
                return
            except Exception:
                logger.exception("Failed to refresh lock")

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.task.cancel()

        # Record how long the lock was held
        if self.acquired_at is not None:
            held_duration = time.monotonic() - self.acquired_at
            lock_held_duration_histogram.observe(held_duration, labels={"lock_type": self.lock_type})

        locks_active_gauge.dec(1, labels={"lock_type": self.lock_type})

        await self._delete_lock()

    async def is_locked(self) -> bool:
        async with self.transaction() as txn:
            async with txn.connection.cursor() as cur:
                await cur.execute(
                    "SELECT expires_at FROM distributed_locks WHERE lock_key = %s", (self.user_key,)
                )
                row = await cur.fetchone()
        return row is not None and time.time() < row[0]


def distributed_lock(
    key: str,
    lock_timeout: float = 60.0,
    expire_timeout: float = 30.0,
    refresh_timeout: float = 10.0,
) -> _Lock:
    """
    Context manager to get a distributed lock on a key.

    Params:
    - key: the key to lock with
    - lock_timeout: maximum time to wait for the lock before ResourceLocked is raised.
    - expire_timeout: how long by default the lock will be held without a refresh
    - refresh_timeout: how often to refresh the lock
    """
    return _Lock(
        key,
        lock_timeout=lock_timeout,
        expire_timeout=expire_timeout,
        refresh_timeout=refresh_timeout,
    )


async def is_locked(key: str) -> bool:
    return await distributed_lock(key).is_locked()
