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
import logging
import time
import uuid
from dataclasses import dataclass

import orjson

from nucliadb.common.maindb.exceptions import ConflictError
from nucliadb_telemetry.metrics import Counter, Gauge, Histogram

from .maindb.driver import Transaction
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
        self.key = "/distributed/locks/" + self.user_key
        self.lock_timeout = lock_timeout
        self.expire_timeout = expire_timeout
        self.refresh_timeout = refresh_timeout
        self.value = uuid.uuid4().hex
        self.driver = get_driver()
        self.lock_type = _get_lock_type(self.user_key)
        self.acquired_at: float | None = None

    async def __aenter__(self) -> "_Lock":
        start = time.monotonic()
        while True:
            try:
                async with self.driver.rw_transaction() as txn:
                    lock_data = await self.get_lock_data(txn)
                    if lock_data is None:
                        await self._set_lock_value(txn)
                        await txn.commit()
                        break
                    else:
                        lock_miss_counter.inc(labels={"lock_type": self.lock_type})

                        if time.time() > lock_data.expires_at:
                            # if current time is greater than when it expires, take it over
                            await self._update_lock_value(txn)
                            await txn.commit()
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

    async def get_lock_data(self, txn: Transaction) -> LockValue | None:
        existing_data = await txn.get(self.key, for_update=True)
        if existing_data is None:
            return None
        else:
            return LockValue(**orjson.loads(existing_data))

    async def _update_lock_value(self, txn: Transaction) -> None:
        """
        Update the value for the lock.
        """
        await txn.set(
            self.key,
            orjson.dumps(LockValue(self.value, time.time() + self.expire_timeout)),
        )

    async def _set_lock_value(self, txn: Transaction) -> None:
        """
        Set the value for the lock. If lock already exists, it doesn't update and raises a ConflictError.
        """
        await txn.insert(
            self.key,
            orjson.dumps(LockValue(self.value, time.time() + self.expire_timeout)),
        )

    async def _refresh_task(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.refresh_timeout)
                async with self.driver.rw_transaction() as txn:
                    await self._update_lock_value(txn)
                    await txn.commit()
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

        async with self.driver.rw_transaction() as txn:
            await txn.delete(self.key)
            await txn.commit()

    async def is_locked(self) -> bool:
        async with get_driver().ro_transaction() as txn:
            lock_data = await self.get_lock_data(txn)
        return lock_data is not None and time.time() < lock_data.expires_at


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
