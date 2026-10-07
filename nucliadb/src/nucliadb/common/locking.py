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
import uuid
from collections.abc import AsyncGenerator
from dataclasses import dataclass

from nucliadb.common.maindb.driver import Transaction
from nucliadb_telemetry.metrics import Counter, Gauge, Histogram

from .maindb.utils import get_driver

logger = logging.getLogger(__name__)

RESOURCE_LOCK = "resource-{kbid}-{resource_id}"
RESOURCE_CREATION_SLUG_LOCK = "resource-creation-{kbid}-{resource_slug}"
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
    if key.startswith("resource-creation-"):
        return RESOURCE_CREATION_SLUG_LOCK
    elif key.startswith("resource-"):
        return RESOURCE_LOCK
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
        self.driver = get_driver()

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncGenerator[Transaction, None]:
        async with self.driver.ro_transaction(system=True) as txn:
            yield txn

    async def _cleanup_expired_locks(self) -> None:
        """Clean up expired locks older than 1 day."""
        # TODO(Marklogic) Implement cleanup for Marklogic backend
        return

    async def _maybe_cleanup_expired_locks(self) -> None:
        # Probabilistically run cleanup (1% chance) to distribute cleanup load
        # without adding overhead on every lock acquisition
        # TODO(Marklogic) Implement probabilistic cleanup for Marklogic backend
        return

    async def _get_lock_data(self) -> LockValue | None:
        # TODO(Marklogic) Implement lock retrieval for Marklogic backend
        return None

    async def _set_lock_value(self) -> None:
        # TODO(Marklogic) Implement lock setting for Marklogic backend
        return

    async def _update_lock_value(self) -> None:
        # TODO(Marklogic) Implement lock update for Marklogic backend
        return

    async def _delete_lock(self) -> None:
        # TODO(Marklogic) Implement lock deletion for Marklogic backend
        return

    async def __aenter__(self) -> "_Lock":
        # TODO(Marklogic) Implement lock acquisition for Marklogic backend
        return self

    async def _refresh_task(self) -> None:
        # TODO(Marklogic) Implement lock refresh for Marklogic backend
        return

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        # TODO(Marklogic) Implement lock release for Marklogic backend
        return

    async def is_locked(self) -> bool:
        # TODO(Marklogic) Implement lock status check for Marklogic backend
        return False


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
