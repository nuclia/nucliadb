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
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import AsyncGenerator, cast

import orjson

from nucliadb.common.maindb.exceptions import ConflictError

from .maindb.driver import Driver
from .maindb.pg import PGDriver, PGTransaction
from .maindb.utils import get_driver

logger = logging.getLogger(__name__)

NEW_SHARD_LOCK = "new-shard-{kbid}"
RESOURCE_LOCK = "resource-{kbid}-{resource_id}"
RESOURCE_CREATION_SLUG_LOCK = "resource-creation-{kbid}-{resource_slug}"
KB_SHARDS_LOCK = "shards-kb-{kbid}"
MIGRATIONS_LOCK = "migration"


class ResourceLocked(Exception):
    def __init__(self, key: str):
        self.key = key
        super().__init__(f"{key} is locked")


@dataclass
class LockValue:
    value: str
    expires_at: float


class _BaseLock(ABC):
    """Abstract base class for distributed lock implementations."""

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

    async def __aenter__(self) -> "_BaseLock":
        await self._cleanup_expired_locks()

        start = time.time()
        while True:
            try:
                lock_data = await self._get_lock_data_with_transaction()
                if lock_data is None:
                    await self._set_lock_value_with_transaction()
                    break
                else:
                    if time.time() > lock_data.expires_at:
                        # if current time is greater than when it expires, take it over
                        await self._update_lock_value_with_transaction()
                        break

                    if time.time() > start + self.lock_timeout:
                        # if current time > start time + lock timeout
                        # we've waited too long, raise exception that, we can't get the lock
                        raise ResourceLocked(key=self.user_key)
            except ConflictError:
                # if we get a conflict error, retry
                pass
            await asyncio.sleep(0.1)  # sleep before trying again
        self.task = asyncio.create_task(self._refresh_task())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.task.cancel()
        await self._delete_lock_with_transaction()

    async def _refresh_task(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.refresh_timeout)
                await self._update_lock_value_with_transaction()
            except (asyncio.CancelledError, RuntimeError):
                return
            except Exception:
                logger.exception("Failed to refresh lock")

    @abstractmethod
    async def _cleanup_expired_locks(self) -> None:
        """Clean up expired locks from storage."""
        pass

    @abstractmethod
    async def _get_lock_data_with_transaction(self) -> LockValue | None:
        """Get lock data within a transaction context."""
        pass

    @abstractmethod
    async def _set_lock_value_with_transaction(self) -> None:
        """Set lock value within a transaction context."""
        pass

    @abstractmethod
    async def _update_lock_value_with_transaction(self) -> None:
        """Update lock value within a transaction context."""
        pass

    @abstractmethod
    async def _delete_lock_with_transaction(self) -> None:
        """Delete lock within a transaction context."""
        pass

    @abstractmethod
    async def is_locked(self) -> bool:
        """Check if the lock is currently held."""
        pass


class _Lock(_BaseLock):
    def __init__(
        self,
        key: str,
        *,
        lock_timeout: float,
        expire_timeout: float,
        refresh_timeout: float,
        driver: Driver,
    ):
        super().__init__(
            key,
            lock_timeout=lock_timeout,
            expire_timeout=expire_timeout,
            refresh_timeout=refresh_timeout,
        )
        self.key = "/distributed/locks/" + self.user_key
        self.driver = driver

    async def _cleanup_expired_locks(self) -> None:
        """No-op for key-value store implementation - relies on takeover mechanism."""
        pass

    async def _get_lock_data_with_transaction(self) -> LockValue | None:
        async with self.driver.rw_transaction() as txn:
            existing_data = await txn.get(self.key, for_update=True)
            if existing_data is None:
                return None
            else:
                return LockValue(**orjson.loads(existing_data))

    async def _set_lock_value_with_transaction(self) -> None:
        async with self.driver.rw_transaction() as txn:
            await txn.insert(
                self.key,
                orjson.dumps(LockValue(self.value, time.time() + self.expire_timeout)),
            )
            await txn.commit()

    async def _update_lock_value_with_transaction(self) -> None:
        async with self.driver.rw_transaction() as txn:
            await txn.set(
                self.key,
                orjson.dumps(LockValue(self.value, time.time() + self.expire_timeout)),
            )
            await txn.commit()

    async def _delete_lock_with_transaction(self) -> None:
        async with self.driver.rw_transaction() as txn:
            await txn.delete(self.key)
            await txn.commit()

    async def is_locked(self) -> bool:
        async with get_driver().ro_transaction() as txn:
            existing_data = await txn.get(self.key, for_update=False)
            if existing_data is None:
                return False
            lock_data = LockValue(**orjson.loads(existing_data))
        return time.time() < lock_data.expires_at


class _PGLock(_BaseLock):
    def __init__(
        self,
        key: str,
        *,
        lock_timeout: float,
        expire_timeout: float,
        refresh_timeout: float,
        driver: PGDriver,
    ):
        super().__init__(
            key,
            lock_timeout=lock_timeout,
            expire_timeout=expire_timeout,
            refresh_timeout=refresh_timeout,
        )
        self.driver = driver

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncGenerator[PGTransaction, None]:
        async with self.driver._transaction(read_only=False) as txn:
            txn = cast(PGTransaction, txn)
            yield txn

    async def _cleanup_expired_locks(self) -> None:
        """Clean up expired locks older than 2 days."""
        try:
            two_days_ago = time.time() - (2 * 24 * 60 * 60)  # 2 days in seconds
            async with self.transaction() as txn:
                async with txn.connection.cursor() as cur:
                    await cur.execute(
                        "DELETE FROM distributed_locks WHERE expires_at < %s", (two_days_ago,)
                    )
                await txn.commit()
        except Exception:
            # If cleanup fails, log and continue - don't block lock acquisition
            logger.exception("Failed to cleanup expired locks")

    async def _get_lock_data_with_transaction(self) -> LockValue | None:
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

    async def _set_lock_value_with_transaction(self) -> None:
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

    async def _update_lock_value_with_transaction(self) -> None:
        async with self.transaction() as txn:
            async with txn.connection.cursor() as cur:
                await cur.execute(
                    "UPDATE distributed_locks SET lock_value = %s, expires_at = %s WHERE lock_key = %s",
                    (self.value, time.time() + self.expire_timeout, self.user_key),
                )
            await txn.commit()

    async def _delete_lock_with_transaction(self) -> None:
        async with self.transaction() as txn:
            async with txn.connection.cursor() as cur:
                await cur.execute("DELETE FROM distributed_locks WHERE lock_key = %s", (self.user_key,))
            await txn.commit()

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
) -> _BaseLock:
    """
    Context manager to get a distributed lock on a key.

    Params:
    - key: the key to lock with
    - lock_timeout: maximum time to wait for the lock before ResourceLocked is raised.
    - expire_timeout: how long by default the lock will be held without a refresh
    - refresh_timeout: how often to refresh the lock
    """
    driver = get_driver()
    if isinstance(driver, PGDriver):
        return _PGLock(
            key,
            lock_timeout=lock_timeout,
            expire_timeout=expire_timeout,
            refresh_timeout=refresh_timeout,
            driver=driver,
        )
    else:
        return _Lock(
            key,
            lock_timeout=lock_timeout,
            expire_timeout=expire_timeout,
            refresh_timeout=refresh_timeout,
            driver=driver,
        )


async def is_locked(key: str) -> bool:
    return await distributed_lock(key).is_locked()
