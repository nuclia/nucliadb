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
from typing import Optional

import orjson

from nucliadb.common.maindb.exceptions import ConflictError

from .maindb.driver import Transaction
from .maindb.utils import get_driver

logger = logging.getLogger(__name__)

NEW_SHARD_LOCK = "new-shard-{kbid}"
RESOURCE_INDEX_LOCK = "resource-index-{kbid}-{resource_id}"
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

    async def __aenter__(self) -> "_Lock":
        start = time.time()
        while True:
            try:
                async with self.driver.rw_transaction() as txn:
                    lock_data = await self.get_lock_data(txn)
                    if lock_data is None:
                        await self._set_lock_value(txn)
                        await txn.commit()
                        break
                    else:
                        if time.time() > lock_data.expires_at:
                            # if current time is greater than when it expires, take it over
                            await self._update_lock_value(txn)
                            await txn.commit()
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

    async def get_lock_data(self, txn: Transaction) -> Optional[LockValue]:
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
