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
KB_SHARDS_LOCK = "shards-kb-{kbid}"


class ResourceLocked(Exception): ...


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
        self.key = "/distributed/locks/" + key
        self.lock_timeout = lock_timeout
        self.expire_timeout = expire_timeout
        self.refresh_timeout = refresh_timeout
        self.value = uuid.uuid4().hex
        self.driver = get_driver()

    async def __aenter__(self) -> "_Lock":
        start = time.time()
        while True:
            try:
                async with self.driver.transaction() as txn:
                    lock_data = await self.get_lock_data(txn)
                    if lock_data is None:
                        await self._set_lock_value(txn)
                        await txn.commit()
                        break
                    else:
                        if time.time() > lock_data.expires_at:
                            # if current time is greater than when it expires, take it over
                            await self._set_lock_value(txn)
                            await txn.commit()
                            break

                        if time.time() > start + self.lock_timeout:
                            # if current time > start time + lock timeout
                            # we've waited too long, raise exception that, we can't get the lock
                            raise ResourceLocked()
            except ConflictError:
                # if we get a conflict error, retry
                pass
            await asyncio.sleep(0.1)  # sleep before trying again
        self.task = asyncio.create_task(self._refresh_task())
        return self

    async def get_lock_data(self, txn: Transaction) -> Optional[LockValue]:
        existing_data = await txn.get(self.key)
        if existing_data is None:
            return None
        else:
            return LockValue(**orjson.loads(existing_data))

    async def _set_lock_value(self, txn: Transaction) -> None:
        await txn.set(
            self.key,
            orjson.dumps(LockValue(self.value, time.time() + self.expire_timeout)),
        )

    async def _refresh_task(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.refresh_timeout)
                async with self.driver.transaction() as txn:
                    await self._set_lock_value(txn)
                    await txn.commit()
            except (asyncio.CancelledError, RuntimeError):
                return
            except Exception:
                logger.exception("Failed to refresh lock")

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.task.cancel()
        async with self.driver.transaction() as txn:
            await txn.delete(self.key)
            await txn.commit()

    async def is_locked(self, key: str) -> bool:
        async with get_driver().transaction(read_only=True) as txn:
            lock_data = await self.get_lock_data(txn)
        return lock_data is None or time.time() > lock_data.expires_at


def distributed_lock(
    key: str,
    lock_timeout: float = 60.0,  # max time to wait for lock
    expire_timeout: float = 30.0,  # how long by default the lock will be held without a refresh
    refresh_timeout: float = 10.0,  # how often to refresh
) -> _Lock:
    return _Lock(
        key,
        lock_timeout=lock_timeout,
        expire_timeout=expire_timeout,
        refresh_timeout=refresh_timeout,
    )
