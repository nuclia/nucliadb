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
import time
import uuid

from redis import asyncio as aioredis


class ResourceLocked(Exception):
    ...


class RedisLock:
    """
    Reference: https://redis.io/docs/manual/patterns/distributed-locks/

    This is not the most safe redlock implementation.

    This is only completely safe on a single redis instance.

    If you have a multi master setup, you should use a redlock implementation.
    """

    task: asyncio.Task

    def __init__(self, key: str, mng: "RedisDistributedLockManager"):
        self.mng = mng
        self.key = "_distributed_lock" + key
        self.value = uuid.uuid4().hex

    async def __aenter__(self) -> "RedisLock":
        start = time.monotonic()
        while not await self.mng.driver.set(
            self.key, self.value, nx=True, ex=self.mng.expire_timeout
        ):
            if time.monotonic() - start > self.mng.timeout:
                raise ResourceLocked()

        self.task = asyncio.create_task(self._refresh_task())
        return self

    async def _refresh_task(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.mng.refresh_interval)
                await self.mng.driver.set(
                    self.key, self.value, ex=self.mng.expire_timeout
                )
            except (asyncio.CancelledError, RuntimeError):
                return

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.task.cancel()
        await self.mng.release_call(keys=[self.key], args=[self.value])


class RedisDistributedLockManager:
    def __init__(
        self,
        dsn: str,
        expire_timeout: int = 30,
        timeout: int = 10,
        refresh_interval: int = 10,
    ):
        self.dsn = dsn
        self.driver = aioredis.from_url(dsn)
        self.release_call = self.driver.register_script(
            """if redis.call("get",KEYS[1]) == ARGV[1] then
    return redis.call("del",KEYS[1])
else
    return 0
end
"""
        )
        self.expire_timeout = expire_timeout
        self.refresh_interval = refresh_interval
        self.timeout = timeout

    async def close(self):
        await self.driver.close(close_connection_pool=True)

    def lock(self, key: str) -> RedisLock:
        return RedisLock(key, self)
