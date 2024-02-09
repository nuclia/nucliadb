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
from __future__ import annotations

import asyncio
from typing import Any, AsyncGenerator, Optional, Union

import asyncpg
import backoff

from nucliadb.common.maindb.driver import DEFAULT_SCAN_LIMIT, Driver, Transaction

RETRIABLE_EXCEPTIONS = (
    asyncpg.CannotConnectNowError,
    OSError,
    ConnectionResetError,
)

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS resources (
    key TEXT PRIMARY KEY,
    value BYTEA
);
"""


class DataLayer:
    def __init__(self, connection: Union[asyncpg.Connection, asyncpg.Pool]):
        self.connection = connection
        self.lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[bytes]:
        async with self.lock:
            return await self.connection.fetchval(
                "SELECT value FROM resources WHERE key = $1", key
            )

    async def set(self, key: str, value: bytes) -> None:
        async with self.lock:
            await self.connection.execute(
                """
INSERT INTO resources (key, value)
VALUES ($1, $2)
ON CONFLICT (key)
DO UPDATE SET value = EXCLUDED.value
""",
                key,
                value,
            )

    async def delete(self, key: str) -> None:
        async with self.lock:
            await self.connection.execute("DELETE FROM resources WHERE key = $1", key)

    async def batch_get(self, keys: list[str]) -> list[Optional[bytes]]:
        async with self.lock:
            records = {
                record["key"]: record["value"]
                for record in await self.connection.fetch(
                    "SELECT key, value FROM resources WHERE key = ANY($1)", keys
                )
            }
        # get sorted by keys
        return [records.get(key) for key in keys]

    async def scan_keys(
        self,
        prefix: str,
        limit: int = DEFAULT_SCAN_LIMIT,
        include_start: bool = True,
    ) -> AsyncGenerator[str, None]:
        query = """SELECT key FROM resources
WHERE key LIKE $1
ORDER BY key
"""
        args: list[Any] = [prefix + "%"]
        if limit > 0:
            query += " LIMIT $2"
            args.append(limit)
        async with self.lock:
            async for record in self.connection.cursor(query, *args):
                if not include_start and record["key"] == prefix:
                    continue
                yield record["key"]

    async def count(self, match: str) -> int:
        async with self.lock:
            results = await self.connection.fetch(
                "SELECT count(*) FROM resources WHERE key LIKE $1", match + "%"
            )
        return results[0]["count"]


class PGTransaction(Transaction):
    driver: PGDriver

    def __init__(
        self,
        pool: asyncpg.Pool,
        connection: asyncpg.Connection,
        txn: Any,
        driver: PGDriver,
    ):
        self.pool = pool
        self.connection = connection
        self.data_layer = DataLayer(connection)
        self.txn = txn
        self.driver = driver
        self.open = True
        self._lock = asyncio.Lock()

    async def abort(self):
        async with self._lock:
            if self.open:
                try:
                    await self.txn.rollback()
                finally:
                    self.open = False
                    await self.connection.close()

    async def commit(self):
        async with self._lock:
            try:
                await self.txn.commit()
            except Exception:
                await self.txn.rollback()
                raise
            finally:
                self.open = False
                await self.connection.close()

    async def batch_get(self, keys: list[str]):
        return await self.data_layer.batch_get(keys)

    async def get(self, key: str) -> Optional[bytes]:
        return await self.data_layer.get(key)

    async def set(self, key: str, value: bytes):
        await self.data_layer.set(key, value)

    async def delete(self, key: str):
        await self.data_layer.delete(key)

    @backoff.on_exception(
        backoff.expo, RETRIABLE_EXCEPTIONS, jitter=backoff.random_jitter, max_tries=2
    )
    async def keys(
        self,
        match: str,
        count: int = DEFAULT_SCAN_LIMIT,
        include_start: bool = True,
    ):
        async with self.pool.acquire() as conn, conn.transaction():
            # all txn implementations implement this API outside of the current txn
            dl = DataLayer(conn)
            async for key in dl.scan_keys(match, count, include_start=include_start):
                yield key

    async def count(self, match: str) -> int:
        return await self.data_layer.count(match)


class ReadOnlyPGTransaction(Transaction):
    driver: PGDriver

    def __init__(self, pool: asyncpg.Pool, driver: PGDriver):
        self.pool = pool
        self.driver = driver
        self.open = True

    async def abort(self):
        # This is a no-op because we don't have a transaction to abort on read-only transactions.
        ...

    async def commit(self):
        raise Exception("Cannot commit transaction in read only mode")

    async def batch_get(self, keys: list[str]):
        return await DataLayer(self.pool).batch_get(keys)

    async def get(self, key: str) -> Optional[bytes]:
        return await DataLayer(self.pool).get(key)

    async def set(self, key: str, value: bytes):
        raise Exception("Cannot set in read only transaction")

    async def delete(self, key: str):
        raise Exception("Cannot delete in read only transaction")

    async def keys(
        self,
        match: str,
        count: int = DEFAULT_SCAN_LIMIT,
        include_start: bool = True,
    ):
        async with self.pool.acquire() as conn, conn.transaction():
            # all txn implementations implement this API outside of the current txn
            dl = DataLayer(conn)
            async for key in dl.scan_keys(match, count, include_start=include_start):
                yield key

    async def count(self, match: str) -> int:
        return await DataLayer(self.pool).count(match)


class PGDriver(Driver):
    pool: asyncpg.Pool

    def __init__(self, url: str, connection_pool_max_size: int = 10):
        self.url = url
        self.connection_pool_max_size = connection_pool_max_size
        self._lock = asyncio.Lock()

    async def initialize(self):
        async with self._lock:
            if self.initialized is False:
                self.pool = await asyncpg.create_pool(
                    self.url,
                    max_size=self.connection_pool_max_size,
                )

                # check if table exists
                try:
                    async with self.pool.acquire() as conn:
                        await conn.execute(CREATE_TABLE)
                except asyncpg.exceptions.UniqueViolationError:  # pragma: no cover
                    pass

            self.initialized = True

    async def finalize(self):
        async with self._lock:
            await self.pool.close()
            self.initialized = False

    @backoff.on_exception(
        backoff.expo, RETRIABLE_EXCEPTIONS, jitter=backoff.random_jitter, max_tries=3
    )
    async def begin(
        self, read_only: bool = False
    ) -> Union[PGTransaction, ReadOnlyPGTransaction]:
        if read_only:
            return ReadOnlyPGTransaction(self.pool, driver=self)
        else:
            conn: asyncpg.Connection = await self.pool.acquire()
            txn = conn.transaction()
            await txn.start()
            return PGTransaction(self.pool, conn, txn, driver=self)
