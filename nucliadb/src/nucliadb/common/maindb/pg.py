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
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional

import backoff
import psycopg
import psycopg_pool

from nucliadb.common.maindb.driver import DEFAULT_SCAN_LIMIT, Driver, Transaction
from nucliadb.common.maindb.exceptions import ConflictError
from nucliadb_telemetry import metrics

RETRIABLE_EXCEPTIONS = (
    psycopg_pool.PoolTimeout,
    OSError,
    ConnectionResetError,
)

logger = logging.getLogger(__name__)

# Request Metrics
pg_observer = metrics.Observer(
    "pg_client",
    labels={"type": ""},
)

# Pool metrics
POOL_METRICS_COUNTERS = {
    # Requests for a connection to the pool
    "requests_num": metrics.Counter("pg_client_pool_requests_total"),
    "requests_queued": metrics.Counter("pg_client_pool_requests_queued_total"),
    "requests_errors": metrics.Counter("pg_client_pool_requests_errors_total"),
    "requests_wait_ms": metrics.Counter("pg_client_pool_requests_queued_seconds_total"),
    "usage_ms": metrics.Counter("pg_client_pool_requests_usage_seconds_total"),
    # Pool opening a connection to PG
    "connections_num": metrics.Counter("pg_client_pool_connections_total"),
    "connections_ms": metrics.Counter("pg_client_pool_connections_seconds_total"),
}
POOL_METRICS_GAUGES = {
    "pool_size": metrics.Gauge("pg_client_pool_connections_open"),
    # The two below most likely change too rapidly to be useful in a metric
    "pool_available": metrics.Gauge("pg_client_pool_connections_available"),
    "requests_waiting": metrics.Gauge("pg_client_pool_requests_waiting"),
}


class DataLayer:
    def __init__(self, connection: psycopg.AsyncConnection):
        self.connection = connection

    async def get(self, key: str, select_for_update: bool = False) -> Optional[bytes]:
        with pg_observer({"type": "get"}):
            statement = "SELECT value FROM resources WHERE key = %s"
            if select_for_update:
                statement += " FOR UPDATE"
            async with self.connection.cursor() as cur:
                await cur.execute(statement, (key,))
                row = await cur.fetchone()
                return row[0] if row else None

    async def set(self, key: str, value: bytes) -> None:
        with pg_observer({"type": "set"}):
            async with self.connection.cursor() as cur:
                await cur.execute(
                    "INSERT INTO resources (key, value) "
                    "VALUES (%s, %s) "
                    "ON CONFLICT (key) "
                    "DO UPDATE SET value = EXCLUDED.value",
                    (key, value),
                )

    async def insert(self, key: str, value: bytes) -> None:
        with pg_observer({"type": "insert"}):
            async with self.connection.cursor() as cur:
                try:
                    await cur.execute(
                        "INSERT INTO resources (key, value) VALUES (%s, %s) ",
                        (key, value),
                    )
                except psycopg.errors.UniqueViolation:
                    raise ConflictError(key)

    async def delete(self, key: str) -> None:
        with pg_observer({"type": "delete"}):
            async with self.connection.cursor() as cur:
                await cur.execute("DELETE FROM resources WHERE key = %s", (key,))

    async def batch_get(self, keys: list[str], select_for_update: bool = False) -> list[Optional[bytes]]:
        with pg_observer({"type": "batch_get"}):
            async with self.connection.cursor() as cur:
                statement = "SELECT key, value FROM resources WHERE key = ANY(%s)"
                if select_for_update:
                    statement += " FOR UPDATE"
                await cur.execute(statement, (keys,))
                records = {record[0]: record[1] for record in await cur.fetchall()}
            # get sorted by keys
            return [records.get(key) for key in keys]

    async def scan_keys(
        self,
        prefix: str,
        limit: int = DEFAULT_SCAN_LIMIT,
        include_start: bool = True,
    ) -> AsyncGenerator[str, None]:
        query = "SELECT key FROM resources WHERE key LIKE %s ORDER BY key"

        args: list[Any] = [prefix + "%"]
        if limit > 0:
            query += " LIMIT %s"
            args.append(limit)
        with pg_observer({"type": "scan_keys"}):
            async with self.connection.cursor() as cur:
                async for record in cur.stream(query, args):
                    if not include_start and record[0] == prefix:
                        continue
                    yield record[0]

    async def count(self, match: str) -> int:
        with pg_observer({"type": "count"}):
            async with self.connection.cursor() as cur:
                await cur.execute("SELECT count(*) FROM resources WHERE key LIKE %s", (match + "%",))
                row = await cur.fetchone()
                return row[0]  # type: ignore


class PGTransaction(Transaction):
    driver: PGDriver

    def __init__(
        self,
        driver: PGDriver,
        connection: psycopg.AsyncConnection,
    ):
        self.driver = driver
        self.connection = connection
        self.data_layer = DataLayer(connection)
        self.open = True

    async def abort(self):
        with pg_observer({"type": "rollback"}):
            if self.open:
                try:
                    await self.connection.rollback()
                finally:
                    self.open = False

    async def commit(self):
        with pg_observer({"type": "commit"}):
            try:
                await self.connection.commit()
            except Exception:
                await self.connection.rollback()
                raise
            finally:
                self.open = False

    async def batch_get(self, keys: list[str], for_update: bool = True):
        return await self.data_layer.batch_get(keys, select_for_update=for_update)

    async def get(self, key: str, for_update: bool = True) -> Optional[bytes]:
        return await self.data_layer.get(key, select_for_update=for_update)

    async def set(self, key: str, value: bytes):
        await self.data_layer.set(key, value)

    async def insert(self, key: str, value: bytes):
        await self.data_layer.insert(key, value)

    async def delete(self, key: str):
        await self.data_layer.delete(key)

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, jitter=backoff.random_jitter, max_tries=2)
    async def keys(
        self,
        match: str,
        count: int = DEFAULT_SCAN_LIMIT,
        include_start: bool = True,
    ):
        # Check out a new connection to guarantee that the cursor iteration does not
        # run concurrently with other queries
        async with self.driver._get_connection() as conn, conn.transaction():
            dl = DataLayer(conn)
            async for key in dl.scan_keys(match, count, include_start=include_start):
                yield key

    async def count(self, match: str) -> int:
        return await self.data_layer.count(match)


class ReadOnlyPGTransaction(Transaction):
    driver: PGDriver

    def __init__(self, driver: PGDriver):
        self.driver = driver
        self.open = True

    async def abort(self):
        # This is a no-op because we don't have a transaction to abort on read-only transactions.
        ...

    async def commit(self):
        raise Exception("Cannot commit transaction in read only mode")

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, jitter=backoff.random_jitter, max_tries=3)
    async def batch_get(self, keys: list[str], for_update: bool = False):
        async with self.driver._get_connection() as conn:
            return await DataLayer(conn).batch_get(keys, select_for_update=False)

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, jitter=backoff.random_jitter, max_tries=3)
    async def get(self, key: str, for_update: bool = False) -> Optional[bytes]:
        async with self.driver._get_connection() as conn:
            return await DataLayer(conn).get(key, select_for_update=False)

    async def set(self, key: str, value: bytes):
        raise Exception("Cannot set in read only transaction")

    async def delete(self, key: str):
        raise Exception("Cannot delete in read only transaction")

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, jitter=backoff.random_jitter, max_tries=3)
    async def keys(
        self,
        match: str,
        count: int = DEFAULT_SCAN_LIMIT,
        include_start: bool = True,
    ):
        async with self.driver._get_connection() as conn, conn.transaction():
            dl = DataLayer(conn)
            async for key in dl.scan_keys(match, count, include_start=include_start):
                yield key

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, jitter=backoff.random_jitter, max_tries=3)
    async def count(self, match: str) -> int:
        async with self.driver._get_connection() as conn:
            return await DataLayer(conn).count(match)


class PGDriver(Driver):
    pool: psycopg_pool.AsyncConnectionPool

    def __init__(
        self,
        url: str,
        connection_pool_min_size: int = 10,
        connection_pool_max_size: int = 10,
        acquire_timeout_ms: int = 200,
    ):
        self.url = url
        self.connection_pool_min_size = connection_pool_min_size
        self.connection_pool_max_size = connection_pool_max_size
        self.acquire_timeout_ms = acquire_timeout_ms
        self._lock = asyncio.Lock()

    async def initialize(self):
        async with self._lock:
            if self.initialized is False:
                self.pool = psycopg_pool.AsyncConnectionPool(
                    self.url,
                    min_size=self.connection_pool_min_size,
                    max_size=self.connection_pool_max_size,
                    check=psycopg_pool.AsyncConnectionPool.check_connection,
                    open=False,
                )
                await self.pool.open()

            self.initialized = True
            self.metrics_task = asyncio.create_task(self._report_metrics_task())

    async def finalize(self):
        async with self._lock:
            if self.initialized:
                await self.pool.close()
                self.initialized = False
                self.metrics_task.cancel()

    async def _report_metrics_task(self):
        while True:
            self._report_metrics()
            await asyncio.sleep(60)

    def _report_metrics(self):
        if not self.initialized:
            return

        metrics = self.pool.pop_stats()
        for key, metric in POOL_METRICS_COUNTERS.items():
            value = metrics.get(key, 0)
            if key.endswith("_ms"):
                value /= 1000
            metric.inc(value=value)

        for key, metric in POOL_METRICS_GAUGES.items():
            value = metrics.get(key, 0)
            metric.set(value)

    @asynccontextmanager
    async def transaction(self, read_only: bool = False) -> AsyncGenerator[Transaction, None]:
        if read_only:
            yield ReadOnlyPGTransaction(self)
        else:
            async with self._get_connection() as conn:
                yield PGTransaction(self, conn)

    @asynccontextmanager
    async def _get_connection(self) -> AsyncGenerator[psycopg.AsyncConnection, None]:
        timeout = self.acquire_timeout_ms / 1000
        # Manual retry loop since backoff.on_exception does not play well with async context managers
        retries = 0
        while True:
            with pg_observer({"type": "acquire"}):
                try:
                    async with self.pool.connection(timeout=timeout) as conn:
                        yield conn
                        return
                except psycopg_pool.PoolTimeout as e:
                    logger.warning(
                        f"Timeout getting connection from the pool, backing off. Retries = {retries}"
                    )
                    if retries < 3:
                        await asyncio.sleep(1)
                        retries += 1
                    else:
                        raise e
                except Exception as e:
                    raise e
