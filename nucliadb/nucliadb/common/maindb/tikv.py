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
import contextlib
import logging
import random
from typing import Any, List, Optional, Union

import backoff

from nucliadb.common.maindb.driver import (
    DEFAULT_BATCH_SCAN_LIMIT,
    DEFAULT_SCAN_LIMIT,
    Driver,
    Transaction,
)
from nucliadb.common.maindb.exceptions import ConflictError
from nucliadb_telemetry import metrics

try:
    from tikv_client import asynchronous  # type: ignore

    TiKV = True
except ImportError:  # pragma: no cover
    TiKV = False


class LeaderNotFoundError(Exception):
    """
    Raised when the tikv client raises an exception indicating that the leader of a region is not found.
    This is a transient error and the operation should be retried.
    """

    pass


tikv_observer = metrics.Observer(
    "tikv_client",
    labels={"type": ""},
    error_mappings={
        "conflict_error": ConflictError,
        "timeout_error": TimeoutError,
        "leader_not_found_error": LeaderNotFoundError,
    },
)
logger = logging.getLogger(__name__)


class TiKVDataLayer:
    def __init__(
        self, connection: Union[asynchronous.RawClient, asynchronous.Transaction]
    ):
        self.connection = connection

    async def abort(self):
        with tikv_observer({"type": "rollback"}):
            try:
                await self.connection.rollback()
            except Exception:
                logger.exception("Error rolling back transaction")

    async def commit(self):
        with tikv_observer({"type": "commit"}), self.tikv_error_handler():
            await self.connection.commit()

    async def batch_get(self, keys: list[str]) -> list[Optional[bytes]]:
        bytes_keys: list[bytes] = [x.encode() for x in keys]
        with tikv_observer({"type": "batch_get"}), self.tikv_error_handler():
            output = {}
            for key, value in await self.connection.batch_get(bytes_keys):
                output[key.decode()] = value
            return [output.get(key) for key in keys]

    @backoff.on_exception(
        backoff.expo,
        (TimeoutError, LeaderNotFoundError),
        jitter=backoff.random_jitter,
        max_tries=2,
    )
    async def get(self, key: str) -> Optional[bytes]:
        with tikv_observer({"type": "get"}), self.tikv_error_handler():
            return await self.connection.get(key.encode())

    @contextlib.contextmanager
    def tikv_error_handler(self):
        """
        The tikv_client library does not provide specific exceptions and simply
        raises generic Exception class with different error strings. That forces
        us to parse the error string to determine the type of error...
        """
        try:
            yield
        except Exception as exc:
            exc_text = str(exc)
            if "WriteConflict" in exc_text:
                raise ConflictError(exc_text) from exc
            elif "4-DEADLINE_EXCEEDED" in exc_text:
                raise TimeoutError(exc_text) from exc
            elif "Leader of region" in exc_text and "not found" in exc_text:
                raise LeaderNotFoundError(exc_text) from exc
            else:
                raise

    async def set(self, key: str, value: bytes) -> None:
        with tikv_observer({"type": "put"}), self.tikv_error_handler():
            await self.connection.put(key.encode(), value)

    async def delete(self, key: str) -> None:
        with tikv_observer({"type": "delete"}), self.tikv_error_handler():
            await self.connection.delete(key.encode())

    async def keys(
        self,
        match: str,
        count: int = DEFAULT_SCAN_LIMIT,
        include_start: bool = True,
    ):
        """
        Get keys from tikv, up to a configurable limit.

        Use -1 as the count of objects keep iterating in batches
        until all matching keys are retrieved.
        With any other count, only up to count keys will be returned.
        """
        get_all_keys = count == -1
        limit = DEFAULT_BATCH_SCAN_LIMIT if get_all_keys else count
        start_key = match.encode()
        _include_start = include_start

        while True:
            with tikv_observer({"type": "scan_keys"}), self.tikv_error_handler():
                keys = await self.connection.scan_keys(
                    start=start_key,
                    end=None,
                    limit=limit,
                    include_start=_include_start,
                )
            for key in keys:
                str_key = key.decode()
                if str_key.startswith(match):
                    yield str_key
                else:
                    break
            else:
                if len(keys) == limit and get_all_keys:
                    # If all keys were requested and it may exist
                    # some more keys to retrieve
                    start_key = keys[-1]
                    _include_start = False
                    continue

            # If not all keys were requested
            # or the for loop found an unmatched key
            break

    async def count(self, match: str) -> int:
        """
        Count the number of keys that match the given prefix
        as efficiently as possible with the available API.
        """
        original_match = match.encode()
        start_key = original_match
        _include_start = True
        batch_size = 5000

        value = 0
        while True:
            with tikv_observer({"type": "scan_keys"}), self.tikv_error_handler():
                keys = await self.connection.scan_keys(
                    start=start_key,
                    end=None,
                    limit=batch_size,
                    include_start=_include_start,
                )
            if len(keys) == 0:
                break

            if not keys[-1].startswith(original_match):
                # done counting this range, find the correct size of the match
                # with a binary search and break out
                left, right = 0, len(keys) - 1
                result_index = 0
                match_found = False
                while left <= right:
                    mid = left + (right - left) // 2

                    if keys[mid].startswith(original_match):
                        match_found = True
                        left = mid + 1  # Move to the right half
                        result_index = mid
                    else:
                        right = mid - 1  # Move to the left half
                if match_found:
                    value += result_index + 1
                break
            else:
                value += len(keys)

            if len(keys) == batch_size:
                start_key = keys[-1]
                _include_start = False
                continue
            else:
                # done counting
                break
        return value


class TiKVTransaction(Transaction):
    driver: TiKVDriver

    def __init__(self, txn: Any, driver: TiKVDriver):
        self.txn = txn
        self.driver = driver
        self.data_layer = TiKVDataLayer(txn)
        self.open = True

    async def abort(self):
        if not self.open:
            return
        await self.data_layer.abort()
        self.open = False

    async def commit(self):
        assert self.open
        await self.data_layer.commit()
        self.open = False

    async def batch_get(self, keys: list[str]) -> list[Optional[bytes]]:
        assert self.open
        return await self.data_layer.batch_get(keys)

    @backoff.on_exception(
        backoff.expo,
        (TimeoutError, LeaderNotFoundError),
        jitter=backoff.random_jitter,
        max_tries=2,
    )
    async def get(self, key: str) -> Optional[bytes]:
        assert self.open
        return await self.data_layer.get(key)

    async def set(self, key: str, value: bytes) -> None:
        assert self.open
        return await self.data_layer.set(key, value)

    async def delete(self, key: str) -> None:
        assert self.open
        return await self.data_layer.delete(key)

    async def keys(
        self, match: str, count: int = DEFAULT_SCAN_LIMIT, include_start: bool = True
    ):
        assert self.open
        # XXX must have connection outside of current txn
        conn_holder = self.driver.get_connection_holder()
        txn = await conn_holder.get_snapshot()
        dl = TiKVDataLayer(txn)

        async for key in dl.keys(match, count, include_start):
            yield key

    async def count(self, match: str) -> int:
        assert self.open
        return await self.data_layer.count(match)


class ReadOnlyTiKVTransaction(Transaction):
    driver: TiKVDriver

    def __init__(self, connection: asynchronous.Snapshot, driver: TiKVDriver):
        self.connection = connection
        self.data_layer = TiKVDataLayer(connection)
        self.driver = driver
        self.open = True

    async def abort(self):
        self.open = False
        # Read only transactions are implemented as snapshots, which
        # are read only and isolated, and they don't need to be aborted.

    async def commit(self):
        raise Exception("Cannot commit transaction in read only mode")

    async def batch_get(self, keys: list[str]) -> list[Optional[bytes]]:
        assert self.open
        return await self.data_layer.batch_get(keys)

    async def get(self, key: str) -> Optional[bytes]:
        assert self.open
        return await self.data_layer.get(key)

    async def set(self, key: str, value: bytes) -> None:
        raise Exception("Cannot set in read only transaction")

    async def delete(self, key: str) -> None:
        raise Exception("Cannot delete in read only transaction")

    async def keys(
        self, match: str, count: int = DEFAULT_SCAN_LIMIT, include_start: bool = True
    ):
        assert self.open
        async for key in self.data_layer.keys(match, count, include_start):
            yield key

    async def count(self, match: str) -> int:
        assert self.open
        return await self.data_layer.count(match)


class ConnectionHolder:
    _txn_connection: asynchronous.TransactionClient

    def __init__(self, url: list[str]):
        self.url = url
        self.connect_lock = asyncio.Lock()

    async def initialize(self) -> None:
        self._txn_connection = await asynchronous.TransactionClient.connect(self.url)

    async def get_snapshot(
        self, timestamp: Optional[float] = None, retried: bool = False
    ) -> asynchronous.Snapshot:
        if self.connect_lock.locked():  # pragma: no cover
            async with self.connect_lock:
                ...
        try:
            if timestamp is None:
                with tikv_observer({"type": "current_timestamp"}):
                    timestamp = await self._txn_connection.current_timestamp()
            return self._txn_connection.snapshot(timestamp, pessimistic=False)
        except Exception:
            if retried:
                raise
            logger.exception(
                f"Error getting snapshot for tikv. Retrying once and then failing."
            )
            await self.reinitialize()
            return await self.get_snapshot(timestamp, retried=True)

    async def begin_transaction(self) -> asynchronous.Transaction:
        if self.connect_lock.locked():  # pragma: no cover
            async with self.connect_lock:
                ...
        try:
            # pessimistic=False means faster but more conflicts
            with tikv_observer({"type": "begin"}):
                return await self._txn_connection.begin(pessimistic=False)
        except Exception:
            logger.exception(
                f"Error getting transaction for tikv. Retrying once and then failing."
            )
            await self.reinitialize()
            return await self._txn_connection.begin(pessimistic=False)

    async def reinitialize(self) -> None:
        if self.connect_lock.locked():
            async with self.connect_lock:
                # wait for lock and then just continue because someone else is establishing the connection
                return
        else:
            async with self.connect_lock:
                logger.warning("Reconnecting to TiKV")
                await self.initialize()


class TiKVDriver(Driver):
    def __init__(self, url: List[str], pool_size: int = 3):
        if TiKV is False:
            raise ImportError("TiKV is not installed")
        self.url = url
        self.pool: list[ConnectionHolder] = []
        self.pool_size = pool_size

    async def initialize(self):
        self.pool = [ConnectionHolder(self.url) for _ in range(self.pool_size)]
        for holder in self.pool:
            await holder.reinitialize()

    async def finalize(self):
        self.pool.clear()

    def get_connection_holder(self) -> ConnectionHolder:
        return random.choice(self.pool)

    async def begin(
        self, read_only: bool = False
    ) -> Union[TiKVTransaction, ReadOnlyTiKVTransaction]:
        conn = self.get_connection_holder()
        # if read_only:
        #     return ReadOnlyTiKVTransaction(await conn.get_snapshot(), self)
        # else:
        return TiKVTransaction(await conn.begin_transaction(), self)
