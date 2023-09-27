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
from typing import Any, List, Optional

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
    from tikv_client.asynchronous import TransactionClient  # type: ignore

    TiKV = True
except ImportError:  # pragma: no cover
    TiKV = False

tikv_observer = metrics.Observer(
    "tikv_client",
    labels={"type": ""},
    error_mappings={"conflict_error": ConflictError, "timeout_error": TimeoutError},
)
logger = logging.getLogger(__name__)


class TiKVTransaction(Transaction):
    driver: TiKVDriver

    def __init__(self, txn: Any, driver: TiKVDriver):
        self.txn = txn
        self.driver = driver
        self.open = True

    async def abort(self):
        if not self.open:
            return

        with tikv_observer({"type": "rollback"}):
            try:
                await self.txn.rollback()
            except Exception:
                logger.exception("Error rolling back transaction")
        self.open = False

    async def commit(self):
        with tikv_observer({"type": "commit"}):
            try:
                await self.txn.commit()
            except Exception as exc:
                exc_text = str(exc)
                if "WriteConflict" in exc_text:
                    raise ConflictError(exc_text) from exc
                else:
                    raise
        self.open = False

    async def batch_get(self, keys: List[str]):
        bytes_keys: List[bytes] = [x.encode() for x in keys]
        with tikv_observer({"type": "batch_get"}):
            return await self.txn.batch_get(bytes_keys)

    @backoff.on_exception(backoff.expo, (TimeoutError,), max_tries=2)
    async def get(self, key: str) -> Optional[bytes]:
        with tikv_observer({"type": "get"}):
            try:
                return await self.txn.get(key.encode())
            except Exception as exc:
                # The tikv_client library does not provide specific exceptions and simply
                # raises generic Exception class with different error strings. That forces
                # us to parse the error string to determine the type of error...
                exc_text = str(exc)
                if "4-DEADLINE_EXCEEDED" in exc_text:
                    raise TimeoutError(exc_text) from exc
                else:
                    raise

    async def set(self, key: str, value: bytes):
        with tikv_observer({"type": "put"}):
            await self.txn.put(key.encode(), value)

    async def delete(self, key: str):
        with tikv_observer({"type": "delete"}):
            await self.txn.delete(key.encode())

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
        assert self.driver.tikv is not None
        with tikv_observer({"type": "begin"}):
            # We need to create a new transaction
            # this seems weird but is necessary with current usage of tikv_client
            txn = await self.driver.tikv.begin(pessimistic=False)

        get_all_keys = count == -1
        limit = DEFAULT_BATCH_SCAN_LIMIT if get_all_keys else count
        start_key = match.encode()
        _include_start = include_start

        try:
            while True:
                with tikv_observer({"type": "scan_keys"}):
                    keys = await txn.scan_keys(
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
        finally:
            with tikv_observer({"type": "rollback"}):
                await txn.rollback()

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
            with tikv_observer({"type": "scan_keys"}):
                keys = await self.txn.scan_keys(
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


class TiKVDriver(Driver):
    tikv = None

    def __init__(self, url: List[str]):
        if TiKV is False:
            raise ImportError("TiKV is not installed")
        self.url = url
        self.connect_lock = asyncio.Lock()
        self.connect_in_progress = False

    async def initialize(self):
        async with self.connect_lock:
            if self.initialized is False and self.tikv is None:
                self.tikv = await TransactionClient.connect(self.url)
            self.initialized = True

    async def reinitialize(self) -> None:
        if self.connect_in_progress:
            async with self.connect_lock:
                # wait for lock and then just continue because someone else is establishing the connection
                return
        else:
            self.connect_in_progress = True
            try:
                async with self.connect_lock:
                    logger.warning("Reconnecting to TiKV")
                    self.tikv = await TransactionClient.connect(self.url)
            finally:
                self.connect_in_progress = False

    async def finalize(self):
        pass

    async def begin(self) -> TiKVTransaction:
        if self.tikv is None:
            raise AttributeError()
        with tikv_observer({"type": "begin"}):
            try:
                txn = await self.tikv.begin(pessimistic=False)
            except Exception as exc:
                if "failed to connect to" not in str(exc):
                    # NO exception handling in client, this is ugly but the best
                    # we have right now
                    # Exception looks like this:
                    # Exception: [//client-rust-5a1ccd35a54db20f/eb1d2da/tikv-client-pd/src/cluster.rs:264]:
                    #   failed to connect to
                    #   [Member { name: "pd", member_id: 3474484975246189105, peer_urls: ["http://127.0.0.1:2380"],
                    #             client_urls: ["http://127.0.0.1:2379"], leader_priority: 0, deploy_path: "",
                    #             binary_version: "", git_hash: "", dc_location: "" }]
                    raise
                # attempt to reconnect once per driver so we need to deal with locks
                await self.reinitialize()
                txn = await self.tikv.begin(pessimistic=False)

        return TiKVTransaction(txn, driver=self)
