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

import logging
from typing import Any, List, Optional

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
    "tikv_client", labels={"type": ""}, error_mappings={"conflict_error": ConflictError}
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

    async def get(self, key: str) -> Optional[bytes]:
        with tikv_observer({"type": "get"}):
            return await self.txn.get(key.encode())

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
        get_all_keys = count == -1
        limit = DEFAULT_BATCH_SCAN_LIMIT if get_all_keys else count
        start_key = match.encode()
        _include_start = include_start

        while True:
            with tikv_observer({"type": "scan_keys"}):
                keys = await self.txn.scan_keys(
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
                while left <= right:
                    mid = left + (right - left) // 2

                    if keys[mid].startswith(original_match):
                        left = mid + 1  # Move to the right half
                        result_index = mid
                    else:
                        right = mid - 1  # Move to the left half

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

    async def initialize(self):
        if self.initialized is False and self.tikv is None:
            self.tikv = await TransactionClient.connect(self.url)
        self.initialized = True

    async def finalize(self):
        pass

    async def begin(self) -> TiKVTransaction:
        if self.tikv is None:
            raise AttributeError()
        with tikv_observer({"type": "begin"}):
            txn = await self.tikv.begin(pessimistic=False)
        return TiKVTransaction(txn, driver=self)
