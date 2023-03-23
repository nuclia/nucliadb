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

from typing import Any, List, Optional

from nucliadb.ingest.maindb.driver import (
    DEFAULT_BATCH_SCAN_LIMIT,
    DEFAULT_SCAN_LIMIT,
    TXNID,
    Driver,
    Transaction,
)
from nucliadb.ingest.maindb.exceptions import NoWorkerCommit
from nucliadb_telemetry import metrics

try:
    from tikv_client.asynchronous import TransactionClient  # type: ignore

    TiKV = True
except ImportError:  # pragma: no cover
    TiKV = False

tikv_observer = metrics.Observer("tikv_client", labels={"type": ""})


class TiKVTransaction(Transaction):
    driver: TiKVDriver

    def __init__(self, txn: Any, driver: TiKVDriver):
        self.txn = txn
        self.driver = driver
        self.open = True

    async def abort(self):
        with tikv_observer({"type": "rollback"}):
            await self.txn.rollback()
        self.open = False

    async def commit(
        self,
        worker: Optional[str] = None,
        tid: Optional[int] = None,
        resource: bool = True,
    ):
        if resource:
            if worker is None or tid is None:
                raise NoWorkerCommit()
            if tid != -1:
                # If tid == -1 means we are injecting a resource via gRPC. We don't
                # want to save the tid in this case, as it would break the next
                # transactionability check.
                key = TXNID.format(worker=worker)
                with tikv_observer({"type": "put"}):
                    await self.txn.put(key.encode(), str(tid).encode())

        with tikv_observer({"type": "commit"}):
            await self.txn.commit()
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
        assert self.driver.tikv is not None
        with tikv_observer({"type": "begin"}):
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
            await txn.rollback()


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
        return TiKVTransaction(await self.tikv.begin(pessimistic=False), driver=self)

    async def keys(
        self, match: str, count: int = DEFAULT_SCAN_LIMIT, include_start: bool = True
    ):
        """
        Get keys from tikv, up to a configurable limit.

        Use -1 as the count of objects keep iterating in batches
        until all matching keys are retrieved.
        With any other count, only up to count keys will be returned.
        """
        titxn: TiKVTransaction = await self.begin()

        get_all_keys = count == -1
        limit = DEFAULT_BATCH_SCAN_LIMIT if get_all_keys else count
        start_key = match.encode()
        _include_start = include_start

        while True:
            keys = await titxn.txn.scan_keys(
                start=start_key, end=None, limit=limit, include_start=_include_start
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

        await titxn.abort()
