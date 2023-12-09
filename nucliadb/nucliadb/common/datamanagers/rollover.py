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
import logging
from typing import AsyncGenerator, Optional

import orjson

from nucliadb.common.maindb.driver import Driver
from nucliadb_protos import writer_pb2

from .utils import get_kv_pb

logger = logging.getLogger(__name__)

KB_ROLLOVER_SHARDS = "/kbs/{kbid}/rollover/shards"
KB_ROLLOVER_RESOURCES_TO_INDEX = "/kbs/{kbid}/rollover/to-index/{resource}"
KB_ROLLOVER_RESOURCES_INDEXED = "/kbs/{kbid}/rollover/indexed/{resource}"


class RolloverDataManager:
    def __init__(self, driver: Driver):
        self.driver = driver

    async def get_kb_rollover_shards(self, kbid: str) -> Optional[writer_pb2.Shards]:
        key = KB_ROLLOVER_SHARDS.format(kbid=kbid)
        async with self.driver.transaction(wait_for_abort=False) as txn:
            return await get_kv_pb(txn, key, writer_pb2.Shards)

    async def update_kb_rollover_shards(
        self, kbid: str, kb_shards: writer_pb2.Shards
    ) -> None:
        key = KB_ROLLOVER_SHARDS.format(kbid=kbid)
        async with self.driver.transaction() as txn:
            await txn.set(key, kb_shards.SerializeToString())
            await txn.commit()

    async def delete_kb_rollover_shards(self, kbid: str) -> None:
        key = KB_ROLLOVER_SHARDS.format(kbid=kbid)
        async with self.driver.transaction() as txn:
            await txn.delete(key)
            await txn.commit()

    async def add_batch_to_index(self, kbid: str, batch: list[str]) -> None:
        async with self.driver.transaction() as txn:
            for key in batch:
                key = KB_ROLLOVER_RESOURCES_TO_INDEX.format(kbid=kbid, resource=key)
                await txn.set(key, b"")
            await txn.commit()

    async def get_to_index(self, kbid: str) -> Optional[str]:
        key = KB_ROLLOVER_RESOURCES_TO_INDEX.format(kbid=kbid, resource="")
        async with self.driver.transaction(wait_for_abort=False) as txn:
            found = [key async for key in txn.keys(key, count=1)]
            if found:
                return found[0].split("/")[-1]
        return None

    async def remove_to_index(self, kbid: str, resource: str) -> None:
        to_index = KB_ROLLOVER_RESOURCES_TO_INDEX.format(kbid=kbid, resource=resource)
        async with self.driver.transaction() as txn:
            await txn.delete(to_index)
            await txn.commit()

    async def add_indexed(
        self, kbid: str, resource_id: str, shard_id: str, modification_time: int
    ) -> None:
        to_index = KB_ROLLOVER_RESOURCES_TO_INDEX.format(
            kbid=kbid, resource=resource_id
        )
        indexed = KB_ROLLOVER_RESOURCES_INDEXED.format(kbid=kbid, resource=resource_id)
        data = [shard_id, modification_time]
        async with self.driver.transaction() as txn:
            await txn.delete(to_index)
            await txn.set(indexed, orjson.dumps(data))
            await txn.commit()

    async def get_indexed_data(
        self, kbid: str, resource_id: str
    ) -> Optional[tuple[str, int]]:
        key = KB_ROLLOVER_RESOURCES_INDEXED.format(kbid=kbid, resource=resource_id)
        async with self.driver.transaction(wait_for_abort=False) as txn:
            val = await txn.get(key)
            if val is not None:
                data = orjson.loads(val)
                return tuple(data)  # type: ignore
        return None

    async def remove_indexed(self, kbid: str, batch: list[str]) -> None:
        async with self.driver.transaction(wait_for_abort=False) as txn:
            for resource_id in batch:
                key = KB_ROLLOVER_RESOURCES_INDEXED.format(
                    kbid=kbid, resource=resource_id
                )
                await txn.delete(key)
            await txn.commit()

    async def iter_indexed_keys(self, kbid: str) -> AsyncGenerator[str, None]:
        start_key = KB_ROLLOVER_RESOURCES_INDEXED.format(kbid=kbid, resource="")
        async with self.driver.transaction(wait_for_abort=False) as txn:
            async for key in txn.keys(match=start_key, count=-1):
                yield key.split("/")[-1]

    async def _get_batch_indexed_data(
        self, kbid, batch: list[str]
    ) -> list[tuple[str, tuple[str, int]]]:
        async with self.driver.transaction(wait_for_abort=False) as txn:
            values = await txn.batch_get(
                [
                    KB_ROLLOVER_RESOURCES_INDEXED.format(
                        kbid=kbid, resource=resource_id
                    )
                    for resource_id in batch
                ]
            )
        results: list[tuple[str, tuple[str, int]]] = []
        for key, val in zip(batch, values):
            if val is not None:
                data: tuple[str, int] = tuple(orjson.loads(val))  # type: ignore
                results.append((key.split("/")[-1], data))
        return results

    async def iterate_indexed_data(
        self, kbid: str
    ) -> AsyncGenerator[tuple[str, tuple[str, int]], None]:
        batch = []
        async for resource_id in self.iter_indexed_keys(kbid):
            batch.append(resource_id)
            if len(batch) >= 200:
                for key, val in await self._get_batch_indexed_data(kbid, batch):
                    yield key, val
                batch = []
        if len(batch) > 0:
            for key, val in await self._get_batch_indexed_data(kbid, batch):
                yield key, val
