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

from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos import writer_pb2

from .utils import get_kv_pb, with_ro_transaction

logger = logging.getLogger(__name__)

KB_ROLLOVER_SHARDS = "/kbs/{kbid}/rollover/shards"
KB_ROLLOVER_RESOURCES_TO_INDEX = "/kbs/{kbid}/rollover/to-index/{resource}"
KB_ROLLOVER_RESOURCES_INDEXED = "/kbs/{kbid}/rollover/indexed/{resource}"


async def get_kb_rollover_shards(txn: Transaction, *, kbid: str) -> Optional[writer_pb2.Shards]:
    key = KB_ROLLOVER_SHARDS.format(kbid=kbid)
    return await get_kv_pb(txn, key, writer_pb2.Shards)


async def update_kb_rollover_shards(
    txn: Transaction, *, kbid: str, kb_shards: writer_pb2.Shards
) -> None:
    key = KB_ROLLOVER_SHARDS.format(kbid=kbid)
    await txn.set(key, kb_shards.SerializeToString())


async def delete_kb_rollover_shards(txn: Transaction, *, kbid: str) -> None:
    key = KB_ROLLOVER_SHARDS.format(kbid=kbid)
    await txn.delete(key)


async def is_rollover_shard(txn: Transaction, *, kbid: str, shard_id: str) -> bool:
    shards = await get_kb_rollover_shards(txn, kbid=kbid)
    if shards is None:
        return False

    for shard_obj in shards.shards:
        for replica_obj in shard_obj.replicas:
            if shard_id == replica_obj.shard.id:
                return True
    return False


async def add_batch_to_index(txn: Transaction, *, kbid: str, batch: list[str]) -> None:
    for key in batch:
        key = KB_ROLLOVER_RESOURCES_TO_INDEX.format(kbid=kbid, resource=key)
        await txn.set(key, b"")


async def get_to_index(txn: Transaction, *, kbid: str) -> Optional[str]:
    key = KB_ROLLOVER_RESOURCES_TO_INDEX.format(kbid=kbid, resource="")
    found = [key async for key in txn.keys(key, count=1)]
    if found:
        return found[0].split("/")[-1]
    return None


async def remove_to_index(txn: Transaction, *, kbid: str, resource: str) -> None:
    to_index = KB_ROLLOVER_RESOURCES_TO_INDEX.format(kbid=kbid, resource=resource)
    await txn.delete(to_index)


async def add_indexed(
    txn: Transaction,
    *,
    kbid: str,
    resource_id: str,
    shard_id: str,
    modification_time: int,
) -> None:
    to_index = KB_ROLLOVER_RESOURCES_TO_INDEX.format(kbid=kbid, resource=resource_id)
    indexed = KB_ROLLOVER_RESOURCES_INDEXED.format(kbid=kbid, resource=resource_id)
    data = [shard_id, modification_time]
    await txn.delete(to_index)
    await txn.set(indexed, orjson.dumps(data))


async def get_indexed_data(
    txn: Transaction, *, kbid: str, resource_id: str
) -> Optional[tuple[str, int]]:
    key = KB_ROLLOVER_RESOURCES_INDEXED.format(kbid=kbid, resource=resource_id)
    val = await txn.get(key)
    if val is not None:
        data = orjson.loads(val)
        return tuple(data)
    return None


async def remove_indexed(txn: Transaction, *, kbid: str, batch: list[str]) -> None:
    for resource_id in batch:
        key = KB_ROLLOVER_RESOURCES_INDEXED.format(kbid=kbid, resource=resource_id)
        await txn.delete(key)


async def iter_indexed_keys(*, kbid: str) -> AsyncGenerator[str, None]:
    """
    For TiKV internally `keys` is not part of this transaction so this is
    internally managed
    """
    start_key = KB_ROLLOVER_RESOURCES_INDEXED.format(kbid=kbid, resource="")
    async with with_ro_transaction() as txn:
        async for key in txn.keys(match=start_key, count=-1):
            yield key.split("/")[-1]


async def _get_batch_indexed_data(*, kbid, batch: list[str]) -> list[tuple[str, tuple[str, int]]]:
    async with with_ro_transaction() as txn:
        values = await txn.batch_get(
            [
                KB_ROLLOVER_RESOURCES_INDEXED.format(kbid=kbid, resource=resource_id)
                for resource_id in batch
            ]
        )
    results: list[tuple[str, tuple[str, int]]] = []
    for key, val in zip(batch, values):
        if val is not None:
            data: tuple[str, int] = tuple(orjson.loads(val))
            results.append((key.split("/")[-1], data))
    return results


async def iterate_indexed_data(*, kbid: str) -> AsyncGenerator[tuple[str, tuple[str, int]], None]:
    """
    This function is optimized for reducing the time a transaction is open.

    For this reason, it is not using the `txn` argument passed in.
    """
    batch = []

    async for resource_id in iter_indexed_keys(kbid=kbid):
        batch.append(resource_id)
        if len(batch) >= 200:
            for key, val in await _get_batch_indexed_data(kbid=kbid, batch=batch):
                yield key, val
            batch = []
    if len(batch) > 0:
        for key, val in await _get_batch_indexed_data(kbid=kbid, batch=batch):
            yield key, val
