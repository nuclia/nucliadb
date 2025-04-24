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
from pydantic import BaseModel

from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos import knowledgebox_pb2 as kb_pb2
from nucliadb_protos import writer_pb2

from .utils import get_kv_pb, with_ro_transaction

logger = logging.getLogger(__name__)

KB_ROLLOVER_STATE = "/kbs/{kbid}/rollover/state"
KB_ROLLOVER_SHARDS = "/kbs/{kbid}/rollover/shards"
KB_ROLLOVER_EXTERNAL_INDEX_METADATA = "/kbs/{kbid}/rollover/external_index_metadata"
KB_ROLLOVER_RESOURCES_TO_INDEX = "/kbs/{kbid}/rollover/to-index/{resource}"
KB_ROLLOVER_RESOURCES_INDEXED = "/kbs/{kbid}/rollover/indexed/{resource}"


class RolloverState(BaseModel):
    rollover_shards_created: bool = False
    external_index_created: bool = False
    resources_scheduled: bool = False
    resources_indexed: bool = False
    cutover_shards: bool = False
    cutover_external_index: bool = False
    resources_validated: bool = False


class RolloverStateNotFoundError(Exception):
    """
    Raised when the rollover state is not found.
    """

    ...


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
        if shard_id == shard_obj.nidx_shard_id:
            return True
    return False


async def add_batch_to_index(txn: Transaction, *, kbid: str, batch: list[str]) -> None:
    for key in batch:
        key = KB_ROLLOVER_RESOURCES_TO_INDEX.format(kbid=kbid, resource=key)
        await txn.set(key, b"")


async def get_to_index(txn: Transaction, *, kbid: str, count: int) -> Optional[list[str]]:
    key = KB_ROLLOVER_RESOURCES_TO_INDEX.format(kbid=kbid, resource="")
    found = [key async for key in txn.keys(key, count=count)]
    if found:
        return [f.split("/")[-1] for f in found]
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
        shard_id: str = data[0]
        modification_time: int = data[1]
        return shard_id, modification_time
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
        async for key in txn.keys(match=start_key):
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
            shard_id: str
            modification_time: int
            shard_id, modification_time = orjson.loads(val)
            data = (shard_id, modification_time)
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


async def get_rollover_state(txn: Transaction, kbid: str) -> RolloverState:
    key = KB_ROLLOVER_STATE.format(kbid=kbid)
    val = await txn.get(key)
    if not val:
        raise RolloverStateNotFoundError(kbid)
    return RolloverState.model_validate_json(val)


async def set_rollover_state(txn: Transaction, kbid: str, state: RolloverState) -> None:
    key = KB_ROLLOVER_STATE.format(kbid=kbid)
    await txn.set(key, state.model_dump_json().encode())


async def clear_rollover_state(txn: Transaction, kbid: str) -> None:
    key = KB_ROLLOVER_STATE.format(kbid=kbid)
    await txn.delete(key)


async def update_kb_rollover_external_index_metadata(
    txn: Transaction, *, kbid: str, metadata: kb_pb2.StoredExternalIndexProviderMetadata
) -> None:
    key = KB_ROLLOVER_EXTERNAL_INDEX_METADATA.format(kbid=kbid)
    await txn.set(key, metadata.SerializeToString())


async def get_kb_rollover_external_index_metadata(
    txn: Transaction, *, kbid: str
) -> Optional[kb_pb2.StoredExternalIndexProviderMetadata]:
    key = KB_ROLLOVER_EXTERNAL_INDEX_METADATA.format(kbid=kbid)
    val = await txn.get(key)
    if not val:
        return None
    return kb_pb2.StoredExternalIndexProviderMetadata.FromString(val)


async def delete_kb_rollover_external_index_metadata(txn: Transaction, *, kbid: str) -> None:
    key = KB_ROLLOVER_EXTERNAL_INDEX_METADATA.format(kbid=kbid)
    await txn.delete(key)
