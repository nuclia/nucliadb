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
from typing import AsyncIterator, Optional

import backoff

from nucliadb.common import datamanagers
from nucliadb.common.cluster.base import AbstractIndexNode
from nucliadb.common.cluster.exceptions import ShardsNotFound
from nucliadb.common.cluster.manager import get_index_node
from nucliadb.common.maindb.driver import Transaction
from nucliadb.vectorsets.models import VectorSetHNSWIndex, VectorSets

KB_VECTORSET = "/kbs/{kbid}/vectorsets2"


class NodeNotFoundError(Exception):
    pass


async def get_vectorsets_kv(txn: Transaction, *, kbid: str) -> Optional[VectorSets]:
    key = KB_VECTORSET.format(kbid=kbid)
    vectorsets_bytes: Optional[bytes] = await txn.get(key)
    if not vectorsets_bytes:
        return None
    return VectorSets.parse_raw(vectorsets_bytes)


async def set_vectorsets_kv(txn: Transaction, *, kbid: str, vectorsets: VectorSets):
    key = KB_VECTORSET.format(kbid=kbid)
    await txn.set(key, vectorsets.json().encode())


async def create_vectorset_indexes(
    *,
    kbid: str,
    semantic_vector_dimension: int,
    semantic_vector_similarity: str,
) -> list[VectorSetHNSWIndex]:
    result = []
    async for node, shard_id in iterate_kb_nodes(kbid=kbid):
        hnsw_index_id = await node.create_vectorset(
            shard_id=shard_id,
            vector_dimension=semantic_vector_dimension,
            similarity=semantic_vector_similarity,
        )
        result.append(
            VectorSetHNSWIndex(
                node_id=node.id,
                shard_replica_id=shard_id,
                hnsw_index_id=hnsw_index_id,
            )
        )
    return result


async def delete_vectorset_indexes(
    *, vectorset_indexes: list[VectorSetHNSWIndex]
) -> None:
    for hnsw_index in vectorset_indexes:
        node = get_index_node_retried(hnsw_index.node_id)
        await node.del_vectorset(
            shard_id=hnsw_index.shard_replica_id, vectorset=hnsw_index.hnsw_index_id
        )


@backoff.on_exception(
    backoff.expo, NodeNotFoundError, jitter=backoff.random_jitter, max_tries=4
)
def get_index_node_retried(node_id: str) -> AbstractIndexNode:
    node = get_index_node(node_id)
    if node is None:
        raise NodeNotFoundError(f"Node {node_id} not found")
    return node


async def iterate_kb_nodes(
    *, kbid: str
) -> AsyncIterator[tuple[AbstractIndexNode, str]]:
    async with datamanagers.with_transaction(read_only=True) as txn:
        shards_obj = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
        if shards_obj is None:
            raise ShardsNotFound(kbid)
        for shard in shards_obj.shards:
            for replica in shard.replicas:
                node = get_index_node_retried(replica.node)
                yield node, replica.shard.id
