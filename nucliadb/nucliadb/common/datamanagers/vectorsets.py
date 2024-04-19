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
from typing import Optional

from nucliadb.common.cluster.manager import iterate_kb_nodes
from nucliadb.common.maindb.driver import Transaction
from nucliadb.vectorsets.models import VectorSets

KB_VECTORSET = "/kbs/{kbid}/vectorsets2"


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
) -> None:
    async for node, shard_id in iterate_kb_nodes(kbid=kbid):
        await node.create_vectorset(
            shard_id=shard_id,
            vector_dimension=semantic_vector_dimension,
            similarity=semantic_vector_similarity,
        )


async def delete_vectorset_indexes(*, kbid: str, vectorset_id: str) -> None:
    """
    Delete the hnsw indexes for the vectorset from all shards.
    """
    async for node, shard_replica_id in iterate_kb_nodes(kbid=kbid):
        await node.del_vectorset(shard_id=shard_replica_id, vectorset=vectorset_id)
