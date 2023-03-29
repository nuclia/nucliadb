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

from typing import Optional
from uuid import uuid4

from nucliadb_protos.noderesources_pb2 import Resource, ResourceID
from nucliadb_protos.nodewriter_pb2 import OpStatus
from nucliadb_protos.utils_pb2 import VectorSimilarity
from nucliadb_protos.writer_pb2 import ShardObject as PBShard
from nucliadb_protos.writer_pb2 import ShardReplica
from nucliadb_protos.writer_pb2 import Shards as PBShards

from nucliadb.ingest.maindb.driver import Transaction
from nucliadb.ingest.orm import NODE_CLUSTER
from nucliadb.ingest.orm.abc import AbstractNode
from nucliadb.ingest.orm.grpc_node_binding import LocalReaderWrapper, LocalWriterWrapper
from nucliadb.ingest.orm.local_shard import LocalShard
from nucliadb_utils.keys import KB_SHARDS


class LocalNode(AbstractNode):
    _writer: LocalWriterWrapper
    _reader: LocalReaderWrapper
    label: str = "local"

    def __init__(self):
        self._writer = LocalWriterWrapper()
        self._reader = LocalReaderWrapper()
        self.address = "local"

    @property
    def reader(self) -> LocalReaderWrapper:  # type: ignore
        return self._reader

    @property
    def writer(self) -> LocalWriterWrapper:  # type: ignore
        return self._writer

    @classmethod
    async def get(cls, _) -> LocalNode:
        return NODE_CLUSTER.get_local_node()

    @classmethod
    async def create_shard_by_kbid(
        cls, txn: Transaction, kbid: str, similarity: VectorSimilarity.ValueType
    ) -> LocalShard:
        node = NODE_CLUSTER.get_local_node()
        sharduuid = uuid4().hex
        shard = PBShard(shard=sharduuid)
        shard_created = await node.new_shard(kbid, similarity=similarity)
        sr = ShardReplica(node=str(node))
        sr.shard.CopyFrom(shard_created)
        shard.replicas.append(sr)

        key = KB_SHARDS.format(kbid=kbid)
        payload = await txn.get(key)
        kb_shards = PBShards()
        if payload is not None:
            kb_shards.ParseFromString(payload)
        else:
            kb_shards.kbid = kbid
            kb_shards.actual = -1
            kb_shards.similarity = similarity
        kb_shards.shards.append(shard)
        kb_shards.actual += 1
        await txn.set(key, kb_shards.SerializeToString())

        return LocalShard(sharduuid=sharduuid, shard=shard, node=node)

    @classmethod
    def create_shard_klass(cls, shard_id: str, pbshard: PBShard):
        node = NODE_CLUSTER.get_local_node()
        return LocalShard(sharduuid=shard_id, shard=pbshard, node=node)

    @classmethod
    async def get_current_active_shard(
        cls, txn: Transaction, kbid: str
    ) -> Optional[LocalShard]:
        key = KB_SHARDS.format(kbid=kbid)
        kb_shards_bytes: Optional[bytes] = await txn.get(key)
        if kb_shards_bytes is not None:
            kb_shards = PBShards()
            kb_shards.ParseFromString(kb_shards_bytes)
            shard: PBShard = kb_shards.shards[kb_shards.actual]
            node = NODE_CLUSTER.get_local_node()
            return LocalShard(sharduuid=shard.shard, shard=shard, node=node)
        else:
            return None

    async def add_resource(self, req: Resource) -> OpStatus:
        return await self.writer.SetResource(req)

    async def delete_resource(self, req: ResourceID) -> OpStatus:
        return await self.writer.RemoveResource(req)

    def __str__(self):
        return "LOCAL NODE"

    def __repr__(self):
        return "LOCAL NODE"
