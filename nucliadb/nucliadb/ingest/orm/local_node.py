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

from nucliadb.ingest.maindb.driver import Transaction
from nucliadb.ingest.orm import NODE_CLUSTER
from nucliadb.ingest.orm.abc import AbstractNode
from nucliadb.ingest.orm.grpc_node_binding import LocalReaderWrapper, LocalWriterWrapper
from nucliadb.ingest.orm.local_shard import LocalShard
from nucliadb_protos import (
    nodereader_pb2,
    noderesources_pb2,
    nodesidecar_pb2,
    nodewriter_pb2,
    utils_pb2,
    writer_pb2,
)
from nucliadb_utils.keys import KB_SHARDS


class LocalNodeSidecarInterface:
    """
    backward compatibile interface for sidecar
    type interactions when running standalone.

    Right now, side car only provides cached counters.

    Long term, this should be removed and any caching
    should be done at the node reader.
    """

    def __init__(self, reader: LocalReaderWrapper):
        self._reader = reader

    async def GetCount(
        self, shard_id: noderesources_pb2.ShardId
    ) -> nodesidecar_pb2.Counter:
        shard = await self._reader.GetShard(
            nodereader_pb2.GetShardRequest(shard_id=shard_id)
        )
        response = nodesidecar_pb2.Counter()
        if shard is not None:
            response.resources = shard.resources
            response.paragraphs = shard.paragraphs
        return response


class LocalNode(AbstractNode):
    _writer: LocalWriterWrapper
    _reader: LocalReaderWrapper
    label: str = "local"

    def __init__(self):
        self._writer = LocalWriterWrapper()
        self._reader = LocalReaderWrapper()
        self.address = "local"
        self.sidecar = LocalNodeSidecarInterface(self._reader)

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
        cls,
        txn: Transaction,
        kbid: str,
        similarity: utils_pb2.VectorSimilarity.ValueType,
    ) -> LocalShard:
        node = NODE_CLUSTER.get_local_node()
        sharduuid = uuid4().hex
        shard = writer_pb2.ShardObject(shard=sharduuid)
        shard_created = await node.new_shard(kbid, similarity=similarity)
        sr = writer_pb2.ShardReplica(node=str(node))
        sr.shard.CopyFrom(shard_created)
        shard.replicas.append(sr)

        key = KB_SHARDS.format(kbid=kbid)
        payload = await txn.get(key)
        kb_shards = writer_pb2.Shards()
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
    def create_shard_klass(cls, shard_id: str, pbshard: writer_pb2.ShardObject):
        node = NODE_CLUSTER.get_local_node()
        return LocalShard(sharduuid=shard_id, shard=pbshard, node=node)

    @classmethod
    async def get_current_active_shard(
        cls, txn: Transaction, kbid: str
    ) -> Optional[LocalShard]:
        key = KB_SHARDS.format(kbid=kbid)
        kb_shards_bytes: Optional[bytes] = await txn.get(key)
        if kb_shards_bytes is not None:
            kb_shards = writer_pb2.Shards()
            kb_shards.ParseFromString(kb_shards_bytes)
            shard: writer_pb2.ShardObject = kb_shards.shards[kb_shards.actual]
            node = NODE_CLUSTER.get_local_node()
            return LocalShard(sharduuid=shard.shard, shard=shard, node=node)
        else:
            return None

    async def add_resource(
        self, req: noderesources_pb2.Resource
    ) -> nodewriter_pb2.OpStatus:
        return await self.writer.SetResource(req)

    async def delete_resource(
        self, req: noderesources_pb2.ResourceID
    ) -> nodewriter_pb2.OpStatus:
        return await self.writer.RemoveResource(req)

    def __str__(self):
        return "LOCAL NODE"

    def __repr__(self):
        return "LOCAL NODE"
