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

from typing import List, Optional
from uuid import uuid4

from nucliadb_protos.noderesources_pb2 import EmptyQuery
from nucliadb_protos.noderesources_pb2 import Shard as NodeResourcesShard
from nucliadb_protos.noderesources_pb2 import ShardCreated, ShardId
from nucliadb_protos.writer_pb2 import ShardObject as PBShard
from nucliadb_protos.writer_pb2 import ShardReplica
from nucliadb_protos.writer_pb2 import Shards as PBShards

from nucliadb_ingest.maindb.driver import Transaction
from nucliadb_ingest.orm import NODE_CLUSTER
from nucliadb.local_shard import LocalShard
from nucliadb_utils.keys import KB_SHARDS
import nucliadb_node_binding  # type: ignore


class LocalNode:
    writer: nucliadb_node_binding.NodeWriter
    reader: nucliadb_node_binding.NodeReader

    def __init__(self, folder: str):
        self.folder = folder
        self.writer = nucliadb_node_binding.NodeWriter.new()
        self.reader = nucliadb_node_binding.NodeReader.new()

    @classmethod
    async def create_shard_by_kbid(cls, txn: Transaction, kbid: str) -> LocalShard:
        node = NODE_CLUSTER.get_local_node()
        sharduuid = uuid4().hex
        shard = PBShard(shard=sharduuid)
        shard_created = await node.new_shard()
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
        kb_shards.shards.append(shard)
        kb_shards.actual += 1
        await txn.set(key, kb_shards.SerializeToString())

        return LocalShard(sharduuid=sharduuid, shard=shard, node=node)

    @classmethod
    async def actual_shard(cls, txn: Transaction, kbid: str) -> Optional[LocalShard]:
        key = KB_SHARDS.format(kbid=kbid)
        kb_shards_bytes: Optional[bytes] = await txn.get(key)
        if kb_shards_bytes is not None:
            kb_shards = PBShards()
            kb_shards.ParseFromString(kb_shards_bytes)
            shard: PBShard = kb_shards.shards[kb_shards.actual]
            return LocalShard(sharduuid=shard.shard, shard=shard)
        else:
            return None

    async def get_shard(self, id: str) -> ShardId:
        req = ShardId(id=id)
        resp = await self.writer.get_shard(req)  # type: ignore
        return resp

    async def get_reader_shard(self, id: str) -> NodeResourcesShard:
        req = ShardId(id=id)
        resp = await self.reader.get_shard(req)  # type: ignore
        return resp

    async def new_shard(self) -> ShardCreated:
        req = EmptyQuery()
        resp = await self.writer.new_shard(req)  # type: ignore
        return resp

    async def delete_shard(self, id: str) -> int:
        req = ShardId(id=id)
        resp = await self.writer.delete_shard(req, timeout=5)  # type: ignore
        return resp.id

    async def list_shards(self) -> List[str]:
        req = EmptyQuery()
        resp = await self.writer.list_shards(req)  # type: ignore
        return resp.shards

    async def add_resource(self, req):
        await self.writer.add_resource(req)

    async def delete_resource(self, req):
        await self.writer.delete_resource(req)
