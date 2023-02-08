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

from typing import TYPE_CHECKING, Dict, Optional

from nucliadb_protos.noderesources_pb2 import Resource as PBBrainResource
from nucliadb_protos.noderesources_pb2 import ResourceID
from nucliadb_protos.noderesources_pb2 import ShardCleaned as PBShardCleaned
from nucliadb_protos.writer_pb2 import ShardObject as PBShard

from nucliadb.ingest.orm.abc import AbstractShard, ShardCounter

if TYPE_CHECKING:
    from nucliadb.ingest.orm.local_node import LocalNode


class LocalShard(AbstractShard):
    def __init__(self, sharduuid: str, shard: PBShard, node: LocalNode):
        self.shard = shard
        self.sharduuid = sharduuid
        self.node = node

    async def delete_resource(self, uuid: str, txid: int):
        req = ResourceID()
        req.uuid = uuid

        for shardreplica in self.shard.replicas:
            req.shard_id = shardreplica.shard.id
            await self.node.delete_resource(req)

    async def add_resource(
        self, resource: PBBrainResource, txid: int, reindex_id: Optional[str] = None
    ) -> Optional[ShardCounter]:
        for shardreplica in self.shard.replicas:
            resource.shard_id = resource.resource.shard_id = shardreplica.shard.id
            res = await self.node.add_resource(resource)
        # We don't have paragraphs returned from the writer SetResource, and
        # we also don't need them here, as they are used for auditing purposes
        return ShardCounter(shard=self.sharduuid, fields=res.count, paragraphs=0)

    async def clean_and_upgrade(self) -> Dict[str, PBShardCleaned]:
        shards_cleaned: Dict[str, PBShardCleaned] = {}
        for shardreplica in self.shard.replicas:
            replica_id = shardreplica.shard.id
            cleaned: PBShardCleaned = await self.node.clean_and_upgrade_shard(
                replica_id
            )
            shards_cleaned[replica_id] = cleaned
        return shards_cleaned
