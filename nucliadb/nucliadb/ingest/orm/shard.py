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

from typing import Any, Dict, Optional

from lru import LRU  # type: ignore
from nucliadb_protos.noderesources_pb2 import (
    Resource as PBBrainResource,  # type: ignore
)
from nucliadb_protos.noderesources_pb2 import ShardCleaned as PBShardCleaned
from nucliadb_protos.nodewriter_pb2 import Counter, IndexMessage
from nucliadb_protos.writer_pb2 import ShardObject as PBShard

from nucliadb.ingest import SERVICE_NAME, logger  # type: ignore
from nucliadb.ingest.orm import NODES
from nucliadb.ingest.orm.abc import AbstractShard
from nucliadb_utils.utilities import get_indexing, get_storage

SHARDS = LRU(100)


class Shard(AbstractShard):
    def __init__(self, sharduuid: str, shard: PBShard, node: Optional[Any] = None):
        self.shard = shard
        self.sharduuid = sharduuid
        self.node = node

    async def delete_resource(self, uuid: str, txid: int):

        indexing = get_indexing()

        for shardreplica in self.shard.replicas:
            indexpb: IndexMessage = IndexMessage()
            indexpb.node = shardreplica.node
            indexpb.shard = shardreplica.shard.id
            indexpb.txid = txid
            indexpb.resource = uuid
            indexpb.typemessage = IndexMessage.TypeMessage.DELETION
            await indexing.index(indexpb, shardreplica.node)

    async def add_resource(
        self, resource: PBBrainResource, txid: int, reindex_id: Optional[str] = None
    ) -> int:

        storage = await get_storage(service_name=SERVICE_NAME)
        indexing = get_indexing()

        count: int = -1
        indexpb: IndexMessage

        for shardreplica in self.shard.replicas:
            resource.shard_id = (
                resource.resource.shard_id
            ) = shard = shardreplica.shard.id
            if reindex_id is not None:
                logger.info(f"Starting storage.reindexing at shard {shard}")
                indexpb = await storage.reindexing(
                    resource, shardreplica.node, shard, reindex_id
                )
                logger.info(f"Finished storage.reindexing at shard {shard}")
            else:
                indexpb = await storage.indexing(
                    resource, shardreplica.node, shard, txid
                )

            logger.info(f"Starting indexing.indexing at shard {shard}")
            await indexing.index(indexpb, shardreplica.node)
            logger.info(f"Finished indexing.indexing at shard {shard}")

            try:
                res: Counter = await NODES[shardreplica.node].sidecar.GetCount(shardreplica.shard)  # type: ignore
                if count < res.resources:
                    count = res.resources
            except Exception:
                pass

        return count

    async def clean_and_upgrade(self) -> Dict[str, PBShardCleaned]:
        replicas_cleaned: Dict[str, PBShardCleaned] = {}
        for shardreplica in self.shard.replicas:
            node = NODES[shardreplica.node]
            replica_id = shardreplica.shard
            cleaned = await node.writer.CleanAndUpgradeShard(replica_id)  # type: ignore
            replicas_cleaned[replica_id.id] = cleaned
        return replicas_cleaned
