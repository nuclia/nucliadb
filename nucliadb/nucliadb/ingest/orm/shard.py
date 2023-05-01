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

import uuid
from typing import Any, Dict, List, Optional, Tuple

from lru import LRU  # type: ignore
from nucliadb_protos.noderesources_pb2 import Resource as PBBrainResource
from nucliadb_protos.noderesources_pb2 import ShardCleaned as PBShardCleaned
from nucliadb_protos.noderesources_pb2 import ShardId
from nucliadb_protos.nodesidecar_pb2 import Counter
from nucliadb_protos.nodewriter_pb2 import IndexMessage, TypeMessage
from nucliadb_protos.writer_pb2 import ShardObject as PBShard

from nucliadb.ingest import SERVICE_NAME  # type: ignore
from nucliadb.ingest.orm import NODES
from nucliadb.ingest.orm.abc import AbstractShard, ShardCounter
from nucliadb_telemetry import errors
from nucliadb_utils.utilities import get_indexing, get_storage

SHARDS = LRU(100)


class Shard(AbstractShard):
    def __init__(self, sharduuid: str, shard: PBShard, node: Optional[Any] = None):
        self.shard = shard
        self.sharduuid = sharduuid
        self.node = node

    def indexing_replicas(self) -> List[Tuple[str, str]]:
        """
        Returns the replica ids and nodes for the shard replicas
        """
        result = []
        for replica in self.shard.replicas:
            result.append((replica.shard.id, replica.node))
        return result

    async def delete_resource(self, uuid: str, txid: int, partition: str, kb: str):
        indexing = get_indexing()
        storage = await get_storage(service_name=SERVICE_NAME)

        await storage.delete_indexing(
            resource_uid=uuid, txid=txid, kb=kb, logical_shard=self.sharduuid
        )

        for replica_id, node_id in self.indexing_replicas():
            indexpb: IndexMessage = IndexMessage()
            indexpb.node = node_id
            indexpb.shard = replica_id
            indexpb.txid = txid
            indexpb.resource = uuid
            indexpb.typemessage = TypeMessage.DELETION
            indexpb.partition = partition
            indexpb.kbid = kb
            await indexing.index(indexpb, node_id)

    async def add_resource(
        self,
        resource: PBBrainResource,
        txid: int,
        partition: str,
        kb: str,
        reindex_id: Optional[str] = None,
    ) -> Optional[ShardCounter]:
        if txid == -1 and reindex_id is None:
            # This means we are injecting a complete resource via ingest gRPC
            # outside of a transaction. We need to treat this as a reindex operation.
            reindex_id = uuid.uuid4().hex

        storage = await get_storage(service_name=SERVICE_NAME)
        indexing = get_indexing()

        indexpb: IndexMessage

        if reindex_id is not None:
            indexpb = await storage.reindexing(
                resource, reindex_id, partition, kb=kb, logical_shard=self.sharduuid
            )
        else:
            indexpb = await storage.indexing(
                resource, txid, partition, kb=kb, logical_shard=self.sharduuid
            )

        shard_counter: Optional[ShardCounter] = None
        for replica_id, node_id in self.indexing_replicas():
            indexpb.node = node_id
            indexpb.shard = replica_id
            await indexing.index(indexpb, node_id)
            try:
                counter: Counter = await NODES[node_id].sidecar.GetCount(ShardId(id=replica_id))  # type: ignore
                shard_counter = ShardCounter(
                    shard=self.sharduuid,
                    # even though counter returns a `resources` value here,
                    # `resources` here from a shard is actually more like `fields`
                    fields=counter.resources,
                    paragraphs=counter.paragraphs,
                )
            except Exception as exc:
                errors.capture_exception(exc)
        return shard_counter

    async def clean_and_upgrade(self) -> Dict[str, PBShardCleaned]:
        replicas_cleaned: Dict[str, PBShardCleaned] = {}
        for shardreplica in self.shard.replicas:
            node = NODES[shardreplica.node]
            replica_id = shardreplica.shard
            cleaned = await node.writer.CleanAndUpgradeShard(replica_id)  # type: ignore
            replicas_cleaned[replica_id.id] = cleaned
        return replicas_cleaned
