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
import random
from typing import List, Optional, Tuple

from nucliadb_protos.writer_pb2 import ShardObject
from nucliadb_protos.writer_pb2 import Shards as PBShards

from nucliadb.ingest.maindb.driver import Driver
from nucliadb.ingest.orm import NODE_CLUSTER, NODES
from nucliadb.ingest.orm.node import Node
from nucliadb_utils.exceptions import ShardsNotFound
from nucliadb_utils.keys import KB_SHARDS


class NodesManager:
    def __init__(self, driver: Driver, cache):
        self.driver = driver
        self.cache = cache

    async def get_shards_by_kbid_inner(self, kbid: str) -> PBShards:
        key = KB_SHARDS.format(kbid=kbid)
        txn = await self.driver.begin()
        payload = await txn.get(key)
        await txn.abort()
        if payload is None:
            # could be None because /shards doesn't exist, or beacause the whole KB does not exist.
            # In any case, this should not happen
            raise ShardsNotFound(kbid)

        pb = PBShards()
        pb.ParseFromString(payload)
        return pb

    async def get_shards_by_kbid(self, kbid: str) -> List[ShardObject]:
        shards = await self.get_shards_by_kbid_inner(kbid)
        return [x for x in shards.shards]

    def choose_node(
        self, shard: ShardObject, shards: Optional[List[str]] = None
    ) -> Tuple[Node, Optional[str], str]:
        shards = shards or []

        if NODE_CLUSTER.local_node:
            return (
                NODE_CLUSTER.get_local_node(),
                shard.replicas[0].shard.id,
                shard.replicas[0].node,
            )
        nodes = [x for x in range(len(shard.replicas))]
        random.shuffle(nodes)
        node_obj = None
        shard_id = None
        for node in nodes:
            node_id = shard.replicas[node].node
            if node_id in NODES:
                node_obj = NODES[node_id]
                shard_id = shard.replicas[node].shard.id
                if len(shards) > 0 and shard_id not in shards:
                    node_obj = None
                    shard_id = None
                else:
                    break

        if node_obj is None or node_id is None:
            raise KeyError("Could not find a node to query")

        return node_obj, shard_id, node_id
