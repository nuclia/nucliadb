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
from typing import AsyncIterator, List, Optional, Tuple

from nucliadb_protos.train_pb2 import (
    GetFieldsRequest,
    GetParagraphsRequest,
    GetResourcesRequest,
    GetSentencesRequest,
    TrainField,
    TrainParagraph,
    TrainResource,
    TrainSentence,
)
from nucliadb_protos.writer_pb2 import ShardObject
from nucliadb_protos.writer_pb2 import Shards as PBShards

from nucliadb.ingest.maindb.driver import Driver, Transaction
from nucliadb.ingest.orm import NODE_CLUSTER, NODES
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.node import Node
from nucliadb.ingest.orm.resource import KB_RESOURCE_SLUG_BASE
from nucliadb_utils.cache.utility import Cache
from nucliadb_utils.exceptions import ShardsNotFound
from nucliadb_utils.keys import KB_SHARDS
from nucliadb_utils.storages.storage import Storage


class TrainNodesManager:
    def __init__(self, driver: Driver, cache: Cache, storage: Storage):
        self.driver = driver
        self.cache = cache
        self.storage = storage

    async def get_reader(self, kbid: str, shard: str) -> Tuple[Node, str]:
        shards = await self.get_shards_by_kbid_inner(kbid)
        try:
            shard_object: ShardObject = next(
                filter(lambda x: x.shard == shard, shards.shards)
            )
        except StopIteration:
            raise KeyError("Shard not found")

        if NODE_CLUSTER.local_node:
            return (
                NODE_CLUSTER.get_local_node(),
                shard_object.replicas[0].shard.id,
            )
        nodes = [x for x in range(len(shard_object.replicas))]
        random.shuffle(nodes)
        node_obj = None
        shard_id = None
        for node in nodes:
            node_id = shard_object.replicas[node].node
            if node_id in NODES:
                node_obj = NODES[node_id]
                shard_id = shard_object.replicas[node].shard.id
                break

        if node_obj is None or node_id is None or shard_id is None:
            raise KeyError("Could not find a node to query")

        return node_obj, shard_id

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

    async def get_kb_obj(self, txn: Transaction, kbid: str) -> Optional[KnowledgeBox]:
        if kbid is None:
            return None

        if not (await KnowledgeBox.exist_kb(txn, kbid)):
            return None

        kbobj = KnowledgeBox(txn, self.storage, self.cache, kbid)
        return kbobj

    async def kb_sentences(
        self, request: GetSentencesRequest
    ) -> AsyncIterator[TrainSentence]:
        txn = await self.driver.begin()
        kb = KnowledgeBox(txn, self.storage, self.cache, request.kb.uuid)
        if request.uuid != "":
            # Filter by uuid
            resource = await kb.get(request.uuid)
            if resource:
                async for sentence in resource.iterate_sentences(request.metadata):
                    yield sentence
        else:
            async for resource in kb.iterate_resources():
                async for sentence in resource.iterate_sentences(request.metadata):
                    yield sentence
        await txn.abort()

    async def kb_paragraphs(
        self, request: GetParagraphsRequest
    ) -> AsyncIterator[TrainParagraph]:
        txn = await self.driver.begin()
        kb = KnowledgeBox(txn, self.storage, self.cache, request.kb.uuid)
        if request.uuid != "":
            # Filter by uuid
            resource = await kb.get(request.uuid)
            if resource:
                async for paragraph in resource.iterate_paragraphs(request.metadata):
                    yield paragraph
        else:
            async for resource in kb.iterate_resources():
                async for paragraph in resource.iterate_paragraphs(request.metadata):
                    yield paragraph
        await txn.abort()

    async def kb_fields(self, request: GetFieldsRequest) -> AsyncIterator[TrainField]:
        txn = await self.driver.begin()
        kb = KnowledgeBox(txn, self.storage, self.cache, request.kb.uuid)
        if request.uuid != "":
            # Filter by uuid
            resource = await kb.get(request.uuid)
            if resource:
                async for field in resource.iterate_fields(request.metadata):
                    yield field
        else:
            async for resource in kb.iterate_resources():
                async for field in resource.iterate_fields(request.metadata):
                    yield field
        await txn.abort()

    async def kb_resources(
        self, request: GetResourcesRequest
    ) -> AsyncIterator[TrainResource]:
        txn = await self.driver.begin()
        kb = KnowledgeBox(txn, self.storage, self.cache, request.kb.uuid)
        base = KB_RESOURCE_SLUG_BASE.format(kbid=request.kb.uuid)
        async for key in txn.keys(match=base, count=-1):
            # Fetch and Add wanted item
            rid = await txn.get(key)
            if rid is not None:
                resource = await kb.get(rid.decode())
                if resource is not None:
                    yield await resource.get_resource(request.metadata)

        await txn.abort()
