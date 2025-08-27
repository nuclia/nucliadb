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

from nucliadb.common import datamanagers
from nucliadb.common.cluster import manager

# XXX: this keys shouldn't be exposed outside datamanagers
from nucliadb.common.datamanagers.resources import KB_RESOURCE_SLUG_BASE
from nucliadb.common.maindb.driver import Driver, Transaction
from nucliadb.ingest.orm.entities import EntitiesManager
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.train.resource import (
    generate_train_resource,
    iterate_fields,
    iterate_paragraphs,
    iterate_sentences,
)
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
from nucliadb_utils.storages.storage import Storage


class TrainShardManager(manager.KBShardManager):
    def __init__(self, driver: Driver, storage: Storage):
        super().__init__()
        self.driver = driver
        self.storage = storage

    async def get_shard_id(self, kbid: str, shard: str) -> str:
        shards = await self.get_shards_by_kbid_inner(kbid)
        try:
            shard_object: ShardObject = next(filter(lambda x: x.shard == shard, shards.shards))
        except StopIteration:
            raise KeyError("Shard not found")

        return shard_object.nidx_shard_id

    async def get_kb_obj(self, txn: Transaction, kbid: str) -> Optional[KnowledgeBox]:
        if kbid is None:
            return None

        if not (await datamanagers.kb.exists_kb(txn, kbid=kbid)):
            return None

        kbobj = KnowledgeBox(txn, self.storage, kbid)
        return kbobj

    async def get_kb_entities_manager(self, txn: Transaction, kbid: str) -> Optional[EntitiesManager]:
        kbobj = await self.get_kb_obj(txn, kbid)
        if kbobj is None:
            return None

        manager = EntitiesManager(kbobj, txn)
        return manager

    async def kb_sentences(self, request: GetSentencesRequest) -> AsyncIterator[TrainSentence]:
        async with self.driver.ro_transaction() as txn:
            kb = KnowledgeBox(txn, self.storage, request.kb.uuid)
            if request.uuid != "":
                # Filter by uuid
                resource = await kb.get(request.uuid)
                if resource:
                    async for sentence in iterate_sentences(resource, request.metadata):
                        yield sentence
            else:
                async for resource in kb.iterate_resources():
                    async for sentence in iterate_sentences(resource, request.metadata):
                        yield sentence

    async def kb_paragraphs(self, request: GetParagraphsRequest) -> AsyncIterator[TrainParagraph]:
        async with self.driver.ro_transaction() as txn:
            kb = KnowledgeBox(txn, self.storage, request.kb.uuid)
            if request.uuid != "":
                # Filter by uuid
                resource = await kb.get(request.uuid)
                if resource:
                    async for paragraph in iterate_paragraphs(resource, request.metadata):
                        yield paragraph
            else:
                async for resource in kb.iterate_resources():
                    async for paragraph in iterate_paragraphs(resource, request.metadata):
                        yield paragraph

    async def kb_fields(self, request: GetFieldsRequest) -> AsyncIterator[TrainField]:
        async with self.driver.ro_transaction() as txn:
            kb = KnowledgeBox(txn, self.storage, request.kb.uuid)
            if request.uuid != "":
                # Filter by uuid
                resource = await kb.get(request.uuid)
                if resource:
                    async for field in iterate_fields(resource, request.metadata):
                        yield field
            else:
                async for resource in kb.iterate_resources():
                    async for field in iterate_fields(resource, request.metadata):
                        yield field

    async def kb_resources(self, request: GetResourcesRequest) -> AsyncIterator[TrainResource]:
        async with self.driver.ro_transaction() as txn:
            kb = KnowledgeBox(txn, self.storage, request.kb.uuid)
            base = KB_RESOURCE_SLUG_BASE.format(kbid=request.kb.uuid)
            async for key in txn.keys(match=base):
                # Fetch and Add wanted item
                rid = await txn.get(key, for_update=False)
                if rid is not None:
                    resource = await kb.get(rid.decode())
                    if resource is not None:
                        yield await generate_train_resource(resource, request.metadata)
