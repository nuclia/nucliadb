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
from typing import AsyncGenerator, AsyncIterator, Callable

from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
    GetEntitiesResponse,
    GetLabelsResponse,
)

from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.entities import EntitiesManager
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb_utils.const import Streams
from nucliadb_utils.partition import PartitionUtility
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.transaction import TransactionUtility

# Broker message fields that are populated by the processing pipeline
PROCESSING_BM_FIELDS = [
    "link_extracted_data",
    "file_extracted_data",
    "extracted_text",
    "field_metadata",
    "field_vectors",
    "field_large_metadata",
    "user_vectors",
]

# Broker message fields that are populated by the nucliadb writer component
WRITER_BM_FIELDS = [
    "links",
    "files",
    "texts",
    "conversations",
    "layouts",
    "keywordsets",
    "datetimes",
]

BinaryStream = AsyncGenerator[bytes, None]
BinaryStreamGenerator = Callable[[int], BinaryStream]


class KBExporterDataManager:
    def __init__(self, driver: Driver, storage: Storage):
        self.driver = driver
        self.storage = storage

    async def iter_broker_messages(self, kbid: str) -> AsyncIterator[BrokerMessage]:
        async with self.driver.transaction() as txn:
            kb = KnowledgeBoxORM(txn, self.storage, kbid)
            async for resource in kb.iterate_resources():
                resource.disable_vectors = False
                bm = await resource.generate_broker_message()
                yield bm

    def get_binaries(self, bm: BrokerMessage) -> list[CloudFile]:
        binaries: list[CloudFile] = []
        for file_field in bm.files.values():
            if file_field.HasField("file"):
                self._clone_collect_cf(binaries, file_field.file)

        for conversation in bm.conversations.values():
            for message in conversation.messages:
                for attachment in message.content.attachments:
                    self._clone_collect_cf(binaries, attachment)

        for layout in bm.layouts.values():
            for block in layout.body.blocks.values():
                if block.HasField("file"):
                    self._clone_collect_cf(binaries, block.file)

        for field_extracted_data in bm.file_extracted_data:
            if field_extracted_data.HasField("file_thumbnail"):
                self._clone_collect_cf(binaries, field_extracted_data.file_thumbnail)
            if field_extracted_data.HasField("file_preview"):
                self._clone_collect_cf(binaries, field_extracted_data.file_preview)
            for file_generated in field_extracted_data.file_generated.values():
                self._clone_collect_cf(binaries, file_generated)
            for page in field_extracted_data.file_pages_previews.pages:
                self._clone_collect_cf(binaries, page)

        for link_extracted_data in bm.link_extracted_data:
            if link_extracted_data.HasField("link_thumbnail"):
                self._clone_collect_cf(binaries, link_extracted_data.link_thumbnail)
            if link_extracted_data.HasField("link_preview"):
                self._clone_collect_cf(binaries, link_extracted_data.link_preview)
            if link_extracted_data.HasField("link_image"):
                self._clone_collect_cf(binaries, link_extracted_data.link_image)

        for field_metadata in bm.field_metadata:
            if field_metadata.metadata.metadata.HasField("thumbnail"):
                self._clone_collect_cf(
                    binaries, field_metadata.metadata.metadata.thumbnail
                )

        return binaries

    @staticmethod
    def _clone_collect_cf(binaries: list[CloudFile], origin: CloudFile):
        cf = CloudFile()
        cf.CopyFrom(origin)
        # Mark the cloud file of the broker message being exported as export source
        # so that it's clear that is part of an export while importing.
        origin.source = CloudFile.Source.EXPORT
        binaries.append(cf)

    async def download_binary(self, cf: CloudFile) -> AsyncIterator[bytes]:
        async for data in self.storage.download(cf.bucket_name, cf.uri):
            yield data

    async def get_entities(self, kbid: str) -> GetEntitiesResponse:
        ger = GetEntitiesResponse()
        async with self.driver.transaction() as txn:
            kb = KnowledgeBoxORM(txn, self.storage, kbid)
            em = EntitiesManager(kb, txn)
            await em.get_entities(ger)
            return ger

    async def get_labels(self, kbid: str) -> GetLabelsResponse:
        glr = GetLabelsResponse()
        async with self.driver.transaction() as txn:
            kb = KnowledgeBoxORM(txn, self.storage, kbid)
            labels = await kb.get_labels()
            glr.labels.CopyFrom(labels)
            return glr


class KBImporterDataManager:
    def __init__(
        self,
        driver: Driver,
        storage: Storage,
        partitioning: PartitionUtility,
        transaction: TransactionUtility,
    ):
        self.driver = driver
        self.storage = storage
        self.partitioning = partitioning
        self.transaction = transaction

    @staticmethod
    def writer_bm(bm: BrokerMessage) -> BrokerMessage:
        wbm = BrokerMessage()
        wbm.CopyFrom(bm)
        for field in PROCESSING_BM_FIELDS:
            wbm.ClearField(field)  # type: ignore
        wbm.type = BrokerMessage.MessageType.AUTOCOMMIT
        wbm.source = BrokerMessage.MessageSource.WRITER
        return wbm

    @staticmethod
    def processor_bm(bm: BrokerMessage) -> BrokerMessage:
        pbm = BrokerMessage()
        pbm.CopyFrom(bm)
        for field in WRITER_BM_FIELDS:
            pbm.ClearField(field)  # type: ignore
        pbm.type = BrokerMessage.MessageType.AUTOCOMMIT
        pbm.source = BrokerMessage.MessageSource.PROCESSOR
        return pbm

    async def import_broker_message(self, kbid: str, bm: BrokerMessage):
        bm.kbid = kbid
        partition = self.partitioning.generate_partition(kbid, bm.uuid)
        for import_bm in [self.writer_bm(bm), self.processor_bm(bm)]:
            await self.transaction.commit(
                import_bm,
                partition,
                wait=False,
                target_subject=Streams.INGEST_PROCESSED.subject,
            )

    async def import_binary(
        self, kbid: str, cf: CloudFile, bytes_gen: BinaryStreamGenerator
    ):
        new_cf = CloudFile()
        new_cf.CopyFrom(cf)
        bucket_name = self.storage.get_bucket_name(kbid)
        new_cf.bucket_name = bucket_name

        src_kb = cf.uri.split("/")[1]
        new_cf.uri = new_cf.uri.replace(src_kb, kbid, 1)

        destination_field = self.storage.field_klass(
            storage=self.storage, bucket=bucket_name, fullkey=new_cf.uri
        )
        await self.storage.uploaditerator(
            bytes_gen(self.storage.chunk_size), destination_field, new_cf
        )

    async def set_entities(self, kbid: str, ger: GetEntitiesResponse):
        async with self.driver.transaction() as txn:
            kb = KnowledgeBoxORM(txn, self.storage, kbid)
            em = EntitiesManager(kb, txn)
            for group, entities in ger.groups.items():
                await em.set_entities_group(group, entities)
            await txn.commit()

    async def set_labels(self, kbid: str, gel: GetLabelsResponse):
        async with self.driver.transaction() as txn:
            kb = KnowledgeBoxORM(txn, self.storage, kbid)
            for labelset, labelset_obj in gel.labels.labelset.items():
                await kb.set_labelset(labelset, labelset_obj)
            await txn.commit()
