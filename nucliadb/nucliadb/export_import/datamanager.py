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
from typing import AsyncIterator

from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
    GetEntitiesResponse,
    GetLabelsResponse,
)

from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.entities import EntitiesManager
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb_utils.storages.storage import Storage


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
                _clone_collect_cf(binaries, file_field.file)

        for conversation in bm.conversations.values():
            for message in conversation.messages:
                for attachment in message.content.attachments:
                    _clone_collect_cf(binaries, attachment)

        for layout in bm.layouts.values():
            for block in layout.body.blocks.values():
                if block.HasField("file"):
                    _clone_collect_cf(binaries, block.file)

        for field_extracted_data in bm.file_extracted_data:
            if field_extracted_data.HasField("file_thumbnail"):
                _clone_collect_cf(binaries, field_extracted_data.file_thumbnail)
            if field_extracted_data.HasField("file_preview"):
                _clone_collect_cf(binaries, field_extracted_data.file_preview)
            for file_generated in field_extracted_data.file_generated.values():
                _clone_collect_cf(binaries, file_generated)
            for page in field_extracted_data.file_pages_previews.pages:
                _clone_collect_cf(binaries, page)

        for link_extracted_data in bm.link_extracted_data:
            if link_extracted_data.HasField("link_thumbnail"):
                _clone_collect_cf(binaries, link_extracted_data.link_thumbnail)
            if link_extracted_data.HasField("link_preview"):
                _clone_collect_cf(binaries, link_extracted_data.link_preview)
            if link_extracted_data.HasField("link_image"):
                _clone_collect_cf(binaries, link_extracted_data.link_image)

        for field_metadata in bm.field_metadata:
            if field_metadata.metadata.metadata.HasField("thumbnail"):
                _clone_collect_cf(binaries, field_metadata.metadata.metadata.thumbnail)

        return binaries

    async def download_cf_to_file(self, cf: CloudFile, destination_file) -> None:
        async for data in self.storage.download(cf.bucket_name, cf.uri):
            destination_file.write(data)

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


def _clone_collect_cf(binaries: list[CloudFile], origin: CloudFile):
    cf = CloudFile()
    cf.CopyFrom(origin)
    # Mark the cloud file of the broker message being exported as export source
    # so that it's clear that is part of an export while importing.
    origin.source = CloudFile.Source.EXPORT
    binaries.append(cf)
