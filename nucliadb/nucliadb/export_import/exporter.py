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

from typing import AsyncGenerator, AsyncIterator

from nucliadb.common.context import ApplicationContext
from nucliadb.common.datamanagers.entities import EntitiesDataManager
from nucliadb.common.datamanagers.labels import LabelsDataManager
from nucliadb.common.datamanagers.resources import ResourcesDataManager
from nucliadb.export_import.models import ExportedItemType
from nucliadb_protos import knowledgebox_pb2 as kb_pb2
from nucliadb_protos import resources_pb2, writer_pb2


async def export_kb(context: ApplicationContext, kbid: str) -> AsyncIterator[bytes]:
    """Export the data of a knowledgebox to a stream of bytes.

    See https://github.com/nuclia/nucliadb/blob/main/docs/internal/EXPORTS.md
    for an overview on format of the export file/stream.
    """
    async for bm in iter_broker_messages(context, kbid):
        # Stream the binary files of the broker message
        for cloud_file in get_binaries(bm):
            binary_size = cloud_file.size
            serialized_cf = cloud_file.SerializeToString()

            yield ExportedItemType.BINARY.encode("utf-8")
            yield len(serialized_cf).to_bytes(4, byteorder="big")
            yield serialized_cf

            yield binary_size.to_bytes(4, byteorder="big")
            async for chunk in download_binary(context, cloud_file):
                yield chunk

        # Stream the broker message
        bm_bytes = bm.SerializeToString()
        yield ExportedItemType.RESOURCE.encode("utf-8")
        yield len(bm_bytes).to_bytes(4, byteorder="big")
        yield bm_bytes

        # Stream the entities and labels of the knowledgebox
        entities = await get_entities(context, kbid)
        bytes = entities.SerializeToString()
        yield ExportedItemType.ENTITIES.encode("utf-8")
        yield len(bytes).to_bytes(4, byteorder="big")
        yield bytes

        labels = await get_labels(context, kbid)
        bytes = labels.SerializeToString()
        yield ExportedItemType.LABELS.encode("utf-8")
        yield len(bytes).to_bytes(4, byteorder="big")
        yield bytes


async def iter_broker_messages(
    context: ApplicationContext, kbid: str
) -> AsyncGenerator[writer_pb2.BrokerMessage, None]:
    rdm = ResourcesDataManager(context.kv_driver, context.blob_storage)
    async for rid in rdm.iterate_resource_ids(kbid):
        resource = await rdm.get_resource(kbid, rid)
        if resource is None:
            continue
        resource.disable_vectors = False
        bm = await resource.generate_broker_message()
        yield bm


def get_binaries(bm: writer_pb2.BrokerMessage) -> list[resources_pb2.CloudFile]:
    """Return the list of binaries of a broker message."""
    binaries: list[resources_pb2.CloudFile] = []
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


def _clone_collect_cf(
    binaries: list[resources_pb2.CloudFile], origin: resources_pb2.CloudFile
):
    cf = resources_pb2.CloudFile()
    cf.CopyFrom(origin)
    # Mark the cloud file of the broker message being exported as export source
    # so that it's clear that is part of an export while importing.
    origin.source = resources_pb2.CloudFile.Source.EXPORT
    binaries.append(cf)


async def download_binary(
    context: ApplicationContext, cf: resources_pb2.CloudFile
) -> AsyncGenerator[bytes, None]:
    async for data in context.blob_storage.download(cf.bucket_name, cf.uri):
        yield data


async def get_entities(context: ApplicationContext, kbid: str) -> kb_pb2.EntitiesGroups:
    edm = EntitiesDataManager(context.kv_driver)
    return await edm.get_entities_groups(kbid)


async def get_labels(context: ApplicationContext, kbid: str) -> kb_pb2.Labels:
    ldm = LabelsDataManager(context.kv_driver)
    return await ldm.get_labels(kbid)
