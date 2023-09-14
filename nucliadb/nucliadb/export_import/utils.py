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
from typing import AsyncGenerator, Callable

from nucliadb.common.context import ApplicationContext
from nucliadb.common.datamanagers.entities import EntitiesDataManager
from nucliadb.common.datamanagers.labels import LabelsDataManager
from nucliadb.common.datamanagers.resources import ResourcesDataManager
from nucliadb_protos import knowledgebox_pb2 as kb_pb2
from nucliadb_protos import resources_pb2, writer_pb2
from nucliadb_utils.const import Streams

BinaryStream = AsyncGenerator[bytes, None]
BinaryStreamGenerator = Callable[[int], BinaryStream]


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


async def import_broker_message(
    context: ApplicationContext, kbid: str, bm: writer_pb2.BrokerMessage
) -> None:
    bm.kbid = kbid
    partition = context.partitioning.generate_partition(kbid, bm.uuid)
    for import_bm in [get_writer_bm(bm), get_processor_bm(bm)]:
        await context.transaction.commit(
            import_bm,
            partition,
            wait=False,
            target_subject=Streams.INGEST_PROCESSED.subject,
        )


def get_writer_bm(bm: writer_pb2.BrokerMessage) -> writer_pb2.BrokerMessage:
    wbm = writer_pb2.BrokerMessage()
    wbm.CopyFrom(bm)
    for field in PROCESSING_BM_FIELDS:
        wbm.ClearField(field)  # type: ignore
    wbm.type = writer_pb2.BrokerMessage.MessageType.AUTOCOMMIT
    wbm.source = writer_pb2.BrokerMessage.MessageSource.WRITER
    return wbm


def get_processor_bm(bm: writer_pb2.BrokerMessage) -> writer_pb2.BrokerMessage:
    pbm = writer_pb2.BrokerMessage()
    pbm.CopyFrom(bm)
    for field in WRITER_BM_FIELDS:
        pbm.ClearField(field)  # type: ignore
    pbm.type = writer_pb2.BrokerMessage.MessageType.AUTOCOMMIT
    pbm.source = writer_pb2.BrokerMessage.MessageSource.PROCESSOR
    return pbm


async def import_binary(
    context: ApplicationContext,
    kbid: str,
    cf: resources_pb2.CloudFile,
    binary_generator: BinaryStreamGenerator,
) -> None:
    new_cf = resources_pb2.CloudFile()
    new_cf.CopyFrom(cf)
    bucket_name = context.blob_storage.get_bucket_name(kbid)
    new_cf.bucket_name = bucket_name

    src_kb = cf.uri.split("/")[1]
    new_cf.uri = new_cf.uri.replace(src_kb, kbid, 1)

    destination_field = context.blob_storage.field_klass(
        storage=context.blob_storage, bucket=bucket_name, fullkey=new_cf.uri
    )
    await context.blob_storage.uploaditerator(
        binary_generator(context.blob_storage.chunk_size), destination_field, new_cf
    )


async def set_entities_groups(
    context: ApplicationContext, kbid: str, entities_groups: kb_pb2.EntitiesGroups
) -> None:
    edm = EntitiesDataManager(context.kv_driver)
    await edm.set_entities_groups(kbid, entities_groups)


async def set_labels(
    context: ApplicationContext, kbid: str, labels: kb_pb2.Labels
) -> None:
    ldm = LabelsDataManager(context.kv_driver)
    await ldm.set_labels(kbid, labels)


async def iter_broker_messages(
    context: ApplicationContext, kbid: str
) -> AsyncGenerator[writer_pb2.BrokerMessage, None]:
    rdm = ResourcesDataManager(context.kv_driver, context.blob_storage)
    async for rid in rdm.iterate_resource_ids(kbid):
        bm = await rdm.get_broker_message(kbid, rid)
        if bm is None:
            continue
        yield bm


def get_cloud_files(bm: writer_pb2.BrokerMessage) -> list[resources_pb2.CloudFile]:
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

        for _, split_metadata in field_metadata.metadata.split_metadata.items():
            if split_metadata.HasField("thumbnail"):
                _clone_collect_cf(binaries, split_metadata.thumbnail)

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
