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
import functools
from io import BytesIO
from typing import AsyncGenerator, AsyncIterator, Callable, Optional

import nats.errors
from google.protobuf.message import DecodeError as ProtobufDecodeError

from nucliadb.common.context import ApplicationContext
from nucliadb.common.datamanagers.entities import EntitiesDataManager
from nucliadb.common.datamanagers.labels import LabelsDataManager
from nucliadb.common.datamanagers.resources import ResourcesDataManager
from nucliadb.export_import import logger
from nucliadb.export_import.datamanager import ExportImportDataManager
from nucliadb.export_import.exceptions import (
    ExportStreamExhausted,
    WrongExportStreamFormat,
)
from nucliadb.export_import.models import ExportedItemType, ExportItem, Metadata
from nucliadb_models.export_import import Status
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
    for pb in [get_writer_bm(bm), get_processor_bm(bm)]:
        await transaction_commit(context, pb, partition)


async def transaction_commit(
    context: ApplicationContext, bm: writer_pb2.BrokerMessage, partition: int
) -> None:
    """
    Try to send the broker message over nats. If it's too big, upload
    it to blob storage and over nats only send a reference to it.
    """
    try:
        await context.transaction.commit(
            bm,
            partition,
            wait=False,
            target_subject=Streams.INGEST_PROCESSED.subject,
        )
    except nats.errors.MaxPayloadError:
        stored_key = await context.blob_storage.set_stream_message(
            kbid=bm.kbid, rid=bm.uuid, data=bm.SerializeToString()
        )
        referenced_bm = writer_pb2.BrokerMessageBlobReference(
            uuid=bm.uuid, kbid=bm.kbid, storage_key=stored_key
        )
        await context.transaction.commit(
            writer=referenced_bm,
            partition=partition,
            target_subject=Streams.INGEST_PROCESSED.subject,
            # This header is needed as it's the way we flag the transaction
            # consumer to download from storage
            headers={"X-MESSAGE-TYPE": "PROXY"},
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


async def iter_kb_resource_uuids(
    context: ApplicationContext, kbid: str
) -> AsyncGenerator[str, None]:
    rdm = ResourcesDataManager(context.kv_driver, context.blob_storage)
    async for rid in rdm.iterate_resource_ids(kbid):
        yield rid


async def get_broker_message(
    context: ApplicationContext, kbid: str, rid: str
) -> Optional[writer_pb2.BrokerMessage]:
    rdm = ResourcesDataManager(context.kv_driver, context.blob_storage)
    return await rdm.get_broker_message(kbid, rid)


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


class EndOfStream(Exception):
    ...


class ExportStream:
    """
    Models a stream of export bytes that can be read from asynchronously.
    """

    def __init__(self, export: BytesIO):
        self.export = export
        self.read_bytes = 0
        self._length = len(export.getvalue())

    async def read(self, n_bytes):
        """
        Reads n_bytes from the export stream.
        Raises ExportStreamExhausted if there are no more bytes to read.
        """
        if self.read_bytes == self._length:
            raise ExportStreamExhausted()
        chunk = self.export.read(n_bytes)
        self.read_bytes += len(chunk)
        return chunk


class IteratorExportStream(ExportStream):
    """
    Adapts the parent class to be able to read bytes yielded from an async iterator.
    """

    def __init__(self, iterator: AsyncIterator[bytes]):
        self.iterator = iterator
        self.buffer = b""
        self.read_bytes = 0

    def _read_from_buffer(self, n_bytes: int) -> bytes:
        value = self.buffer[:n_bytes]
        self.buffer = self.buffer[n_bytes:]
        self.read_bytes += len(value)
        return value

    async def read(self, n_bytes: int) -> bytes:
        while True:
            try:
                if self.buffer != b"" and len(self.buffer) >= n_bytes:
                    return self._read_from_buffer(n_bytes)

                next_chunk = await self.iterator.__anext__()
                if next_chunk == b"":
                    raise EndOfStream()

                self.buffer += next_chunk
                if len(self.buffer) >= n_bytes:
                    return self._read_from_buffer(n_bytes)
                else:
                    # Need to read another chunk
                    continue

            except (StopAsyncIteration, EndOfStream):
                if self.buffer != b"":
                    return self._read_from_buffer(n_bytes)
                else:
                    raise ExportStreamExhausted()


class ExportStreamReader:
    """
    Async generator that reads from an export stream and
    yields the deserialized export items ready to be imported.
    """

    def __init__(self, export_stream: ExportStream):
        self.stream = export_stream

    @property
    def read_bytes(self) -> int:
        return self.stream.read_bytes

    async def seek(self, offset: int):
        await self.stream.read(offset)

    async def read_type(self) -> ExportedItemType:
        type_bytes = await self.stream.read(3)
        try:
            return ExportedItemType(type_bytes.decode())
        except ValueError:
            raise WrongExportStreamFormat()

    async def read_item(self) -> bytes:
        size_bytes = await self.stream.read(4)
        size = int.from_bytes(size_bytes, byteorder="big")
        data = await self.stream.read(size)
        return data

    async def read_binary(
        self,
    ) -> tuple[resources_pb2.CloudFile, BinaryStreamGenerator]:
        data = await self.read_item()
        cf = resources_pb2.CloudFile()
        try:
            cf.ParseFromString(data)
        except ProtobufDecodeError:
            raise WrongExportStreamFormat()
        binary_size_bytes = await self.stream.read(4)
        binary_size = int.from_bytes(binary_size_bytes, byteorder="big")

        async def file_chunks_generator(chunk_size: int) -> BinaryStream:
            bytes_read = 0
            while True:
                bytes_to_read = min(binary_size - bytes_read, chunk_size)
                if bytes_to_read == 0:
                    break
                chunk = await self.stream.read(bytes_to_read)
                yield chunk
                bytes_read += len(chunk)

        return cf, file_chunks_generator

    async def read_bm(self) -> writer_pb2.BrokerMessage:
        data = await self.read_item()
        bm = writer_pb2.BrokerMessage()
        try:
            bm.ParseFromString(data)
        except ProtobufDecodeError:
            raise WrongExportStreamFormat()
        return bm

    async def read_entities(self) -> kb_pb2.EntitiesGroups:
        data = await self.read_item()
        entities = kb_pb2.EntitiesGroups()
        try:
            entities.ParseFromString(data)
        except ProtobufDecodeError:
            raise WrongExportStreamFormat()
        return entities

    async def read_labels(self) -> kb_pb2.Labels:
        data = await self.read_item()
        labels = kb_pb2.Labels()
        try:
            labels.ParseFromString(data)
        except ProtobufDecodeError:
            raise WrongExportStreamFormat()
        return labels

    async def iter_items(self) -> AsyncGenerator[ExportItem, None]:
        while True:
            try:
                item_type = await self.read_type()
                read_data_func = {
                    ExportedItemType.RESOURCE: self.read_bm,
                    ExportedItemType.BINARY: self.read_binary,
                    ExportedItemType.ENTITIES: self.read_entities,
                    ExportedItemType.LABELS: self.read_labels,
                }[item_type]
                data = await read_data_func()  # type: ignore
                yield item_type, data
            except ExportStreamExhausted:
                break


class TaskRetryHandler:
    """
    Class that wraps an import/export task and adds retry logic to it.
    """

    def __init__(
        self,
        type: str,
        data_manager: ExportImportDataManager,
        metadata: Metadata,
        max_tries: int = 5,
    ):
        self.type = type
        self.max_tries = max_tries
        self.dm = data_manager
        self.metadata = metadata

    def wrap(self, func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            metadata = self.metadata
            task = metadata.task
            if task.status in (Status.FINISHED, Status.ERRORED):
                logger.info(
                    f"{self.type} task is {task.status.value}. Skipping",
                    extra={"kbid": metadata.kbid, f"{self.type}_id": metadata.id},
                )
                return

            if task.retries >= self.max_tries:
                task.status = Status.ERRORED
                logger.info(
                    f"{self.type} task reached max retries. Setting to ERRORED state",
                    extra={"kbid": metadata.kbid, f"{self.type}_id": metadata.id},
                )
                await self.dm.set_metadata(self.type, metadata)
                return
            try:
                metadata.task.status = Status.RUNNING
                await func(*args, **kwargs)
            except Exception:
                task.retries += 1
                logger.info(
                    f"{self.type} failed. Will be retried",
                    extra={"kbid": metadata.kbid, f"{self.type}_id": metadata.id},
                )
                raise
            else:
                logger.info(
                    f"{self.type} finished successfully",
                    extra={"kbid": metadata.kbid, f"{self.type}_id": metadata.id},
                )
                task.status = Status.FINISHED
            finally:
                await self.dm.set_metadata(self.type, metadata)

        return wrapper
