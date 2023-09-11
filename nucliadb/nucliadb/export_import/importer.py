from typing import AsyncGenerator

import aiofiles
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
    GetEntitiesRequest,
    GetLabelSetRequest,
)

from nucliadb.export_import import codecs
from nucliadb.export_import.context import ExporterContext

ItemType = codecs.CODEX
ItemData = tuple[bytes, str]
ExportItem = tuple[ItemType, ItemData]


async def import_kb(
    context: ExporterContext, kbid: str, item_generator: AsyncGenerator[ExportItem]
) -> None:
    async for codex, data in item_generator:
        if codex == codecs.CODEX.RESOURCE:
            bm = BrokerMessage()
            bm.ParseFromString(data)
            await context.data_manager.import_broker_message(kbid, bm)

        elif codex == codecs.CODEX.BINARY:
            binary_file_name = data
            async with aiofiles.open(binary_file_name, "rb") as binary_file:
                await context.data_manager.upload_file_to_cf(kbid, binary_file)

        elif codex == codecs.CODEX.ENTITIES:
            ger = GetEntitiesRequest()
            ger.ParseFromString(data)
            await context.data_manager.set_entities(kbid, ger)

        elif codex == codecs.CODEX.LABELS:
            glr = GetLabelSetRequest()
            glr.ParseFromString(data)
            await context.data_manager.set_labels(kbid, glr)

        else:
            # TODO add warning log
            continue


# Importing a binary
"""    
    async def UploadFile(self, request: AsyncIterator[UploadBinaryData], context=None) -> FileUploaded:  # type: ignore
        data: UploadBinaryData

        destination: Optional[StorageField] = None
        cf = CloudFile()
        data = await request.__anext__()
        if data.HasField("metadata"):
            bucket = self.storage.get_bucket_name(data.metadata.kbid)
            destination = self.storage.field_klass(
                storage=self.storage, bucket=bucket, fullkey=data.metadata.key
            )
            cf.content_type = data.metadata.content_type
            cf.filename = data.metadata.filename
            cf.size = data.metadata.size
        else:
            raise AttributeError("Metadata not found")

        async def generate_buffer(
            storage: Storage, request: AsyncIterator[UploadBinaryData]  # type: ignore
        ):
            # Storage requires uploading chunks of a specified size, this is
            # why we need to have an intermediate buffer
            buf = BytesIO()
            async for chunk in request:
                if not chunk.HasField("payload"):
                    raise AttributeError("Payload not found")
                buf.write(chunk.payload)
                while buf.tell() > storage.chunk_size:
                    buf.seek(0)
                    data = buf.read(storage.chunk_size)
                    if len(data):
                        yield data
                    old_data = buf.read()
                    buf = BytesIO()
                    buf.write(old_data)
            buf.seek(0)
            data = buf.read()
            if len(data):
                yield data

        if destination is None:
            raise AttributeError("No destination file")
        await self.storage.uploaditerator(
            generate_buffer(self.storage, request), destination, cf
        )
        result = FileUploaded()
        return result
"""
