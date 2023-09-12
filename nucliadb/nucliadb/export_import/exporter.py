import tempfile
from typing import AsyncIterator
from uuid import uuid4

import aiofiles

from nucliadb.export_import import codecs
from nucliadb.export_import.context import ExporterContext

BINARY_CHUNK_SIZE = 1024 * 1024


async def iter_file_chunks(filename: str) -> AsyncIterator[bytes]:
    async with aiofiles.open(filename, mode="rb") as f:
        while True:
            chunk = await f.read(BINARY_CHUNK_SIZE)
            if not chunk:
                break
            yield chunk


async def export_kb(context: ExporterContext, kbid: str) -> AsyncIterator[bytes]:
    with tempfile.TemporaryDirectory() as dest_dir:
        async for bm in context.data_manager.iter_broker_messages(kbid):
            # Stream the binary files of the broker message
            binary_aux_file = f"{dest_dir}/{uuid4().hex}"
            for cloud_file in context.data_manager.get_binaries(bm):
                async with aiofiles.open(binary_aux_file, "wb") as binary_file:
                    file_size = await context.data_manager.download_binary(
                        cloud_file, binary_file
                    )
                serialized_cf = cloud_file.SerializeToString()
                yield codecs.CODEX.BINARY.encode("utf-8")
                yield len(serialized_cf).to_bytes(4, byteorder="big")
                yield serialized_cf
                yield file_size.to_bytes(4, byteorder="big")
                async for file_chunk in iter_file_chunks(binary_aux_file):
                    yield file_chunk

            # Stream the broker message
            bm_bytes = bm.SerializeToString()
            yield codecs.CODEX.RESOURCE.encode("utf-8")
            yield len(bm_bytes).to_bytes(4, byteorder="big")
            yield bm_bytes

        # Stream the entities and labels of the knowledgebox
        entities = await context.data_manager.get_entities(kbid)
        bytes = entities.SerializeToString()
        yield codecs.CODEX.ENTITIES.encode("utf-8")
        yield len(bytes).to_bytes(4, byteorder="big")
        yield bytes

        labels = await context.data_manager.get_labels(kbid)
        bytes = labels.SerializeToString()
        yield codecs.CODEX.LABELS.encode("utf-8")
        yield len(bytes).to_bytes(4, byteorder="big")
        yield bytes
