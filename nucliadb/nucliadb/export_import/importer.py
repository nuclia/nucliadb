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
import logging
from io import BytesIO
from typing import Any, AsyncGenerator, cast

from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
    GetEntitiesResponse,
    GetLabelsResponse,
)

from nucliadb.export_import.context import KBImporterContext
from nucliadb.export_import.datamanager import BinaryStream, BinaryStreamGenerator
from nucliadb.export_import.models import CODEX

logger = logging.getLogger(__name__)
ExportItem = tuple[CODEX, Any]


class ExportStream:
    def __init__(self, export: BytesIO):
        self.export = export
        self.read_bytes = 0
        self.length = len(export.getvalue())

    async def read(self, n_bytes):
        if self.read_bytes >= self.length:
            raise StopAsyncIteration()

        chunk = self.export.read(n_bytes)
        self.read_bytes += len(chunk)
        return chunk


class ExportStreamReader:
    """
    Async generator that reads from an export stream and
    yields the different items to be imported.
    """

    def __init__(self, export_stream: ExportStream):
        self.stream = export_stream

    async def read_codex(self) -> CODEX:
        codex_bytes = await self.stream.read(3)
        return CODEX(codex_bytes.decode())

    async def read_item(self) -> bytes:
        size_bytes = await self.stream.read(4)
        size = int.from_bytes(size_bytes, byteorder="big")
        data = await self.stream.read(size)
        return data

    async def read_binary(self) -> tuple[CloudFile, BinaryStreamGenerator]:
        data = await self.read_item()
        cf = CloudFile()
        cf.ParseFromString(data)

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

    async def read_bm(self) -> BrokerMessage:
        data = await self.read_item()
        bm = BrokerMessage()
        bm.ParseFromString(data)
        return bm

    async def read_entities(self) -> GetEntitiesResponse:
        data = await self.read_item()
        entities = GetEntitiesResponse()
        entities.ParseFromString(data)
        return entities

    async def read_labels(self) -> GetLabelsResponse:
        data = await self.read_item()
        labels = GetLabelsResponse()
        labels.ParseFromString(data)
        return labels

    async def iter_items(self) -> AsyncGenerator[ExportItem, None]:
        while True:
            try:
                codex = await self.read_codex()
                if codex == CODEX.RESOURCE:
                    data = await self.read_bm()  # type: ignore
                elif codex == CODEX.ENTITIES:
                    data = await self.read_entities()  # type: ignore
                elif codex == CODEX.LABELS:
                    data = await self.read_labels()  # type: ignore
                elif codex == CODEX.BINARY:
                    data = await self.read_binary()  # type: ignore
                else:
                    raise ValueError(f"Unknown codex: {codex}")
                yield codex, data
            except StopAsyncIteration:
                break


async def import_kb(
    context: KBImporterContext, kbid: str, stream: ExportStream
) -> None:
    stream_reader = ExportStreamReader(stream)
    async for codex, data in stream_reader.iter_items():
        if codex == CODEX.RESOURCE:
            bm = cast(BrokerMessage, data)
            await context.data_manager.import_broker_message(kbid, bm)

        elif codex == CODEX.BINARY:
            cf = cast(CloudFile, data[0])
            binary_generator = cast(BinaryStreamGenerator, data[1])
            await context.data_manager.import_binary(kbid, cf, binary_generator)

        elif codex == CODEX.ENTITIES:
            ger = cast(GetEntitiesResponse, data)
            await context.data_manager.set_entities(kbid, ger)

        elif codex == CODEX.LABELS:
            glr = cast(GetLabelsResponse, data)
            await context.data_manager.set_labels(kbid, glr)
        else:
            logger.warning(f"Unknown codex: {codex}")
            continue
