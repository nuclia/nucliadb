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
from typing import Any, AsyncGenerator, Callable, cast

from nucliadb.common.context import ApplicationContext
from nucliadb.export_import.exceptions import ExportStreamExhausted
from nucliadb.export_import.models import ExportedItemType
from nucliadb.export_import.utils import (
    import_binary,
    import_broker_message,
    set_entities_groups,
    set_labels,
)
from nucliadb_protos import knowledgebox_pb2 as kb_pb2
from nucliadb_protos import resources_pb2, writer_pb2

logger = logging.getLogger(__name__)
ExportItem = tuple[ExportedItemType, Any]
BinaryStream = AsyncGenerator[bytes, None]
BinaryStreamGenerator = Callable[[int], BinaryStream]


class ExportStream:
    def __init__(self, export: BytesIO):
        self.export = export
        self.read_bytes = 0
        self.length = len(export.getvalue())

    async def read(self, n_bytes):
        """
        Reads n_bytes from the export stream.
        Raises ExportStreamExhausted if there are no more bytes to read.
        """
        if self.read_bytes >= self.length:
            raise ExportStreamExhausted()
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

    async def read_type(self) -> ExportedItemType:
        type_bytes = await self.stream.read(3)
        return ExportedItemType(type_bytes.decode())

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

    async def read_bm(self) -> writer_pb2.BrokerMessage:
        data = await self.read_item()
        bm = writer_pb2.BrokerMessage()
        bm.ParseFromString(data)
        return bm

    async def read_entities(self) -> kb_pb2.EntitiesGroups:
        data = await self.read_item()
        entities = kb_pb2.EntitiesGroups()
        entities.ParseFromString(data)
        return entities

    async def read_labels(self) -> kb_pb2.Labels:
        data = await self.read_item()
        labels = kb_pb2.Labels()
        labels.ParseFromString(data)
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


async def import_kb(
    context: ApplicationContext, kbid: str, stream: ExportStream
) -> None:
    """
    Imports exported data from a stream into a knowledgebox.
    """
    stream_reader = ExportStreamReader(stream)
    async for item_type, data in stream_reader.iter_items():
        if item_type == ExportedItemType.RESOURCE:
            bm = cast(writer_pb2.BrokerMessage, data)
            await import_broker_message(context, kbid, bm)

        elif item_type == ExportedItemType.BINARY:
            cf = cast(resources_pb2.CloudFile, data[0])
            binary_generator = cast(BinaryStreamGenerator, data[1])
            await import_binary(context, kbid, cf, binary_generator)

        elif item_type == ExportedItemType.ENTITIES:
            entities = cast(kb_pb2.EntitiesGroups, data)
            await set_entities_groups(context, kbid, entities)

        elif item_type == ExportedItemType.LABELS:
            labels = cast(kb_pb2.Labels, data)
            await set_labels(context, kbid, labels)

        else:
            logger.warning(f"Unknown exporteed item type: {item_type}")
            continue
