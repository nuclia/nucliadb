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
from typing import AsyncGenerator, Callable, Optional, cast

from nucliadb.common.context import ApplicationContext
from nucliadb.export_import import logger
from nucliadb.export_import.datamanager import ExportImportDataManager
from nucliadb.export_import.models import (
    ExportedItemType,
    ImportMetadata,
    NatsTaskMessage,
)
from nucliadb.export_import.utils import (
    ExportStreamReader,
    TaskRetryHandler,
    import_binary,
    import_broker_message,
    set_entities_groups,
    set_labels,
)
from nucliadb_protos import knowledgebox_pb2 as kb_pb2
from nucliadb_protos import resources_pb2, writer_pb2

BinaryStream = AsyncGenerator[bytes, None]
BinaryStreamGenerator = Callable[[int], BinaryStream]


async def import_kb(
    context: ApplicationContext,
    kbid: str,
    stream: AsyncGenerator[bytes, None],
    metadata: Optional[ImportMetadata] = None,
) -> None:
    """
    Imports exported data from a stream into a knowledgebox.
    If metadata is provided, the import will be resumable as it will store checkpoints.
    """
    dm = ExportImportDataManager(context.kv_driver, context.blob_storage)
    stream_reader = ExportStreamReader(stream)

    if metadata is not None and metadata.processed > 0:
        # Resuming task in a retry
        await stream_reader.seek(metadata.processed)

    items_count = 0
    async for item_type, data in stream_reader.iter_items():
        items_count += 1
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

        if metadata is not None and items_count % 10 == 0:
            # Save checkpoint in metadata every 10 items
            metadata.processed = stream_reader.read_bytes
            await dm.set_metadata("import", metadata)

    if metadata is not None:
        metadata.processed = stream_reader.read_bytes or 0
        await dm.set_metadata("import", metadata)


async def import_kb_from_blob_storage(context: ApplicationContext, msg: NatsTaskMessage):
    """
    Imports to a knowledgebox from an export stored in the blob storage service.
    """
    kbid, import_id = msg.kbid, msg.id
    dm = ExportImportDataManager(context.kv_driver, context.blob_storage)
    metadata = await dm.get_metadata(type="import", kbid=kbid, id=import_id)

    retry_handler = TaskRetryHandler("import", dm, metadata)

    @retry_handler.wrap
    async def import_kb_retried(context, kbid, metadata):
        stream = dm.download_import(kbid, import_id)
        await import_kb(context, kbid, stream, metadata)

    await import_kb_retried(context, kbid, metadata)

    await dm.try_delete_from_storage("import", kbid, import_id)
