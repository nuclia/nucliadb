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

from nucliadb.common import datamanagers
from nucliadb.common.cluster.utils import wait_for_node
from nucliadb.common.context import ApplicationContext
from nucliadb.export_import import logger
from nucliadb.export_import.datamanager import ExportImportDataManager
from nucliadb.export_import.models import (
    ExportedItemType,
    ImportMetadata,
    NatsTaskMessage,
)
from nucliadb.export_import.utils import (
    ExportStream,
    ExportStreamReader,
    IteratorExportStream,
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
    stream: ExportStream,
    metadata: Optional[ImportMetadata] = None,
    throttling: bool = False,
) -> None:
    """
    Imports exported data from a stream into a knowledgebox.
    If metadata is provided, the import will be resumable as it will store checkpoints.

    If throttling is true, it will throttle the import to avoid overloading the nodes.
    """
    dm = ExportImportDataManager(context.kv_driver, context.blob_storage)
    stream_reader = ExportStreamReader(stream)

    if metadata is not None and metadata.processed > 0:
        # Resuming task in a retry
        await stream_reader.seek(metadata.processed)

    throttle = ImportThrottling(context, kbid) if throttling else NoopThrottling()
    items_count = 0
    async for item_type, data in stream_reader.iter_items():
        items_count += 1
        if item_type == ExportedItemType.RESOURCE:
            if items_count % 20 == 0:
                await throttle.wait()
            bm = cast(writer_pb2.BrokerMessage, data)
            await import_broker_message(context, kbid, bm)
            throttle.add_resource(bm.uuid)

        elif item_type == ExportedItemType.BINARY:
            cf = cast(resources_pb2.CloudFile, data[0])
            binary_generator = cast(BinaryStreamGenerator, data[1])
            await import_binary(context, kbid, cf, binary_generator)

        elif item_type == ExportedItemType.ENTITIES:
            entities = cast(kb_pb2.EntitiesGroups, data)
            await set_entities_groups(kbid, entities)

        elif item_type == ExportedItemType.LABELS:
            labels = cast(kb_pb2.Labels, data)
            await set_labels(kbid, labels)

        else:
            logger.warning(f"Unknown exporteed item type: {item_type}")
            continue

        if metadata is not None and items_count % 10 == 0:
            # Save checkpoint in metadata every 10 items
            metadata.processed = stream_reader.read_bytes
            await dm.set_metadata("import", metadata)

    if metadata is not None:
        metadata.processed = stream_reader.read_bytes
        await dm.set_metadata("import", metadata)


async def import_kb_from_blob_storage(
    context: ApplicationContext, msg: NatsTaskMessage
):
    """
    Imports to a knowledgebox from an export stored in the blob storage service.
    """
    kbid, import_id = msg.kbid, msg.id
    dm = ExportImportDataManager(context.kv_driver, context.blob_storage)
    metadata = await dm.get_metadata(type="import", kbid=kbid, id=import_id)
    iterator = dm.download_import(kbid, import_id)
    stream = IteratorExportStream(iterator)

    retry_handler = TaskRetryHandler("import", dm, metadata)

    @retry_handler.wrap
    async def import_kb_retried(context, kbid, stream, metadata):
        await import_kb(context, kbid, stream, metadata, throttling=True)

    await import_kb_retried(context, kbid, stream, metadata)  # type: ignore

    await dm.try_delete_from_storage("import", kbid, import_id)


class ImportThrottling:
    """
    Waits until all nodes where resources are located have catched up.
    """

    def __init__(self, context: ApplicationContext, kbid: str):
        self.context = context
        self.kbid = kbid
        self.resources_to_check: set[str] = set()

    def add_resource(self, rid: str):
        self.resources_to_check.add(rid)

    async def wait(self):
        logger.info("Waiting for nodes to catch up indexing")
        nodes = await self._get_nodes()
        if not nodes:
            return
        for node_id in nodes:
            await wait_for_node(self.context, node_id)
        self.resources_to_check.clear()

    async def _get_nodes(self) -> set[str]:
        nodes: set[str] = set()
        if not self.resources_to_check:
            return nodes
        async with datamanagers.with_transaction() as txn:
            shards_obj = await datamanagers.cluster.get_kb_shards(txn, kbid=self.kbid)
            if shards_obj is None:
                return nodes
            for resource_id in self.resources_to_check:
                shard_id = await datamanagers.resources.get_resource_shard_id(
                    txn, kbid=self.kbid, rid=resource_id
                )
                if shard_id is None:
                    continue
                shard = next(
                    (shard for shard in shards_obj.shards if shard.shard == shard_id),
                    None,
                )
                if shard is None:
                    continue
                for replica in shard.replicas:
                    nodes.add(replica.node)
        return nodes


class NoopThrottling(ImportThrottling):
    def __init__(self):
        pass

    async def wait(self):
        pass

    def add_resource(self, rid: str):
        pass
