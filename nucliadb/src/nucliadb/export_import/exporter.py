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

from typing import AsyncGenerator, Optional

from nucliadb.common.context import ApplicationContext
from nucliadb.export_import import logger
from nucliadb.export_import.datamanager import ExportImportDataManager
from nucliadb.export_import.models import (
    ExportedItemType,
    ExportMetadata,
    NatsTaskMessage,
)
from nucliadb.export_import.utils import (
    TaskRetryHandler,
    download_binary,
    get_broker_message,
    get_cloud_files,
    get_entities,
    get_labels,
    get_learning_config,
    iter_kb_resource_uuids,
)
from nucliadb_protos import writer_pb2
from nucliadb_telemetry import errors


async def export_kb(
    context: ApplicationContext, kbid: str, metadata: Optional[ExportMetadata] = None
) -> AsyncGenerator[bytes, None]:
    """Export the data of a knowledgebox to a stream of bytes.

    See https://github.com/nuclia/nucliadb/blob/main/docs/internal/EXPORTS.md
    for an overview on format of the export file/stream.

    If a metadata object is provided, uses it to resume the export if it was interrupted.
    """
    async for chunk in export_learning_config(kbid):
        yield chunk

    resources_iterator = export_resources(context, kbid)
    if metadata is not None:
        assert metadata.kbid == kbid
        resources_iterator = export_resources_resumable(context, metadata)

    async for chunk in resources_iterator:
        yield chunk

    async for chunk in export_entities(context, kbid):
        yield chunk

    async for chunk in export_labels(context, kbid):
        yield chunk


async def export_kb_to_blob_storage(context: ApplicationContext, msg: NatsTaskMessage) -> None:
    """
    Exports the data of a knowledgebox to the blob storage service.
    """
    kbid, export_id = msg.kbid, msg.id
    dm = ExportImportDataManager(context.kv_driver, context.blob_storage)
    metadata = await dm.get_metadata(type="export", kbid=kbid, id=export_id)
    iterator = export_kb(context, kbid, metadata)  # type: ignore

    retry_handler = TaskRetryHandler("export", dm, metadata)

    @retry_handler.wrap
    async def upload_export_retried(iterator, kbid, export_id) -> int:
        return await dm.upload_export(iterator, kbid, export_id)

    export_size = await upload_export_retried(iterator, kbid, export_id)

    # Store export size
    metadata.total = metadata.processed = export_size or 0
    await dm.set_metadata("export", metadata)


async def export_resources(
    context: ApplicationContext,
    kbid: str,
) -> AsyncGenerator[bytes, None]:
    async for rid in iter_kb_resource_uuids(context, kbid):
        bm = await get_broker_message(context, kbid, rid)
        if bm is None:
            logger.warning(f"No resource found for rid {rid}")
            continue
        async for chunk in export_resource_with_binaries(context, bm):
            yield chunk


async def export_resources_resumable(context, metadata: ExportMetadata) -> AsyncGenerator[bytes, None]:
    dm = ExportImportDataManager(context.kv_driver, context.blob_storage)

    kbid = metadata.kbid
    if len(metadata.resources_to_export) == 0:
        # Starting an export from scratch
        metadata.exported_resources = []
        async for rid in iter_kb_resource_uuids(context, kbid):
            metadata.resources_to_export.append(rid)
        metadata.resources_to_export.sort()
        metadata.total = len(metadata.resources_to_export)
        await dm.set_metadata("export", metadata)

    try:
        for rid in metadata.resources_to_export:
            bm = await get_broker_message(context, kbid, rid)
            if bm is None:
                logger.warning(f"Skipping resource {rid} as it was deleted")
                continue

            async for chunk in export_resource_with_binaries(context, bm):
                yield chunk

            metadata.exported_resources.append(rid)
            metadata.processed += 1
            await dm.set_metadata("export", metadata)

    except Exception as e:
        errors.capture_exception(e)
        logger.error(f"Error exporting resource {rid}: {e}")
        # Start from scracth next time.
        # TODO: try to resume from the last resource
        metadata.exported_resources = []
        metadata.processed = 0
        await dm.set_metadata("export", metadata)
        raise


async def export_resource_with_binaries(
    context,
    bm: writer_pb2.BrokerMessage,
) -> AsyncGenerator[bytes, None]:
    # Stream the binary files of the broker message
    for cloud_file in get_cloud_files(bm):
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


async def export_entities(
    context: ApplicationContext,
    kbid: str,
) -> AsyncGenerator[bytes, None]:
    entities = await get_entities(context, kbid)
    if len(entities.entities_groups) > 0:
        data = entities.SerializeToString()
        yield ExportedItemType.ENTITIES.encode("utf-8")
        yield len(data).to_bytes(4, byteorder="big")
        yield data


async def export_labels(
    context: ApplicationContext,
    kbid: str,
) -> AsyncGenerator[bytes, None]:
    labels = await get_labels(context, kbid)
    if len(labels.labelset) > 0:
        data = labels.SerializeToString()
        yield ExportedItemType.LABELS.encode("utf-8")
        yield len(data).to_bytes(4, byteorder="big")
        yield data


async def export_learning_config(
    kbid: str,
) -> AsyncGenerator[bytes, None]:
    lconfig = await get_learning_config(kbid)
    if lconfig is None:
        logger.warning(f"No learning configuration found for kbid", extra={"kbid": kbid})
        return
    data = lconfig.json().encode("utf-8")
    yield ExportedItemType.LEARNING_CONFIG.encode("utf-8")
    yield len(data).to_bytes(4, byteorder="big")
    yield data
