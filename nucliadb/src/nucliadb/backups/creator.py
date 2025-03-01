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
import asyncio
from datetime import UTC, datetime
from typing import Optional

from nucliadb.backups.models import BackupMetadata, CreateBackupRequest
from nucliadb.backups.tasks import TaskRetryHandler
from nucliadb.common import datamanagers
from nucliadb.common.context import ApplicationContext
from nucliadb.export_import.utils import (
    download_binary,
    get_broker_message,
    get_cloud_files,
    get_entities,
    get_labels,
)
from nucliadb_protos import writer_pb2


async def backup_kb(context: ApplicationContext, msg: CreateBackupRequest):
    kbid = msg.kbid
    backup_id = msg.backup_id
    retried_backup = TaskRetryHandler(
        task_id=f"{kbid}::{backup_id}",
        context=context,
        max_retries=5,
    ).wrap(_backup_kb)
    await retried_backup(context, kbid, backup_id)


async def _backup_kb(context: ApplicationContext, kbid: str, backup_id: str):
    await backup_resources(context, kbid, backup_id)
    await backup_labels(context, kbid, backup_id)
    await backup_entities(context, kbid, backup_id)


async def backup_resources(context: ApplicationContext, kbid: str, backup_id: str):
    metadata = await get_metadata(context, kbid, backup_id)
    if metadata is None:
        metadata = BackupMetadata(
            kbid=kbid,
            requested_at=datetime.now(tz=UTC),
        )
        async for rid in datamanagers.resources.iterate_resource_ids(kbid=kbid):
            metadata.total_resources += 1
            metadata.missing_resources.append(rid)
        metadata.missing_resources.sort()
        await set_metadata(context, kbid, backup_id, metadata)
    tasks = []
    backing_up = []
    for rid in metadata.missing_resources:
        tasks.append(asyncio.create_task(backup_resource(context, kbid, rid)))
        backing_up.append(rid)
        if len(tasks) >= 50:
            resources_bytes = await asyncio.gather(*tasks)
            metadata.total_size += sum(resources_bytes)
            metadata.missing_resources = [rid for rid in metadata.missing_resources if rid not in backing_up]
            await set_metadata(context, kbid, backup_id, metadata)
            tasks = []
            backing_up = []
    if len(tasks) > 0:
        resources_bytes = await asyncio.gather(*tasks)
        metadata.total_size += sum(resources_bytes)
        metadata.missing_resources = [rid for rid in metadata.missing_resources if rid not in backing_up]
        await set_metadata(context, kbid, backup_id, metadata)
        tasks = []
        backing_up = []


async def backup_resource(context: ApplicationContext, kbid: str, rid: str) -> int:
    """
    Backs up a resource to the blob storage service.
    Returns the size of the resource in bytes.
    """
    bm = await get_broker_message(context, kbid, rid)
    if bm is None:
        return 0
    return 1


async def backup_resource_with_binaries(context, bm: writer_pb2.BrokerMessage) -> int:
    """
    Generate a tar file dynamically with the resource broker message and all its binary files,
    and stream it to the blob storage service. Returns the total size of the tar file in bytes.
    """
    total_bytes = 0
    # Stream the binary files of the broker message
    for cloud_file in get_cloud_files(bm):
        binary_size = cloud_file.size
        serialized_cf = cloud_file.SerializeToString()
#        yield serialized_cf
        async for chunk in download_binary(context, cloud_file):
            pass
 #           yield chunk
    # Stream the broker message
    bm_bytes = bm.SerializeToString()
#    yield bm_bytes
    return total_bytes


async def backup_labels(context: ApplicationContext, kbid: str, backup_id: str):
    labels = await get_labels(context, kbid)
    await context.blob_storage.upload_object(
        bucket_name="backups",
        object_name=f"{kbid}/{backup_id}/labels",
        data=labels.SerializeToString(),
    )


async def backup_entities(context: ApplicationContext, kbid: str, backup_id: str):
    entities = await get_entities(context, kbid)
    await context.blob_storage.upload_object(
        bucket_name="backups",
        object_name=f"{kbid}/{backup_id}/entities",
        data=entities.SerializeToString(),
    )


async def get_metadata(context: ApplicationContext, kbid: str, backup_id: str) -> Optional[BackupMetadata]:
    async with context.kv_driver.transaction(read_only=True) as txn:
        metadata_raw = await txn.get(f"kbs/{kbid}/backups/{backup_id}")
        if metadata_raw is None:
            return None
        return BackupMetadata.model_validate_json(metadata_raw)


async def set_metadata(context: ApplicationContext, kbid: str, backup_id: str, metadata: BackupMetadata):
    async with context.kv_driver.transaction() as txn:
        await txn.set(f"kbs/{kbid}/backups/{backup_id}", metadata.model_dump_json())
        await txn.commit()
