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
import json
import logging
import tarfile
from datetime import datetime, timezone
from typing import AsyncIterator, Optional

from nucliadb.backups.const import (
    BackupFinishedStream,
    MaindbKeys,
    StorageKeys,
)
from nucliadb.backups.models import BackupMetadata, CreateBackupRequest
from nucliadb.backups.settings import settings
from nucliadb.common import datamanagers
from nucliadb.common.context import ApplicationContext
from nucliadb.export_import.utils import (
    download_binary,
    get_broker_message,
    get_cloud_files,
    get_entities,
    get_labels,
    get_search_configurations,
    get_synonyms,
)
from nucliadb.tasks.retries import TaskRetryHandler
from nucliadb_protos import backups_pb2, resources_pb2, writer_pb2
from nucliadb_utils.audit.stream import StreamAuditStorage
from nucliadb_utils.storages.storage import StorageField
from nucliadb_utils.utilities import get_audit

logger = logging.getLogger(__name__)


async def backup_kb_task(context: ApplicationContext, msg: CreateBackupRequest):
    kbid = msg.kb_id
    backup_id = msg.backup_id

    retry_handler = TaskRetryHandler(
        kbid=kbid,
        task_type="backup",
        task_id=backup_id,
        context=context,
        max_retries=5,
    )

    @retry_handler.wrap
    async def _backup_kb(context: ApplicationContext, kbid: str, backup_id: str):
        await backup_kb(context, kbid, backup_id)

    await _backup_kb(context, kbid, backup_id)


async def backup_kb(context: ApplicationContext, kbid: str, backup_id: str):
    """
    Backs up a KB to the cloud storage.
    """
    await backup_resources(context, kbid, backup_id)
    await backup_labels(context, kbid, backup_id)
    await backup_entities(context, kbid, backup_id)
    await backup_synonyms(context, kbid, backup_id)
    await backup_search_configurations(context, kbid, backup_id)
    await notify_backup_completed(context, kbid, backup_id)
    await delete_metadata(context, kbid, backup_id)


async def backup_resources(context: ApplicationContext, kbid: str, backup_id: str):
    metadata = await get_metadata(context, kbid, backup_id)
    if metadata is None:
        metadata = BackupMetadata(
            kb_id=kbid,
            requested_at=datetime.now(tz=timezone.utc),
        )
        async for rid in datamanagers.resources.iterate_resource_ids(kbid=kbid):
            metadata.total_resources += 1
            metadata.missing_resources.append(rid)
        metadata.missing_resources.sort()
        await set_metadata(context, kbid, backup_id, metadata)
    tasks = []
    backing_up = []
    for rid in metadata.missing_resources:
        tasks.append(asyncio.create_task(backup_resource(context, backup_id, kbid, rid)))
        backing_up.append(rid)
        if len(tasks) >= settings.backup_resources_concurrency:
            resources_bytes = await asyncio.gather(*tasks)
            metadata.total_size += sum(resources_bytes)
            metadata.missing_resources = [
                rid for rid in metadata.missing_resources if rid not in backing_up
            ]
            await set_metadata(context, kbid, backup_id, metadata)
            tasks = []
            backing_up = []
            logger.info(
                f"Backup resources: {len(metadata.missing_resources)} remaining",
                extra={"kbid": kbid, "backup_id": backup_id},
            )
    if len(tasks) > 0:
        resources_bytes = await asyncio.gather(*tasks)
        metadata.total_size += sum(resources_bytes)
        metadata.missing_resources = [rid for rid in metadata.missing_resources if rid not in backing_up]
        await set_metadata(context, kbid, backup_id, metadata)
        tasks = []
        backing_up = []
        logger.info(f"Backup resources: completed", extra={"kbid": kbid, "backup_id": backup_id})


async def backup_resource(context: ApplicationContext, backup_id: str, kbid: str, rid: str) -> int:
    """
    Backs up a resource to the blob storage service.
    Returns the size of the resource in bytes.
    """
    bm = await get_broker_message(context, kbid, rid)
    if bm is None:
        # Resource not found. May have been deleted while the backup was running.
        return 0
    return await backup_resource_with_binaries(context, backup_id, kbid, rid, bm)


async def to_tar(name: str, size: int, chunks: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    """
    This function is a generator that adds tar header and padding to the end of the chunks
    to be compatible with the tar format.
    """
    tarinfo = tarfile.TarInfo(name)
    tarinfo.size = size
    tarinfo.mtime = int(datetime.now().timestamp())
    tarinfo.mode = 0o644
    tarinfo.type = tarfile.REGTYPE
    header_bytes = tarinfo.tobuf(format=tarfile.GNU_FORMAT)
    yield header_bytes
    async for chunk in chunks:
        yield chunk
    if size % 512 != 0:
        yield b"\x00" * (512 - (size % 512))


async def backup_resource_with_binaries(
    context, backup_id: str, kbid: str, rid: str, bm: writer_pb2.BrokerMessage
) -> int:
    """
    Generate a tar file dynamically with the resource broker message and all its binary files,
    and stream it to the blob storage service. Returns the total size of the tar file in bytes.
    """
    total_size = 0

    async def resource_data_iterator():
        """
        Each tar file will have the following structure:

        - cloud-files/{cloud_file.uri}  (serialized resources_pb2.CloudFile)
        - binaries/{cloud_file.uri} (the actual binary content of the cloud file)
        - broker-message.pb

        The order is important because the restore process depends on it (needs to import
        the cloud files and its binaries first before the broker message).
        """
        nonlocal total_size

        for index, cloud_file in enumerate(get_cloud_files(bm)):
            if not await exists_cf(context, cloud_file):
                logger.warning(
                    "Cloud file not found in storage, skipping",
                    extra={
                        "kbid": kbid,
                        "rid": rid,
                        "cf_uri": cloud_file.uri,
                        "cf_bucket": cloud_file.bucket_name,
                    },
                )
                continue

            serialized_cf = cloud_file.SerializeToString()

            async def cf_iterator():
                yield serialized_cf

            async for chunk in to_tar(
                name=f"cloud-files/{index}", size=len(serialized_cf), chunks=cf_iterator()
            ):
                yield chunk
                total_size += len(chunk)

            async for chunk in to_tar(
                name=f"binaries/{index}",
                size=cloud_file.size,
                chunks=download_binary(context, cloud_file),
            ):
                yield chunk
                total_size += len(chunk)

        bm_serialized = bm.SerializeToString()

        async def bm_iterator():
            yield bm_serialized

        async for chunk in to_tar(
            name="broker-message.pb", size=len(bm_serialized), chunks=bm_iterator()
        ):
            yield chunk
            total_size += len(chunk)

    await upload_to_bucket(
        context,
        resource_data_iterator(),
        key=StorageKeys.RESOURCE.format(backup_id=backup_id, resource_id=rid),
    )
    return total_size


async def backup_labels(context: ApplicationContext, kbid: str, backup_id: str):
    labels = await get_labels(context, kbid)
    await context.blob_storage.upload_object(
        bucket=settings.backups_bucket,
        key=StorageKeys.LABELS.format(backup_id=backup_id),
        data=labels.SerializeToString(),
    )


async def backup_entities(context: ApplicationContext, kbid: str, backup_id: str):
    entities = await get_entities(context, kbid)
    await context.blob_storage.upload_object(
        bucket=settings.backups_bucket,
        key=StorageKeys.ENTITIES.format(backup_id=backup_id),
        data=entities.SerializeToString(),
    )


async def backup_synonyms(context: ApplicationContext, kbid: str, backup_id: str):
    synonyms = await get_synonyms(context, kbid)
    await context.blob_storage.upload_object(
        bucket=settings.backups_bucket,
        key=StorageKeys.SYNONYMS.format(backup_id=backup_id),
        data=synonyms.SerializeToString(),
    )


async def backup_search_configurations(context: ApplicationContext, kbid: str, backup_id: str):
    search_configurations = await get_search_configurations(context, kbid=kbid)
    serialized_search_configs = {
        config_id: config.model_dump(mode="python", exclude_unset=True)
        for config_id, config in search_configurations.items()
    }
    await context.blob_storage.upload_object(
        bucket=settings.backups_bucket,
        key=StorageKeys.SEARCH_CONFIGURATIONS.format(backup_id=backup_id),
        data=json.dumps(serialized_search_configs).encode(),
    )


async def get_metadata(
    context: ApplicationContext, kbid: str, backup_id: str
) -> Optional[BackupMetadata]:
    async with context.kv_driver.transaction(read_only=True) as txn:
        metadata_raw = await txn.get(MaindbKeys.METADATA.format(kbid=kbid, backup_id=backup_id))
        if metadata_raw is None:
            return None
        return BackupMetadata.model_validate_json(metadata_raw)


async def set_metadata(context: ApplicationContext, kbid: str, backup_id: str, metadata: BackupMetadata):
    async with context.kv_driver.transaction() as txn:
        await txn.set(
            MaindbKeys.METADATA.format(kbid=kbid, backup_id=backup_id),
            metadata.model_dump_json().encode(),
        )
        await txn.commit()


async def delete_metadata(context: ApplicationContext, kbid: str, backup_id: str):
    async with context.kv_driver.transaction() as txn:
        await txn.delete(MaindbKeys.METADATA.format(kbid=kbid, backup_id=backup_id))
        await txn.commit()


async def exists_cf(context: ApplicationContext, cf: resources_pb2.CloudFile) -> bool:
    bucket_name = context.blob_storage.get_bucket_name_from_cf(cf)
    return await context.blob_storage.exists_object(bucket=bucket_name, key=cf.uri)


async def upload_to_bucket(context: ApplicationContext, bytes_iterator: AsyncIterator[bytes], key: str):
    storage = context.blob_storage
    bucket = settings.backups_bucket
    cf = resources_pb2.CloudFile()
    cf.bucket_name = bucket
    cf.content_type = "binary/octet-stream"
    cf.source = resources_pb2.CloudFile.Source.EXPORT
    field: StorageField = storage.field_klass(storage=storage, bucket=bucket, fullkey=key, field=cf)
    await storage.uploaditerator(bytes_iterator, field, cf)


async def notify_backup_completed(context: ApplicationContext, kbid: str, backup_id: str):
    audit = get_audit()
    if audit is None or not isinstance(audit, StreamAuditStorage):
        # We rely on the stream audit utility as it already holds a connection
        # to the idp nats server. If it's not available, we can't send the notification.
        return
    metadata = await get_metadata(context, kbid, backup_id)
    if metadata is None:  # pragma: no cover
        raise ValueError("Backup metadata not found")
    notification = backups_pb2.BackupCreatedNotification()
    notification.finished_at.FromDatetime(datetime.now(tz=timezone.utc))
    notification.kb_id = kbid
    notification.backup_id = backup_id
    notification.size = metadata.total_size
    notification.resources = metadata.total_resources
    await audit.js.publish(
        BackupFinishedStream.subject,
        notification.SerializeToString(),
        stream=BackupFinishedStream.name,
    )
