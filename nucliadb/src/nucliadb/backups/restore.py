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
import functools
import json
import logging
import tarfile
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from contextlib import aclosing
from typing import Any

from pydantic import TypeAdapter

from nucliadb.backups.const import MaindbKeys, StorageKeys
from nucliadb.backups.models import RestoreBackupRequest
from nucliadb.backups.settings import settings
from nucliadb.backups.utils import exists_backup
from nucliadb.common import datamanagers
from nucliadb.common.context import ApplicationContext
from nucliadb.export_import.utils import (
    import_binary,
    restore_broker_message,
    set_kv_schemas,
    set_labels,
    set_search_configurations,
    set_synonyms,
)
from nucliadb.tasks.retries import TaskRetryHandler
from nucliadb_models.configuration import SearchConfiguration
from nucliadb_models.kv_schemas import KBKVSchemas
from nucliadb_protos import knowledgebox_pb2 as kb_pb2
from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_telemetry import metrics
from nucliadb_telemetry.metrics import INF
from nucliadb_utils.storages.utils import Range

logger = logging.getLogger(__name__)


restore_resource_observer = metrics.Observer(
    "nucliadb_backup_restore_resource_duration_seconds",
    labels={},
    buckets=[0.05, 0.1, 0.2, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 2.5 * 60.0, 5 * 60, 10 * 60, INF],
)


async def restore_kb_task(context: ApplicationContext, msg: RestoreBackupRequest):
    kbid = msg.kb_id
    backup_id = msg.backup_id

    retry_handler = TaskRetryHandler(
        kbid=kbid,
        task_type="restore",
        task_id=backup_id,
        context=context,
        max_retries=3,
    )

    @retry_handler.wrap
    async def _restore_kb(context: ApplicationContext, kbid: str, backup_id: str):
        await restore_kb(context, kbid, backup_id)

    await _restore_kb(context, kbid, backup_id)


async def restore_kb(context: ApplicationContext, kbid: str, backup_id: str):
    """
    Downloads the backup files from the cloud storage and imports them into the KB.
    """
    kb_exists = await datamanagers.atomic.kb.exists(kbid=kbid)
    if not kb_exists:
        logger.warning(
            "Trying to restore on a KB that doesn't exist",
            extra={"kbid": kbid, "backup_id": backup_id},
        )
        return
    if not await exists_backup(context.blob_storage, backup_id):
        logger.warning(
            "Trying to restore from a bucket that doesn't exist",
            extra={"kbid": kbid, "backup_id": backup_id},
        )
        return

    await restore_resources(context, kbid, backup_id)
    await restore_labels(context, kbid, backup_id)
    await restore_synonyms(context, kbid, backup_id)
    await restore_search_configurations(context, kbid, backup_id)
    await restore_kv_schemas(context, kbid, backup_id)
    await delete_last_restored(context, kbid, backup_id)


async def restore_resources(context: ApplicationContext, kbid: str, backup_id: str):
    last_restored = await get_last_restored(context, kbid, backup_id)
    tasks = []
    restored_count = 0
    async for object_info in context.blob_storage.iterate_objects(
        bucket=settings.backups_bucket,
        prefix=StorageKeys.RESOURCES_PREFIX.format(backup_id=backup_id),
        start=last_restored,
    ):
        key = object_info.name
        resource_id = key.split("/")[-1].split(".tar")[0]
        tasks.append(asyncio.create_task(restore_resource(context, kbid, backup_id, resource_id)))
        if len(tasks) >= settings.restore_resources_concurrency:
            await asyncio.gather(*tasks)
            restored_count += len(tasks)
            tasks = []
            await set_last_restored(context, kbid, backup_id, key)

            if restored_count % (settings.restore_resources_concurrency * 10) == 0:
                logger.info(
                    f"Restored {restored_count} resources",
                    extra={"kbid": kbid, "backup_id": backup_id},
                )

    if len(tasks) > 0:
        await asyncio.gather(*tasks)
        tasks = []
        await set_last_restored(context, kbid, backup_id, key)

    logger.info(f"Restore resources: completed", extra={"kbid": kbid, "backup_id": backup_id})


async def get_last_restored(context: ApplicationContext, kbid: str, backup_id: str) -> str | None:
    key = MaindbKeys.LAST_RESTORED.format(kbid=kbid, backup_id=backup_id)
    async with context.kv_driver.ro_transaction() as txn:
        raw = await txn.get(key)
        if raw is None:
            return None
        return raw.decode()


async def set_last_restored(context: ApplicationContext, kbid: str, backup_id: str, resource_id: str):
    key = MaindbKeys.LAST_RESTORED.format(kbid=kbid, backup_id=backup_id)
    async with context.kv_driver.rw_transaction() as txn:
        await txn.set(key, resource_id.encode())
        await txn.commit()


async def delete_last_restored(context: ApplicationContext, kbid: str, backup_id: str):
    key = MaindbKeys.LAST_RESTORED.format(kbid=kbid, backup_id=backup_id)
    async with context.kv_driver.rw_transaction() as txn:
        await txn.delete(key)
        await txn.commit()


class CloudFileBinary:
    def __init__(self, uri: str, download_stream: Callable[[int], AsyncIterator[bytes]]):
        self.uri = uri
        self.download_stream = download_stream

    async def read(self, chunk_size: int) -> AsyncIterator[bytes]:
        async for chunk in self.download_stream(chunk_size):
            yield chunk


class ResourceBackupReader:
    def __init__(self, download_stream: Callable[[Range], AsyncGenerator[bytes]]):
        self.download_stream = download_stream
        self.offset = 0
        self.cloud_files: dict[int, CloudFile] = {}

    async def read(self, size: int) -> bytes:
        data = bytearray()
        async for chunk in self.iter_data(self.offset, size):
            data.extend(chunk)
        self.offset += size
        return bytes(data)

    async def iter_data(
        self, offset: int, total_bytes: int, chunk_size: int = 1024 * 1024
    ) -> AsyncIterator[bytes]:
        if total_bytes == 0:
            return
        remaining_bytes = total_bytes
        async with aclosing(
            self.download_stream(Range(start=offset, end=offset + total_bytes - 1))
        ) as stream:
            async for chunk in stream:
                if len(chunk) > remaining_bytes:
                    raise ValueError("Backup range returned more bytes than requested")
                remaining_bytes -= len(chunk)
                for start in range(0, len(chunk), chunk_size):
                    yield chunk[start : start + chunk_size]
        if remaining_bytes:
            raise ValueError("Truncated resource backup")

    async def read_tarinfo(self):
        raw_tar_header = await self.read(512)
        return tarfile.TarInfo.frombuf(raw_tar_header, encoding="utf-8", errors="strict")

    async def read_data(self, tarinfo: tarfile.TarInfo) -> bytes:
        data = await self.read(tarinfo.size)
        self.offset += -tarinfo.size % 512
        return data

    async def read_item(self) -> BrokerMessage | CloudFile | CloudFileBinary:
        tarinfo = await self.read_tarinfo()
        if tarinfo.name.startswith("broker-message"):
            raw_bm = await self.read_data(tarinfo)
            bm = BrokerMessage()
            bm.ParseFromString(raw_bm)
            return bm
        elif tarinfo.name.startswith("cloud-files"):
            cf_index = int(tarinfo.name.split("cloud-files/")[-1])
            raw_cf = await self.read_data(tarinfo)
            cf = CloudFile()
            cf.ParseFromString(raw_cf)
            self.cloud_files[cf_index] = cf
            return cf
        elif tarinfo.name.startswith("binaries"):
            bin_index = int(tarinfo.name.split("binaries/")[-1])
            size = tarinfo.size
            download_stream = functools.partial(self.iter_data, self.offset, size)
            self.offset += size + (-size % 512)
            cf = self.cloud_files.pop(bin_index)
            return CloudFileBinary(cf.uri, download_stream)
        else:  # pragma: no cover
            raise ValueError(f"Unknown tar entry: {tarinfo.name}")


@restore_resource_observer.wrap({})
async def restore_resource(context: ApplicationContext, kbid: str, backup_id: str, resource_id: str):
    download_stream = functools.partial(
        context.blob_storage.download,
        bucket=settings.backups_bucket,
        key=StorageKeys.RESOURCE.format(backup_id=backup_id, resource_id=resource_id),
    )
    reader = ResourceBackupReader(lambda byte_range: download_stream(range=byte_range))
    bm = None
    tasks: set[asyncio.Task[None]] = set()
    try:
        while True:
            if len(tasks) >= settings.restore_binaries_concurrency:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()
                tasks.difference_update(done)
            item = await reader.read_item()
            if isinstance(item, BrokerMessage):
                bm = item
                bm.kbid = kbid
                break
            elif isinstance(item, CloudFile):
                cf = item
                cf_binary = await reader.read_item()
                if not isinstance(cf_binary, CloudFileBinary) or cf.uri != cf_binary.uri:
                    raise ValueError("Cloud file does not match binary in resource backup")
                tasks.add(asyncio.create_task(import_binary(context, kbid, cf, cf_binary.read)))
            else:
                raise ValueError("Unexpected binary in resource backup")
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    if bm is not None:
        await restore_broker_message(context, kbid, bm)


async def restore_labels(context: ApplicationContext, kbid: str, backup_id: str):
    raw = await context.blob_storage.downloadbytes(
        bucket=settings.backups_bucket,
        key=StorageKeys.LABELS.format(backup_id=backup_id),
    )
    labels = kb_pb2.Labels()
    labels.ParseFromString(raw.getvalue())
    await set_labels(context, kbid, labels)


async def restore_synonyms(context: ApplicationContext, kbid: str, backup_id: str):
    raw = await context.blob_storage.downloadbytes(
        bucket=settings.backups_bucket,
        key=StorageKeys.SYNONYMS.format(backup_id=backup_id),
    )
    synonyms = kb_pb2.Synonyms()
    synonyms.ParseFromString(raw.getvalue())
    await set_synonyms(context, kbid, synonyms)


async def restore_search_configurations(context: ApplicationContext, kbid: str, backup_id: str):
    raw = await context.blob_storage.downloadbytes(
        bucket=settings.backups_bucket,
        key=StorageKeys.SEARCH_CONFIGURATIONS.format(backup_id=backup_id),
    )
    value = raw.getvalue()
    if not value:
        # No search configurations to restore
        return
    as_dict: dict[str, Any] = json.loads(value)
    search_configurations: dict[str, SearchConfiguration] = {}
    for name, data in as_dict.items():
        config: SearchConfiguration = TypeAdapter(SearchConfiguration).validate_python(data)
        search_configurations[name] = config
    await set_search_configurations(context, kbid, search_configurations)


async def restore_kv_schemas(context: ApplicationContext, kbid: str, backup_id: str):
    raw = await context.blob_storage.downloadbytes(
        bucket=settings.backups_bucket,
        key=StorageKeys.KV_SCHEMAS.format(backup_id=backup_id),
    )
    value = raw.getvalue()
    if not value:
        # No KV schemas to restore
        return
    schemas = KBKVSchemas.model_validate_json(value)
    await set_kv_schemas(context, kbid, schemas)
