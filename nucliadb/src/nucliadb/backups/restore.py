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
from typing import Any, AsyncIterator, Callable, Optional, Union

from pydantic import TypeAdapter

from nucliadb.backups.const import MaindbKeys, StorageKeys
from nucliadb.backups.models import RestoreBackupRequest
from nucliadb.backups.settings import settings
from nucliadb.common.context import ApplicationContext
from nucliadb.export_import.utils import (
    import_binary,
    restore_broker_message,
    set_entities_groups,
    set_labels,
    set_search_configurations,
    set_synonyms,
)
from nucliadb.tasks.retries import TaskRetryHandler
from nucliadb_models.configuration import SearchConfiguration
from nucliadb_protos import knowledgebox_pb2 as kb_pb2
from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_protos.writer_pb2 import BrokerMessage

logger = logging.getLogger(__name__)


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
    await restore_resources(context, kbid, backup_id)
    await restore_labels(context, kbid, backup_id)
    await restore_entities(context, kbid, backup_id)
    await restore_synonyms(context, kbid, backup_id)
    await restore_search_configurations(context, kbid, backup_id)
    await delete_last_restored(context, kbid, backup_id)


async def restore_resources(context: ApplicationContext, kbid: str, backup_id: str):
    last_restored = await get_last_restored(context, kbid, backup_id)
    tasks = []
    async for object_info in context.blob_storage.iterate_objects(
        bucket=settings.backups_bucket,
        prefix=StorageKeys.RESOURCES_PREFIX.format(backup_id=backup_id),
        start=last_restored,
    ):
        key = object_info.name
        resource_id = key.split("/")[-1].split(".tar")[0]
        tasks.append(asyncio.create_task(restore_resource(context, kbid, backup_id, resource_id)))
        if len(tasks) > settings.restore_resources_concurrency:
            await asyncio.gather(*tasks)
            tasks = []
            await set_last_restored(context, kbid, backup_id, key)
    if len(tasks) > 0:
        await asyncio.gather(*tasks)
        tasks = []
        await set_last_restored(context, kbid, backup_id, key)


async def get_last_restored(context: ApplicationContext, kbid: str, backup_id: str) -> Optional[str]:
    key = MaindbKeys.LAST_RESTORED.format(kbid=kbid, backup_id=backup_id)
    async with context.kv_driver.transaction(read_only=True) as txn:
        raw = await txn.get(key)
        if raw is None:
            return None
        return raw.decode()


async def set_last_restored(context: ApplicationContext, kbid: str, backup_id: str, resource_id: str):
    key = MaindbKeys.LAST_RESTORED.format(kbid=kbid, backup_id=backup_id)
    async with context.kv_driver.transaction() as txn:
        await txn.set(key, resource_id.encode())
        await txn.commit()


async def delete_last_restored(context: ApplicationContext, kbid: str, backup_id: str):
    key = MaindbKeys.LAST_RESTORED.format(kbid=kbid, backup_id=backup_id)
    async with context.kv_driver.transaction() as txn:
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
    def __init__(self, download_stream: AsyncIterator[bytes]):
        self.download_stream = download_stream
        self.buffer = b""
        self.cloud_files: dict[int, CloudFile] = {}

    async def read(self, size: int) -> bytes:
        while len(self.buffer) < size:
            chunk = await self.download_stream.__anext__()
            if not chunk:
                continue
            self.buffer += chunk
        result = self.buffer[:size]
        self.buffer = self.buffer[size:]
        return result

    async def iter_data(self, total_bytes: int, chunk_size: int = 1024 * 1024) -> AsyncIterator[bytes]:
        padding_bytes = 0
        if total_bytes % 512 != 0:
            # We need to read the padding bytes and then discard them
            padding_bytes = 512 - (total_bytes % 512)
        read_bytes = 0
        padding_reached = False
        async for chunk in self._iter(total_bytes + padding_bytes, chunk_size):
            if padding_reached:
                # Skip padding bytes. We can't break here because we need
                # to read the padding bytes from the stream
                continue
            padding_reached = read_bytes + len(chunk) >= total_bytes
            if padding_reached:
                chunk = chunk[: total_bytes - read_bytes]
            else:
                read_bytes += len(chunk)
            yield chunk

    async def _iter(self, total_bytes: int, chunk_size: int = 1024 * 1024) -> AsyncIterator[bytes]:
        remaining_bytes = total_bytes
        while remaining_bytes > 0:
            to_read = min(chunk_size, remaining_bytes)
            chunk = await self.read(to_read)
            yield chunk
            remaining_bytes -= len(chunk)
        assert remaining_bytes == 0

    async def read_tarinfo(self):
        raw_tar_header = await self.read(512)
        return tarfile.TarInfo.frombuf(raw_tar_header, encoding="utf-8", errors="strict")

    async def read_data(self, tarinfo: tarfile.TarInfo) -> bytes:
        tarinfo_size = tarinfo.size
        padding_bytes = 0
        if tarinfo_size % 512 != 0:
            # We need to read the padding bytes and then discard them
            padding_bytes = 512 - (tarinfo_size % 512)
        data = await self.read(tarinfo_size + padding_bytes)
        return data[:tarinfo_size]

    async def read_item(self) -> Union[BrokerMessage, CloudFile, CloudFileBinary]:
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
            download_stream = functools.partial(self.iter_data, size)
            cf = self.cloud_files[bin_index]
            return CloudFileBinary(cf.uri, download_stream)
        else:  # pragma: no cover
            raise ValueError(f"Unknown tar entry: {tarinfo.name}")


async def restore_resource(context: ApplicationContext, kbid: str, backup_id: str, resource_id: str):
    download_stream = context.blob_storage.download(
        bucket=settings.backups_bucket,
        key=StorageKeys.RESOURCE.format(backup_id=backup_id, resource_id=resource_id),
    )
    reader = ResourceBackupReader(download_stream)
    bm = None
    while True:
        item = await reader.read_item()
        if isinstance(item, BrokerMessage):
            # When the broker message is read, this means all cloud files
            # and binaries of that resource have been read and imported
            bm = item
            bm.kbid = kbid
            break
        elif isinstance(item, CloudFile):
            # Read its binary and import it
            cf = item
            cf_binary = await reader.read_item()
            assert isinstance(cf_binary, CloudFileBinary)
            assert cf.uri == cf_binary.uri
            await import_binary(context, kbid, cf, cf_binary.read)
        else:
            logger.error(
                "Unexpected item in resource backup. Backup may be corrupted",
                extra={"item_type": type(item), kbid: kbid, resource_id: resource_id},
            )
            continue
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


async def restore_entities(context: ApplicationContext, kbid: str, backup_id: str):
    raw = await context.blob_storage.downloadbytes(
        bucket=settings.backups_bucket,
        key=StorageKeys.ENTITIES.format(backup_id=backup_id),
    )
    entities = kb_pb2.EntitiesGroups()
    entities.ParseFromString(raw.getvalue())
    await set_entities_groups(context, kbid, entities)


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
