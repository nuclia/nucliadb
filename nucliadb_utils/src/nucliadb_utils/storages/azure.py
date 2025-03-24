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

from __future__ import annotations

import logging
from datetime import datetime
from typing import AsyncGenerator, AsyncIterator, Optional, Union

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobProperties, BlobType, ContentSettings
from azure.storage.blob.aio import BlobServiceClient

from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_utils.storages.exceptions import ObjectNotFoundError
from nucliadb_utils.storages.object_store import ObjectStore
from nucliadb_utils.storages.storage import Storage, StorageField
from nucliadb_utils.storages.utils import ObjectInfo, ObjectMetadata, Range

logger = logging.getLogger(__name__)


class AzureStorageField(StorageField):
    storage: AzureStorage

    async def move(
        self,
        origin_uri: str,
        destination_uri: str,
        origin_bucket_name: str,
        destination_bucket_name: str,
    ):
        await self.storage.object_store.move(
            origin_bucket_name, origin_uri, destination_bucket_name, destination_uri
        )

    async def copy(
        self,
        origin_uri: str,
        destination_uri: str,
        origin_bucket_name: str,
        destination_bucket_name: str,
    ):
        await self.storage.object_store.copy(
            origin_bucket_name, origin_uri, destination_bucket_name, destination_uri
        )

    async def iter_data(self, range: Optional[Range] = None) -> AsyncGenerator[bytes, None]:
        if self.field is not None:
            bucket = self.field.bucket_name
            key = self.field.uri
        else:
            bucket = self.bucket
            key = self.key
        async for chunk in self.storage.object_store.download_stream(bucket, key, range):
            yield chunk

    async def start(self, cf: CloudFile) -> CloudFile:
        """Init an upload.

        cf: New file to upload
        """
        if self.field is not None and self.field.upload_uri != "":
            # If there is a temporal url, delete it
            await self.storage.delete_upload(self.field.upload_uri, self.field.bucket_name)
        if self.field is not None and self.field.uri != "":
            field: CloudFile = CloudFile(
                filename=cf.filename,
                size=cf.size,
                content_type=cf.content_type,
                bucket_name=self.bucket,
                md5=cf.md5,
                source=CloudFile.AZURE,
                old_uri=self.field.uri,
                old_bucket=self.field.bucket_name,
            )
            upload_uri = f"{self.key}-{datetime.now().isoformat()}"
        else:
            field = CloudFile(
                filename=cf.filename,
                size=cf.size,
                md5=cf.md5,
                content_type=cf.content_type,
                bucket_name=self.bucket,
                source=CloudFile.AZURE,
            )
            upload_uri = self.key
        await self.storage.object_store.upload_multipart_start(
            self.bucket,
            upload_uri,
            ObjectMetadata(
                filename=cf.filename,
                size=cf.size,
                content_type=cf.content_type,
            ),
        )
        field.offset = 0
        field.upload_uri = upload_uri
        return field

    async def append(self, cf: CloudFile, iterable: AsyncIterator) -> int:
        if self.field is None:
            raise AttributeError()
        return await self.storage.object_store.upload_multipart_append(
            self.field.bucket_name, self.field.upload_uri, iterable
        )

    async def finish(self):
        self.field.uri = self.key
        self.field.ClearField("resumable_uri")
        self.field.ClearField("offset")
        self.field.ClearField("upload_uri")
        self.field.ClearField("parts")

    async def exists(self) -> Optional[ObjectMetadata]:
        key = None
        bucket = None
        if self.field is not None and self.field.uri != "":
            key = self.field.uri
            bucket = self.field.bucket_name
        elif self.key != "":
            key = self.key
            bucket = self.bucket
        else:
            return None
        try:
            return await self.storage.object_store.get_metadata(bucket, key)
        except ObjectNotFoundError:
            return None

    async def upload(self, iterator: AsyncIterator, origin: CloudFile) -> CloudFile:
        self.field = await self.start(origin)
        if self.field is None:
            raise AttributeError()
        await self.append(origin, iterator)
        await self.finish()
        return self.field

    def __repr__(self):
        return f"{self.storage.source}: {self.bucket}/{self.key}"


class AzureStorage(Storage):
    field_klass = AzureStorageField
    object_store: ObjectStore
    source = CloudFile.AZURE

    def __init__(
        self,
        account_url: str,
        deadletter_bucket: str = "deadletter",
        indexing_bucket: str = "indexing",
        connection_string: Optional[str] = None,
    ):
        self.object_store = AzureObjectStore(account_url, connection_string=connection_string)
        self.deadletter_bucket = deadletter_bucket
        self.indexing_bucket = indexing_bucket

    async def initialize(self, service_name: Optional[str] = None):
        await self.object_store.initialize()
        for bucket in [
            self.deadletter_bucket,
            self.indexing_bucket,
        ]:
            if bucket is None or bucket == "":
                continue
            try:
                await self.object_store.bucket_create(bucket)
            except Exception:
                logger.exception(f"Could not create bucket {bucket}", exc_info=True)

    async def finalize(self):
        await self.object_store.finalize()

    async def delete_upload(self, uri: str, bucket_name: str):
        try:
            await self.object_store.delete(bucket_name, uri)
        except ObjectNotFoundError:
            pass

    async def create_bucket(self, bucket_name: str, kbid: Optional[str] = None):
        if await self.object_store.bucket_exists(bucket_name):
            return
        await self.object_store.bucket_create(bucket_name)

    def get_bucket_name(self, kbid: str):
        return f"nucliadb-{kbid}"

    async def create_kb(self, kbid: str) -> bool:
        bucket_name = self.get_bucket_name(kbid)
        return await self.object_store.bucket_create(bucket_name)

    async def schedule_delete_kb(self, kbid: str) -> bool:
        bucket_name = self.get_bucket_name(kbid)
        deleted, _ = await self.object_store.bucket_delete(bucket_name)
        return deleted

    async def delete_kb(self, kbid: str) -> tuple[bool, bool]:
        bucket_name = self.get_bucket_name(kbid)
        return await self.object_store.bucket_delete(bucket_name)

    async def iterate_objects(
        self, bucket: str, prefix: str, start: Optional[str] = None
    ) -> AsyncGenerator[ObjectInfo, None]:
        async for obj in self.object_store.iterate(bucket, prefix, start):
            yield obj

    async def insert_object(self, bucket_name: str, key: str, data: bytes) -> None:
        await self.object_store.insert(bucket_name, key, data)


class AzureObjectStore(ObjectStore):
    def __init__(self, account_url: str, connection_string: Optional[str] = None):
        self.account_url = account_url
        self.connection_string = connection_string
        self._service_client: Optional[BlobServiceClient] = None

    @property
    def service_client(self) -> BlobServiceClient:
        if self._service_client is None:
            raise AttributeError("Service client not initialized")
        return self._service_client

    async def initialize(self):
        if self.connection_string:
            # For testing purposes
            self._service_client = BlobServiceClient.from_connection_string(self.connection_string)
        else:
            self._service_client = BlobServiceClient(
                self.account_url, credential=DefaultAzureCredential()
            )

    async def finalize(self):
        try:
            if self._service_client is not None:
                await self._service_client.close()
        except Exception:
            logger.warning("Error closing Azure client", exc_info=True)
        self._service_client = None

    async def bucket_create(self, bucket: str, labels: dict[str, str] | None = None) -> bool:
        container_client = self.service_client.get_container_client(bucket)
        try:
            await container_client.create_container()
            return True
        except ResourceExistsError:
            return False

    async def bucket_delete(self, bucket: str) -> tuple[bool, bool]:
        container_client = self.service_client.get_container_client(bucket)
        # There's never a conflict on Azure
        conflict = False
        deleted = False
        try:
            await container_client.delete_container()
            deleted = True
        except ResourceNotFoundError:
            deleted = False
        return deleted, conflict

    async def bucket_exists(self, bucket: str) -> bool:
        container_client = self.service_client.get_container_client(bucket)
        try:
            await container_client.get_container_properties()
            return True
        except ResourceNotFoundError:
            return False

    async def bucket_schedule_delete(self, bucket: str) -> None:
        # In Azure, there is no option to schedule for deletion
        await self.bucket_delete(bucket)

    async def move(
        self,
        origin_bucket: str,
        origin_key: str,
        destination_bucket: str,
        destination_key: str,
    ) -> None:
        await self.copy(origin_bucket, origin_key, destination_bucket, destination_key)
        await self.delete(origin_bucket, origin_key)

    async def copy(
        self,
        origin_bucket: str,
        origin_key: str,
        destination_bucket: str,
        destination_key: str,
    ) -> None:
        origin_blob_client = self.service_client.get_blob_client(origin_bucket, origin_key)
        origin_url = origin_blob_client.url
        destination_blob_client = self.service_client.get_blob_client(
            destination_bucket, destination_key
        )
        result = await destination_blob_client.start_copy_from_url(origin_url, requires_sync=True)
        assert result["copy_status"] == "success"

    async def delete(self, bucket: str, key: str) -> None:
        container_client = self.service_client.get_container_client(bucket)
        try:
            await container_client.delete_blob(key, delete_snapshots="include")
        except ResourceNotFoundError:
            raise ObjectNotFoundError()

    async def upload(
        self,
        bucket: str,
        key: str,
        data: Union[bytes, AsyncGenerator[bytes, None]],
        metadata: ObjectMetadata,
    ) -> None:
        container_client = self.service_client.get_container_client(bucket)
        length: Optional[int] = None
        if isinstance(data, bytes):
            length = len(data)
            metadata.size = length
        else:
            length = metadata.size or None
        custom_metadata = {key: str(value) for key, value in metadata.model_dump().items()}
        await container_client.upload_blob(
            name=key,
            data=data,
            length=length,
            blob_type=BlobType.BLOCKBLOB,
            metadata=custom_metadata,
            content_settings=ContentSettings(
                content_type=metadata.content_type,
                content_disposition=f"attachment; filename={metadata.filename}",
            ),
        )

    async def insert(self, bucket: str, key: str, data: bytes) -> None:
        container_client = self.service_client.get_container_client(bucket)
        await container_client.upload_blob(name=key, data=data, length=len(data))

    async def download(self, bucket: str, key: str) -> bytes:
        container_client = self.service_client.get_container_client(bucket)
        blob_client = container_client.get_blob_client(key)
        try:
            downloader = await blob_client.download_blob()
        except ResourceNotFoundError:
            raise ObjectNotFoundError()
        return await downloader.readall()

    async def download_stream(
        self, bucket: str, key: str, range: Optional[Range] = None
    ) -> AsyncGenerator[bytes, None]:
        range = range or Range()
        container_client = self.service_client.get_container_client(bucket)
        blob_client = container_client.get_blob_client(key)
        offset = None
        length = None
        if range.any():
            offset = range.start or 0
            length = range.end - offset + 1 if range.end else None
        try:
            downloader = await blob_client.download_blob(
                offset=offset,  # type: ignore
                length=length,  # type: ignore
            )
        except ResourceNotFoundError:
            raise ObjectNotFoundError()
        async for chunk in downloader.chunks():
            yield chunk

    async def iterate(
        self, bucket: str, prefix: str, start: Optional[str] = None
    ) -> AsyncGenerator[ObjectInfo, None]:
        container_client = self.service_client.get_container_client(bucket)
        async for blob in container_client.list_blobs(name_starts_with=prefix):
            if start and blob.name <= start:
                continue
            yield ObjectInfo(name=blob.name)

    async def get_metadata(self, bucket: str, key: str) -> ObjectMetadata:
        container_client = self.service_client.get_container_client(bucket)
        blob_client = container_client.get_blob_client(key)
        try:
            properties: BlobProperties = await blob_client.get_blob_properties()
            return parse_object_metadata(properties, key)
        except ResourceNotFoundError:
            raise ObjectNotFoundError()

    async def upload_multipart_start(self, bucket: str, key: str, metadata: ObjectMetadata) -> None:
        container_client = self.service_client.get_container_client(bucket)
        custom_metadata = {key: str(value) for key, value in metadata.model_dump().items()}
        blob_client = container_client.get_blob_client(key)
        await blob_client.create_append_blob(
            metadata=custom_metadata,
            content_settings=ContentSettings(
                content_type=metadata.content_type,
                content_disposition=f"attachment; filename={metadata.filename}",
            ),
        )

    async def upload_multipart_append(
        self, bucket: str, key: str, iterable: AsyncIterator[bytes]
    ) -> int:
        container_client = self.service_client.get_container_client(bucket)
        blob_client = container_client.get_blob_client(key)
        bytes_appended = 0
        async for chunk in iterable:
            bytes_appended += len(chunk)
            await blob_client.append_block(data=chunk)
        return bytes_appended

    async def upload_multipart_finish(self, bucket: str, key: str) -> None:
        # No need to do anything in Azure
        pass


def parse_object_metadata(properties: BlobProperties, key: str) -> ObjectMetadata:
    custom_metadata = properties.metadata or {}
    custom_metadata_size = custom_metadata.get("size")
    if custom_metadata_size and custom_metadata_size != "0":
        size = int(custom_metadata_size)
    else:
        size = properties.size
    filename = custom_metadata.get("filename") or key.split("/")[-1]
    content_type = custom_metadata.get("content_type") or properties.content_settings.content_type or ""
    return ObjectMetadata(
        filename=filename,
        size=size,
        content_type=content_type,
    )
