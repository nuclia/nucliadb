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
from typing import AsyncGenerator, AsyncIterator, Optional, Union

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobProperties, BlobType, ContentSettings
from azure.storage.blob.aio import BlobServiceClient
from nucliadb_protos.resources_pb2 import CloudFile

from nucliadb_utils.storages import CHUNK_SIZE
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
        raise NotImplementedError()

    async def copy(
        self,
        origin_uri: str,
        destination_uri: str,
        origin_bucket_name: str,
        destination_bucket_name: str,
    ):
        raise NotImplementedError()

    async def iter_data(self, headers=None):
        key = self.field.uri if self.field else self.key
        if self.field is None:
            bucket = self.bucket
        else:
            bucket = self.field.bucket_name
        raise NotImplementedError()

    async def read_range(self, start: int, end: int) -> AsyncIterator[bytes]:
        """
        Iterate through ranges of data
        """
        key = self.field.uri if self.field else self.key
        if self.field is None:
            bucket = self.bucket
        else:
            bucket = self.field.bucket_name
        raise NotImplementedError()

    async def start(self, cf: CloudFile) -> CloudFile:
        raise NotImplementedError()

    async def append(self, cf: CloudFile, iterable: AsyncIterator) -> int:
        if self.field is None:
            raise AttributeError()
        count = 0
        raise NotImplementedError()
        return count

    async def finish(self):
        raise NotImplementedError()
        self.field.uri = self.key
        self.field.ClearField("offset")
        self.field.ClearField("upload_uri")

    async def exists(self) -> Optional[ObjectMetadata]:
        raise NotImplementedError()

    async def upload(self, iterator: AsyncIterator, origin: CloudFile) -> CloudFile:
        self.field = await self.start(origin)
        await self.append(origin, iterator)
        await self.finish()
        return self.field

    def __repr__(self):
        return f"{self.storage.source}: {self.bucket}/{self.key}"


class AzureStorage(Storage):
    field_klass = AzureStorageField
    chunk_size = CHUNK_SIZE

    def __init__(self, connection_string: str):
        self.object_store = AzureObjectStore(connection_string)

    async def initialize(self) -> None:
        await self.object_store.initialize()

    async def finalize(self) -> None:
        await self.object_store.finalize()

    def get_bucket_name(self, kbid: str) -> str:
        return self.object_store.get_bucket_name(kbid=kbid)

    async def create_kb(self, kbid: str) -> bool:
        bucket_name = self.get_bucket_name(kbid)
        return await self.object_store.create_bucket(bucket_name)

    async def delete_kb(self, kbid: str) -> tuple[bool, bool]:
        bucket_name = self.get_bucket_name(kbid)
        return await self.object_store.delete_bucket(bucket_name)

    async def delete_upload(self, uri: str, bucket_name: str):
        assert self.service_client is not None
        # Buckets are mapped to Azure's container concept
        container_name = bucket_name
        container_client = self.service_client.get_container_client(container_name)
        await container_client.delete_blob(uri, delete_snapshots="include")

    async def schedule_delete_kb(self, kbid: str) -> bool:
        """
        In Azure blob storage there is no option to schedule
        for deletion, so we will delete immediately.
        Returns whether the container was deleted or not
        """
        deleted, _ = await self.delete_kb(kbid)
        return deleted

    async def iterate_bucket(
        self, bucket: str, prefix: str
    ) -> AsyncIterator[ObjectInfo]:
        assert self.service_client is not None
        # Buckets are mapped to Azure's container concept
        container_name = bucket
        container_client = self.service_client.get_container_client(container_name)
        async for blob_name in container_client.list_blob_names(
            name_starts_with=prefix
        ):
            yield ObjectInfo(name=blob_name)

    async def download(
        self, bucket_name: str, key: str, headers: Optional[dict[str, str]] = None
    ) -> AsyncIterator[bytes]:
        # TODO: download headers is used for range downloads.
        # [refactor] We need to pass these as specific parameters instead.
        assert self.service_client is not None
        # Buckets are mapped to Azure's container concept
        container_name = bucket_name
        container_client = self.service_client.get_container_client(container_name)
        downloader = await container_client.download_blob(blob=key)
        async for chunk in downloader.chunks():
            yield chunk

    async def iterate_objects(
        self, bucket: str, prefix: str
    ) -> AsyncGenerator[ObjectInfo, None]:
        raise NotImplementedError()
        yield ObjectInfo(name="")


class AzureObjectStore(ObjectStore):
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.service_client: Optional[BlobServiceClient] = None

    async def initialize(self):
        self.service_client = BlobServiceClient.from_connection_string(
            self.connection_string
        )

    async def finalize(self):
        try:
            await self.service_client.close()
        except Exception as e:
            logger.warning(f"Error closing Azure client: {e}")
        self.service_client = None

    def get_bucket_name(self, **kwargs) -> str:
        kbid = kwargs["kbid"]
        return f"nucliadb_{kbid}"

    async def create_bucket(
        self, bucket: str, labels: dict[str, str] | None = None
    ) -> None:
        assert self.service_client is not None
        container_client = self.service_client.get_container_client(bucket)
        try:
            await container_client.create_container()
            return True
        except ResourceExistsError:
            return False

    async def delete_bucket(self, bucket: str) -> None:
        assert self.service_client is not None
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
        assert self.service_client is not None
        container_client = self.service_client.get_container_client(bucket)
        try:
            await container_client.get_container_properties()
            return True
        except ResourceNotFoundError:
            return False

    async def schedule_delete_bucket(self, bucket: str) -> None:
        # In Azure, there is no option to schedule for deletion
        await self.delete_bucket(bucket)

    async def move_object(
        self,
        origin_bucket: str,
        origin_key: str,
        destination_bucket: str,
        destination_key: str,
    ) -> None:
        raise NotImplementedError()

    async def copy_object(
        self,
        origin_bucket: str,
        origin_key: str,
        destination_bucket: str,
        destination_key: str,
    ) -> None:
        assert self.service_client is not None
        container_client = self.service_client.get_container_client(origin_bucket)
        blob_client = container_client.get_blob_client(origin_key)
        if destination_bucket != origin_bucket:
            destination_container_client = self.service_client.get_container_client(
                destination_bucket
            )
        else:
            destination_container_client = container_client

    async def delete_object(self, bucket: str, key: str) -> None:
        assert self.service_client is not None
        container_client = self.service_client.get_container_client(bucket)
        try:
            await container_client.delete_blob(key, delete_snapshots="include")
        except ResourceNotFoundError:
            raise ObjectNotFoundError()

    async def upload_object(
        self,
        bucket: str,
        key: str,
        data: Union[bytes, AsyncGenerator[bytes, None]],
        metadata: ObjectMetadata,
    ) -> None:
        assert self.service_client is not None
        container_client = self.service_client.get_container_client(bucket)
        if isinstance(data, bytes):
            length = len(data)
            metadata.size = length
        else:
            length = metadata.size or None
        custom_metadata = {
            key: str(value) for key, value in metadata.model_dump().items()
        }
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

    async def download_object(self, bucket: str, key: str) -> bytes:
        assert self.service_client is not None
        container_client = self.service_client.get_container_client(bucket)
        blob_client = container_client.get_blob_client(key)
        downloader = await blob_client.download_blob()
        return await downloader.readall()

    async def download_object_stream(
        self, bucket: str, key: str, range: Optional[Range] = None
    ) -> AsyncGenerator[bytes, None]:
        range = range or Range()
        assert self.service_client is not None
        container_client = self.service_client.get_container_client(bucket)
        blob_client = container_client.get_blob_client(key)
        offset = None
        length = None
        if range.any():
            offset = range.start or 0
            length = range.end - offset + 1 if range.end else None
        downloader = await blob_client.download_blob(
            offset=offset,
            length=length,
        )
        async for chunk in downloader.chunks():
            yield chunk

    async def iter_objects(
        self, bucket: str, prefix: str
    ) -> AsyncGenerator[ObjectInfo, None]:
        assert self.service_client is not None
        container_client = self.service_client.get_container_client(bucket)
        async for blob in container_client.list_blobs(name_starts_with=prefix):
            yield ObjectInfo(name=blob.name)

    async def get_object_metadata(self, bucket: str, key: str) -> ObjectMetadata:
        assert self.service_client is not None
        container_client = self.service_client.get_container_client(bucket)
        blob_client = container_client.get_blob_client(key)
        try:
            properties: BlobProperties = await blob_client.get_blob_properties()
            return parse_object_metadata(properties, key)
        except ResourceNotFoundError:
            raise ObjectNotFoundError()

    async def multipart_upload_start(
        self, bucket: str, key: str, metadata: ObjectMetadata
    ) -> str:
        raise NotImplementedError()

    async def multipart_upload_append(
        self, bucket: str, key: str, iterable: AsyncIterator[bytes]
    ):
        raise NotImplementedError()

    async def multipart_upload_finish(self, bucket: str, key: str) -> None:
        raise NotImplementedError()


def parse_object_metadata(properties: BlobProperties, key: str) -> ObjectMetadata:
    custom_metadata = properties.metadata or {}
    custom_metadata_size = custom_metadata.get("size")
    if custom_metadata_size and custom_metadata_size != "0":
        size = int(custom_metadata_size)
    else:
        size = properties.size
    filename = custom_metadata.get("filename") or key.split("/")[-1]
    content_type = (
        custom_metadata.get("content_type")
        or properties.content_settings.content_type
        or ""
    )
    return ObjectMetadata(
        filename=filename,
        size=size,
        content_type=content_type,
    )
