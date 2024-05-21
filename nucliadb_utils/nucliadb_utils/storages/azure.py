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
from typing import Any, AsyncIterator, Optional

from azure.storage.blob.aio import BlobServiceClient
from nucliadb_protos.resources_pb2 import CloudFile

from nucliadb_utils.storages import CHUNK_SIZE
from nucliadb_utils.storages.storage import Storage, StorageField

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

    async def exists(self) -> Optional[FileInfo]:  # type:ignore
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
        self.connection_string = connection_string
        self.service_client: Optional[BlobServiceClient] = None
        self.initialized = False

    async def initialize(self):
        self.service_client = BlobServiceClient.from_connection_string(
            self.connection_string
        )
        self.initialized = True

    async def finalize(self):
        try:
            await self.service_client.close()
        except Exception as e:
            logger.warning(f"Error closing Azure client: {e}")
        self.service_client = None
        self.initialized = False

    def get_bucket_name(self, kbid: str):
        raise NotImplementedError()

    async def create_kb(self, kbid: str):
        raise NotImplementedError()

    async def delete_kb(self, kbid: str) -> tuple[bool, bool]:
        raise NotImplementedError()

    async def delete_upload(self, uri: str, bucket_name: str):
        raise NotImplementedError()

    async def schedule_delete_kb(self, kbid: str) -> bool:
        raise NotImplementedError()

    async def iterate_bucket(self, bucket: str, prefix: str) -> AsyncIterator[Any]:
        raise NotImplementedError()

    async def download(
        self, bucket_name: str, key: str, headers: Optional[dict[str, str]] = None
    ) -> AsyncIterator[bytes]:
        raise NotImplementedError()
