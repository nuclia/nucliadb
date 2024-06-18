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

from typing import Optional

from nucliadb.writer import logger
from nucliadb.writer.tus.dm import FileDataManager
from nucliadb.writer.tus.storage import BlobStore, FileStorageManager
from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_utils.storages import CHUNK_SIZE
from nucliadb_utils.storages.azure import AzureObjectStore
from nucliadb_utils.storages.exceptions import ObjectNotFoundError
from nucliadb_utils.storages.utils import ObjectMetadata


class AzureBlobStore(BlobStore):
    async def finalize(self):
        if self._object_store is None:
            return
        try:
            await self._object_store.close()
        except Exception:
            logger.exception("Error closing AzureBlobStore")
        self._object_store = None

    async def initialize(self, account_url: str, connection_string: Optional[str] = None):
        self.bucket = "nucliadb-{kbid}"
        self.source = CloudFile.Source.AZURE
        self._object_store = AzureObjectStore(account_url, connection_string=connection_string)
        await self._object_store.initialize()

    @property
    def object_store(self) -> AzureObjectStore:
        assert self._object_store is not None
        return self._object_store

    async def check_exists(self, bucket_name: str) -> bool:
        return await self.object_store.bucket_exists(bucket_name)

    async def create_bucket(self, bucket_name: str) -> bool:
        created = await self.object_store.bucket_create(bucket_name)
        return not created


class AzureFileStorageManager(FileStorageManager):
    storage: AzureBlobStore
    chunk_size = CHUNK_SIZE
    min_upload_size = None

    @property
    def object_store(self) -> AzureObjectStore:
        return self.storage.object_store

    async def start(self, dm: FileDataManager, path: str, kbid: str):
        bucket = self.storage.get_bucket_name(kbid)
        if dm.filename == 0:
            filename = "file"
        else:
            filename = dm.filename
        metadata = ObjectMetadata(
            filename=filename,
            content_type=dm.content_type,
            size=dm.size,
        )
        await self.object_store.upload_multipart_start(bucket, path, metadata)
        await dm.update(path=path, bucket=bucket)

    async def delete_upload(self, uri: str, kbid: str) -> None:
        bucket = self.storage.get_bucket_name(kbid)
        try:
            await self.object_store.delete(bucket, uri)
        except ObjectNotFoundError:
            logger.warning(
                "Attempt to delete an upload but not found",
                extra={"uri": uri, "kbid": kbid, "bucket": bucket},
            )

    async def append(self, dm: FileDataManager, iterable, offset: int) -> int:
        bucket = dm.get("bucket")
        assert bucket is not None
        path = dm.get("path")
        assert path is not None
        uploaded_bytes = await self.object_store.upload_multipart_append(bucket, path, iterable)
        await dm.update(offset=offset)
        return uploaded_bytes

    async def finish(self, dm: FileDataManager):
        path = dm.get("path")
        await dm.finish()
        return path

    def validate_intermediate_chunk(self, uploaded_bytes: int):
        pass
