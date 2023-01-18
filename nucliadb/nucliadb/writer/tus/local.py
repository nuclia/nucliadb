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

import json
import os
import uuid
from typing import AsyncIterator

import aiofiles
from nucliadb_protos.resources_pb2 import CloudFile

from nucliadb.writer.tus.dm import FileDataMangaer
from nucliadb.writer.tus.exceptions import CloudFileNotFound
from nucliadb.writer.tus.storage import BlobStore, FileStorageManager
from nucliadb_utils.storages import CHUNK_SIZE


class LocalFileStorageManager(FileStorageManager):
    _handler = None
    storage: LocalBlobStore
    chunk_size = CHUNK_SIZE

    def metadata_key(self, uri: str) -> str:
        return f"{uri}.metadata"

    def get_file_path(self, bucket: str, key: str):
        bucket_path = self.storage.get_bucket_path(bucket)
        return f"{bucket_path}/{key}"

    async def start(self, dm: FileDataMangaer, path: str, kbid: str):
        bucket = self.storage.get_bucket_name(kbid)
        upload_file_id = dm.get("upload_file_id", str(uuid.uuid4()))
        init_url = self.get_file_path(bucket, upload_file_id)
        metadata_init_url = self.metadata_key(init_url)
        metadata = {
            "FILENAME": dm.filename,
            "CONTENT_TYPE": dm.content_type,
            "SIZE": dm.size,
        }
        async with aiofiles.open(metadata_init_url, "w+") as resp:
            await resp.write(json.dumps(metadata))

        await dm.update(upload_file_id=upload_file_id, path=path, bucket=bucket)

    async def iter_data(self, uri, kbid: str, headers=None):
        bucket = self.storage.get_bucket_name(kbid)
        file_path = self.get_file_path(bucket, uri)
        async with aiofiles.open(file_path) as resp:
            data = await resp.read(CHUNK_SIZE)
            while data is not None:
                yield data
                data = await resp.read(CHUNK_SIZE)

    async def read_range(
        self, uri: str, kbid: str, start: int, end: int
    ) -> AsyncIterator[bytes]:
        """
        Iterate through ranges of data
        """
        bucket = self.storage.get_bucket_name(kbid)
        file_path = self.get_file_path(bucket, uri)
        try:
            async with aiofiles.open(file_path, "rb") as resp:
                await resp.seek(start)
                count = 0
                data = await resp.read(CHUNK_SIZE)
                while data and count < end:
                    if count + len(data) > end:
                        new_end = end - count
                        data = data[:new_end]
                    yield data
                    count += len(data)
                    data = await resp.read(CHUNK_SIZE)
        except FileNotFoundError:
            raise CloudFileNotFound()

    async def _append(self, data, offset, aiofi):
        await aiofi.seek(offset)
        await aiofi.write(data)
        await aiofi.flush()

    async def append(self, dm: FileDataMangaer, iterable, offset) -> int:
        count = 0
        bucket = dm.get("bucket")
        upload_file_id = dm.get("upload_file_id")
        init_url = self.get_file_path(bucket, upload_file_id)
        async with aiofiles.open(init_url, "wb") as aiofi:
            async for chunk in iterable:
                await self._append(chunk, offset, aiofi)
                size = len(chunk)
                count += size
                offset += size
        return count

    async def finish(self, dm: FileDataMangaer):
        # Move from old to new
        bucket = dm.get("bucket")

        upload_file_id = dm.get("upload_file_id")
        from_url = self.get_file_path(bucket, upload_file_id)

        path = dm.get("path")
        to_url = self.get_file_path(bucket, path)
        to_url_dirs = os.path.dirname(to_url)

        # Move the binary file
        os.makedirs(to_url_dirs, exist_ok=True)
        os.rename(from_url, to_url)

        # Move metadata file too
        from_metadata_url = self.metadata_key(from_url)
        to_metadata_url = self.metadata_key(to_url)
        os.rename(from_metadata_url, to_metadata_url)
        await dm.finish()
        return path

    async def delete_upload(self, uri: str, kbid: str):
        bucket = self.storage.get_bucket_name(kbid)
        file_path = self.get_file_path(bucket, uri)
        os.remove(file_path)


class LocalBlobStore(BlobStore):
    def __init__(self, local_testing_files: str):
        self.local_testing_files = local_testing_files.rstrip("/")
        self.bucket = "ndb_{kbid}"
        self.source = CloudFile.LOCAL

    async def initialize(self):
        pass

    async def finalize(self):
        pass

    def get_bucket_path(self, bucket: str):
        return f"{self.local_testing_files}/{bucket}"

    async def check_exists(self, bucket_name: str) -> bool:
        path = self.get_bucket_path(bucket_name)
        return os.path.exists(path)

    async def create_bucket(self, bucket: str):
        path = self.get_bucket_path(bucket)
        exists = os.path.exists(path)
        os.makedirs(path, exist_ok=True)
        return exists
