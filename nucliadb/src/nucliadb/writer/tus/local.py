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
from typing import Any

import aiofiles

from nucliadb.writer.tus.dm import FileDataManager
from nucliadb.writer.tus.storage import BlobStore, FileStorageManager
from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_utils.storages import CHUNK_SIZE


class LocalFileStorageManager(FileStorageManager):
    _handler = None
    storage: LocalBlobStore
    chunk_size = CHUNK_SIZE
    min_chunk_size = None

    def metadata_key(self, uri: str) -> str:
        return f"{uri}.metadata"

    def get_file_path(self, bucket: str, key: str):
        bucket_path = self.storage.get_bucket_path(bucket)
        return f"{bucket_path}/{key}"

    async def start(self, dm: FileDataManager, path: str, kbid: str):
        bucket = self.storage.get_bucket_name(kbid)
        upload_file_id = dm.get("upload_file_id", str(uuid.uuid4()))
        init_url = self.get_file_path(bucket, upload_file_id)
        metadata = {
            "FILENAME": dm.filename,
            "CONTENT_TYPE": dm.content_type,
            "SIZE": dm.size,
        }
        await self.set_metadata(kbid, upload_file_id, metadata)

        async with aiofiles.open(init_url, "wb+") as aio_fi:
            await aio_fi.write(b"")

        await dm.update(upload_file_id=upload_file_id, path=path, bucket=bucket, kbid=kbid)

    async def set_metadata(self, kbid: str, upload_file_id: str, metadata: dict[str, Any]):
        bucket = self.storage.get_bucket_name(kbid)
        init_url = self.get_file_path(bucket, upload_file_id)
        metadata_init_url = self.metadata_key(init_url)
        async with aiofiles.open(metadata_init_url, "w+") as resp:
            await resp.write(json.dumps(metadata))

    async def append(self, dm: FileDataManager, iterable, offset) -> int:
        count = 0
        bucket = dm.get("bucket")
        upload_file_id = dm.get("upload_file_id")
        init_url = self.get_file_path(bucket, upload_file_id)
        async with aiofiles.open(init_url, "rb+") as aio_fi:
            await aio_fi.seek(offset)
            async for chunk in iterable:
                await aio_fi.write(chunk)
                size = len(chunk)
                count += size
                offset += size
            await aio_fi.flush()
        return count

    async def finish(self, dm: FileDataManager):
        # Move from old to new
        bucket = dm.get("bucket")

        upload_file_id = dm.get("upload_file_id")
        from_url = self.get_file_path(bucket, upload_file_id)

        if dm.size > 0:
            kbid = dm.get("kbid")
            metadata = {
                "FILENAME": dm.filename,
                "CONTENT_TYPE": dm.content_type,
                "SIZE": dm.size,
            }
            await self.set_metadata(kbid, upload_file_id, metadata)

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

    def validate_intermediate_chunk(self, uploaded_bytes: int):
        pass


class LocalBlobStore(BlobStore):
    def __init__(self, local_testing_files: str):
        self.local_testing_files = local_testing_files.rstrip("/")
        self.bucket = "ndb_{kbid}"
        self.source = CloudFile.LOCAL

    async def initialize(self, *args, **kwargs):
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
