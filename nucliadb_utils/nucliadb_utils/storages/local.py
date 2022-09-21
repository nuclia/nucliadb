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

import glob
import json
import os
import shutil
from datetime import datetime
from typing import Any, AsyncIterator, Dict, Optional

import aiofiles
from nucliadb_protos.resources_pb2 import CloudFile

from nucliadb_utils.storages import CHUNK_SIZE
from nucliadb_utils.storages.storage import Storage, StorageField


class LocalStorageField(StorageField):
    storage: LocalStorage
    _handler = None

    def metadata_key(self, uri: Optional[str] = None):
        if uri is None and self.field is not None:
            return f"{self.field.uri}.metadata"
        elif uri is None and self.key is not None:
            return f"{self.key}.metadata"
        elif uri is not None:
            return f"{uri}.metadata"
        raise AttributeError("No URI and no Field Utils")

    async def move(
        self,
        origin_uri: str,
        destination_uri: str,
        origin_bucket_name: str,
        destination_bucket_name: str,
    ):
        origin_bucket_path = self.storage.get_bucket_path(origin_bucket_name)
        destination_bucket_path = self.storage.get_bucket_path(destination_bucket_name)
        origin_path = f"{origin_bucket_path}/{origin_uri}"
        destination_path = f"{destination_bucket_path}/{destination_uri}"
        os.rename(origin_path, destination_path)

    async def copy(
        self,
        origin_uri: str,
        destination_uri: str,
        origin_bucket_name: str,
        destination_bucket_name: str,
    ):
        pass

    def get_file_path(self, bucket: str, key: str):
        return f"{self.storage.get_bucket_name(bucket)}/{key}"

    async def iter_data(self, headers=None):
        key = self.field.uri if self.field else self.key
        if self.field is None:
            bucket = self.bucket
        else:
            bucket = self.field.bucket_name

        path = self.storage.get_bucket_path(bucket)

        async with aiofiles.open(self.get_file_path(path, key)) as resp:
            data = await resp.read(CHUNK_SIZE)
            while data is not None:
                yield data
                data = await resp.read(CHUNK_SIZE)

    async def read_range(self, start: int, end: int) -> AsyncIterator[bytes]:
        """
        Iterate through ranges of data
        """
        key = self.field.uri if self.field else self.key
        if self.field is None:
            bucket = self.bucket
        else:
            bucket = self.field.bucket_name

        path = self.storage.get_bucket_path(bucket)

        async with aiofiles.open(self.get_file_path(path, key), "rb") as resp:
            await resp.seek(start)
            count = 0
            data = await resp.read(CHUNK_SIZE)
            while data is not None and count < end:
                if count + len(data) > end:
                    new_end = end - count
                    data = data[:new_end]
                yield data
                count += len(data)
                data = await resp.read(CHUNK_SIZE)

    async def start(self, cf: CloudFile) -> CloudFile:

        if self.field is not None and self.field.upload_uri != "":
            # If there is a temporal url
            await self.storage.delete_upload(
                self.field.upload_uri, self.field.bucket_name
            )

        if self.field is not None and self.field.uri != "":
            field: CloudFile = CloudFile(
                filename=cf.filename,
                size=cf.size,
                md5=cf.md5,
                content_type=cf.content_type,
                bucket_name=self.bucket,
                source=CloudFile.LOCAL,
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
                source=CloudFile.LOCAL,
            )
            upload_uri = self.key

        path = self.storage.get_bucket_path(self.bucket)
        init_url = f"{path}/{upload_uri}"
        metadata_init_url = self.metadata_key(init_url)
        metadata = json.dumps(
            {"FILENAME": cf.filename, "SIZE": cf.size, "CONTENT_TYPE": cf.content_type}
        )

        path_to_create = os.path.dirname(metadata_init_url)
        os.makedirs(path_to_create, exist_ok=True)
        async with aiofiles.open(metadata_init_url, "w+") as resp:
            await resp.write(json.dumps(metadata))

        self._handler = await aiofiles.threadpool.open(init_url, "wb+")
        field.offset = 0
        field.upload_uri = upload_uri
        return field

    async def _append(self, cf: CloudFile, data: bytes):
        if self.field is None:
            raise AttributeError()

        if self._handler is None:
            raise AttributeError()

        await self._handler.write(data)

    async def append(self, cf: CloudFile, iterable: AsyncIterator) -> int:
        if self.field is None:
            raise AttributeError()
        count = 0
        async for chunk in iterable:
            await self._append(cf, chunk)
            size = len(chunk)
            count += size
            self.field.offset += len(chunk)
        return count

    async def finish(self):
        if self.field.old_uri not in ("", None):
            # Already has a file
            await self.storage.delete_upload(self.field.uri, self.field.bucket_name)
        if self.field.upload_uri != self.key:
            await self.move(
                self.field.upload_uri, self.key, self.field.bucket_name, self.bucket
            )

        await self._handler.close()
        self.field.uri = self.key
        self.field.ClearField("offset")
        self.field.ClearField("upload_uri")

    async def exists(self) -> Optional[Dict[str, str]]:
        if os.path.exists(self.metadata_key()):
            async with aiofiles.open(self.metadata_key(), "r") as metadata:
                return json.loads(await metadata.read())
        return {}

    async def upload(self, iterator: AsyncIterator, origin: CloudFile) -> CloudFile:
        self.field = await self.start(origin)
        if self.field is None:
            raise AttributeError()
        await self.append(origin, iterator)
        await self.finish()
        return self.field

    def __repr__(self):
        return f"{self.storage.source}: {self.bucket}/{self.key}"


class LocalStorage(Storage):
    field_klass = LocalStorageField

    def __init__(self, local_testing_files: str):
        self.local_testing_files = local_testing_files.rstrip("/")
        self.bucket_format = "ndb_{kbid}"
        self.source = CloudFile.LOCAL

    async def initialize(self):
        pass

    async def finalize(self):
        pass

    def get_bucket_name(self, kbid: str):
        return self.bucket_format.format(kbid=kbid)

    def get_bucket_path(self, bucket: str):
        return f"{self.local_testing_files}/{bucket}"

    async def create_kb(self, kbid: str):
        bucket = self.get_bucket_name(kbid)
        path = self.get_bucket_path(bucket)
        try:
            os.makedirs(path, exist_ok=True)
            created = True
        except FileExistsError:
            created = False
        return created

    async def delete_kb(self, kbid: str):
        bucket = self.get_bucket_name(kbid)
        path = self.get_bucket_path(bucket)
        try:
            shutil.rmtree(path)
            deleted = True
        except Exception:
            deleted = False
        return deleted

    async def delete_upload(self, uri: str, bucket_name: str):
        path = self.get_bucket_path(bucket_name)
        file_path = f"{path}/{uri}"
        os.remove(file_path)

    async def schedule_delete_kb(self, kbid: str):
        bucket = self.get_bucket_name(kbid)
        path = self.get_bucket_path(bucket)
        try:
            shutil.rmtree(path)
            deleted = True
        except Exception:
            deleted = False
        return deleted

    async def iterate_bucket(self, bucket: str, prefix: str) -> AsyncIterator[Any]:
        for key in glob.glob(f"{bucket}/{prefix}*"):
            item = {"name": key}
            yield item

    async def download(
        self, bucket_name: str, key: str, headers: Optional[Dict[str, str]] = None
    ):
        path = self.get_bucket_path(bucket_name)
        key_path = f"{path}/{key}"
        if not os.path.exists(key_path):
            return

        async with aiofiles.open(key_path, mode="rb") as f:
            while True:
                body = await f.read(CHUNK_SIZE)
                if body == b"" or body is None:
                    break
                else:
                    yield body
