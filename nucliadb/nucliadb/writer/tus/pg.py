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

from typing import AsyncIterator

import asyncpg
from nucliadb_protos.resources_pb2 import CloudFile

from nucliadb.writer.tus.dm import FileDataMangaer
from nucliadb.writer.tus.exceptions import CloudFileNotFound
from nucliadb.writer.tus.storage import BlobStore, FileStorageManager
from nucliadb_utils.storages import CHUNK_SIZE
from nucliadb_utils.storages.pg import PostgresFileDataLayer


class PGFileStorageManager(FileStorageManager):
    _handler = None
    storage: PGBlobStore
    chunk_size = CHUNK_SIZE

    async def start(self, dm: FileDataMangaer, path: str, kbid: str):
        bucket = self.storage.get_bucket_name(kbid)

        async with self.storage.pool.acquire() as conn:
            async with conn.transaction():
                dl = PostgresFileDataLayer(conn)
                if path is not None:
                    await self.delete_upload(kbid, path)

                await dl.create_file(
                    kb_id=bucket,
                    file_id=path,
                    filename=dm.filename,
                    size=dm.size,
                    content_type=dm.content_type,
                )

        await dm.update(upload_file_id=path, path=path, bucket=bucket)

    async def iter_data(self, uri, kbid: str, headers=None):
        bucket = self.storage.get_bucket_name(kbid)

        async with self.storage.pool.acquire() as conn:
            dl = PostgresFileDataLayer(conn)
            async for chunk in dl.iterate_chunks(bucket, uri):
                yield chunk["data"]

    async def read_range(
        self, uri: str, kbid: str, start: int, end: int
    ) -> AsyncIterator[bytes]:
        """
        Iterate through ranges of data
        """
        bucket = self.storage.get_bucket_name(kbid)

        async with self.storage.pool.acquire() as conn:
            dl = PostgresFileDataLayer(conn)
            file_info = await dl.get_file_info(kbid, uri)
            if file_info is None:
                raise CloudFileNotFound()
            async for data in dl.iterate_range(
                kb_id=bucket, file_id=uri, start=start, end=end
            ):
                yield data

    async def append(self, dm: FileDataMangaer, iterable, offset) -> int:
        bucket = dm.get("bucket")
        path = dm.get("path")
        count = 0
        async with self.storage.pool.acquire() as conn:
            dl = PostgresFileDataLayer(conn)
            async for chunk in iterable:
                await dl.append_chunk(kb_id=bucket, file_id=path, data=chunk)
                size = len(chunk)
                count += size
                offset += len(chunk)
        return count

    async def finish(self, dm: FileDataMangaer):
        path = dm.get("path")
        await dm.finish()
        return path

    async def delete_upload(self, uri: str, kbid: str):
        async with self.storage.pool.acquire() as conn:
            async with conn.transaction():
                dl = PostgresFileDataLayer(conn)
                await dl.delete_file(kbid, uri)


class PGBlobStore(BlobStore):
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.source = CloudFile.POSTGRES

    async def initialize(self):
        self.pool = await asyncpg.create_pool(self.dsn)

    async def finalize(self):
        await self.pool.close()
        self.initialized = False

    async def check_exists(self, bucket_name: str) -> bool:
        return True

    def get_bucket_name(self, kbid: str) -> str:
        return kbid
