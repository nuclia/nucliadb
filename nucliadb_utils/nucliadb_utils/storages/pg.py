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

import asyncio
import uuid
from typing import Any, AsyncIterator, Optional, TypedDict

import asyncpg
from nucliadb_protos.resources_pb2 import CloudFile

from nucliadb_utils.storages import CHUNK_SIZE
from nucliadb_utils.storages.storage import Storage, StorageField

# Table design notes
# - No foreign key constraints ON PURPOSE
# - No cascade handling ON PURPOSE
CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS kb_files (
    kb_id TEXT,
    file_id TEXT,
    filename TEXT,
    size INTEGER,
    content_type TEXT,
    PRIMARY KEY(kb_id, file_id)
);

CREATE TABLE IF NOT EXISTS kb_files_fileparts (
    kb_id TEXT,
    file_id TEXT,
    part_id INTEGER,
    size INTEGER,
    data BYTEA,
    PRIMARY KEY(kb_id, file_id, part_id)
);
"""


class FileInfo(TypedDict):
    filename: str
    size: int
    content_type: str
    key: str


class ChunkInfo(TypedDict):
    part_id: int
    size: int


class Chunk(ChunkInfo):
    data: bytes


class PostgresFileDataLayer:
    """
    Responsible for interating with the database and
    abstracting any sql and connection management.
    """

    def __init__(self, connection: asyncpg.Connection):
        self.connection = connection

    async def initialize_kb(self, kbid: str) -> bool:
        # there's really no record keeping or init
        # per kb that we care to do
        return True

    async def delete_kb(self, kbid: str) -> bool:
        async with self.connection.transaction():
            await self.connection.execute(
                """
DELETE FROM kb_files
WHERE kb_id = $1
""",
                kbid,
            )
            await self.connection.execute(
                """
DELETE FROM kb_files_fileparts
WHERE kb_id = $1
""",
                kbid,
            )
        return True

    async def create_file(
        self, *, kb_id: str, file_id: str, filename: str, size: int, content_type: str
    ) -> None:
        async with self.connection.transaction():
            await self.connection.execute(
                """
INSERT INTO kb_files (kb_id, file_id, filename, size, content_type)
VALUES ($1, $2, $3, $4, $5)
""",
                kb_id,
                file_id,
                filename,
                size,
                content_type,
            )

    async def delete_file(self, kb_id: str, file_id: str) -> None:
        async with self.connection.transaction():
            await self.connection.execute(
                """
DELETE FROM kb_files
WHERE kb_id = $1 AND file_id = $2
""",
                kb_id,
                file_id,
            )
            await self.connection.execute(
                """
DELETE FROM kb_files_fileparts
WHERE kb_id = $1 AND file_id = $2
""",
                kb_id,
                file_id,
            )

    async def append_chunk(self, *, kb_id: str, file_id: str, data: bytes) -> None:
        async with self.connection.transaction():
            await self.connection.execute(
                """
INSERT INTO kb_files_fileparts (kb_id, file_id, part_id, data, size)
VALUES ($1, $2, (SELECT COALESCE(MAX(part_id), 0) + 1 FROM kb_files_fileparts WHERE kb_id = $1 AND file_id = $2), $3, $4)
""",
                kb_id,
                file_id,
                data,
                len(data),
            )

    async def get_file_info(self, kb_id: str, file_id: str) -> Optional[FileInfo]:
        record = await self.connection.fetchrow(
            """
SELECT filename, size, content_type, file_id
FROM kb_files
WHERE kb_id = $1 AND file_id = $2
""",
            kb_id,
            file_id,
        )
        if record is None:
            return None
        return FileInfo(
            filename=record["filename"],
            size=record["size"],
            content_type=record["content_type"],
            key=record["file_id"],
        )

    async def move(
        self,
        *,
        origin_key: str,
        destination_key: str,
        origin_kb: str,
        destination_kb: str,
    ):
        async with self.connection.transaction():
            await self.connection.execute(
                """
UPDATE kb_files
SET kb_id = $1, file_id = $2
WHERE kb_id = $3 AND file_id = $4
""",
                destination_kb,
                destination_key,
                origin_kb,
                origin_key,
            )

            await self.connection.execute(
                """
UPDATE kb_files_fileparts
SET kb_id = $1, file_id = $2
WHERE kb_id = $3 AND file_id = $4
""",
                destination_kb,
                destination_key,
                origin_kb,
                origin_key,
            )

    async def copy(
        self,
        *,
        origin_key: str,
        destination_key: str,
        origin_kb: str,
        destination_kb: str,
    ):
        async with self.connection.transaction():
            await self.connection.execute(
                """
INSERT INTO kb_files (kb_id, file_id, filename, size, content_type)
SELECT $1, $2, filename, size, content_type
FROM kb_files
WHERE kb_id = $3 AND file_id = $4
""",
                destination_kb,
                destination_key,
                origin_kb,
                origin_key,
            )

            await self.connection.execute(
                """
INSERT INTO kb_files_fileparts (kb_id, file_id, part_id, data, size)
SELECT $1, $2, part_id, data, size
FROM kb_files_fileparts
WHERE kb_id = $3 AND file_id = $4
""",
                destination_kb,
                destination_key,
                origin_kb,
                origin_key,
            )

    async def get_chunks_info(
        self, bucket: str, key: str, part_ids: Optional[list[int]] = None
    ) -> list[ChunkInfo]:
        query = """
select kb_id, file_id, part_id, size
from kb_files_fileparts
where kb_id = $1 and file_id = $2
"""
        args: list[Any] = [bucket, key]
        if part_ids is not None:
            query += " and part_id = ANY($3)"
            args.append(part_ids)
        query += " order by part_id"
        chunks = await self.connection.fetch(query, *args)
        return [
            ChunkInfo(
                part_id=chunk["part_id"],
                size=chunk["size"],
            )
            for chunk in chunks
        ]

    async def iterate_kb(
        self, bucket: str, prefix: Optional[str] = None
    ) -> AsyncIterator[FileInfo]:
        query = """
SELECT filename, size, content_type, file_id
FROM kb_files
WHERE kb_id = $1
"""
        args: list[Any] = [bucket]
        if prefix:
            query += " AND filename LIKE $2"
            args.append(prefix + "%")
        async with self.connection.transaction():
            async for record in self.connection.cursor(query, *args):
                yield FileInfo(
                    filename=record["filename"],
                    size=record["size"],
                    content_type=record["content_type"],
                    key=record["file_id"],
                )

    async def iterate_chunks(
        self, bucket: str, key: str, part_ids: Optional[list[int]] = None
    ) -> AsyncIterator[Chunk]:
        chunks = await self.get_chunks_info(bucket, key, part_ids=part_ids)
        for chunk in chunks:
            # who knows how long a download for one of these chunks could be,
            # so let's not try to keep a txn or cursor open.
            data_chunk = await self.connection.fetchrow(
                """
select data
from kb_files_fileparts
where kb_id = $1 and file_id = $2 and part_id = $3
""",
                bucket,
                key,
                chunk["part_id"],
            )
            yield Chunk(
                part_id=chunk["part_id"],
                size=chunk["size"],
                data=data_chunk["data"],
            )

    async def iterate_range(
        self, *, kb_id: str, file_id: str, start: int, end: int
    ) -> AsyncIterator[bytes]:
        chunks = await self.get_chunks_info(
            kb_id,
            file_id,
        )

        # First off, find start part and position
        elapsed = 0
        start_part_id = None
        start_pos = -1
        for chunk in chunks:
            if elapsed + chunk["size"] > start:
                start_part_id = chunk["part_id"]
                start_pos = start - elapsed
                break
            else:
                elapsed += chunk["size"]

        if start_part_id is None:
            return

        # Now, iterate through the chunks and yield the data
        read_bytes = 0
        while read_bytes < end - start:
            data_chunk = await self.connection.fetchrow(
                """
select data
from kb_files_fileparts
where kb_id = $1 and file_id = $2 and part_id = $3
""",
                kb_id,
                file_id,
                start_part_id,
            )
            if data_chunk is None:
                return

            data = data_chunk["data"][
                start_pos : min(
                    start_pos + ((end - start) - read_bytes), len(data_chunk["data"])
                )
            ]
            read_bytes += len(data)
            yield data
            start_pos = 0
            start_part_id += 1


class PostgresStorageField(StorageField):
    storage: PostgresStorage

    async def move(
        self,
        origin_uri: str,
        destination_uri: str,
        origin_bucket_name: str,
        destination_bucket_name: str,
    ):
        async with self.storage.pool.acquire() as conn:
            dl = PostgresFileDataLayer(conn)
            return await dl.move(
                origin_key=origin_uri,
                destination_key=destination_uri,
                origin_kb=origin_bucket_name,
                destination_kb=destination_bucket_name,
            )

    async def copy(
        self,
        origin_uri: str,
        destination_uri: str,
        origin_bucket_name: str,
        destination_bucket_name: str,
    ):
        async with self.storage.pool.acquire() as conn:
            dl = PostgresFileDataLayer(conn)
            return await dl.copy(
                origin_key=origin_uri,
                destination_key=destination_uri,
                origin_kb=origin_bucket_name,
                destination_kb=destination_bucket_name,
            )

    async def iter_data(self, headers=None):
        key = self.field.uri if self.field else self.key
        if self.field is None:
            bucket = self.bucket
        else:
            bucket = self.field.bucket_name

        async with self.storage.pool.acquire() as conn:
            dl = PostgresFileDataLayer(conn)
            async for chunk in dl.iterate_chunks(bucket, key):
                yield chunk["data"]

    async def read_range(self, start: int, end: int) -> AsyncIterator[bytes]:
        """
        Iterate through ranges of data
        """
        key = self.field.uri if self.field else self.key
        if self.field is None:
            bucket = self.bucket
        else:
            bucket = self.field.bucket_name

        async with self.storage.pool.acquire() as conn:
            dl = PostgresFileDataLayer(conn)
            async for data in dl.iterate_range(
                kb_id=bucket, file_id=key, start=start, end=end
            ):
                yield data

    async def start(self, cf: CloudFile) -> CloudFile:
        field = CloudFile(
            filename=cf.filename,
            size=cf.size,
            md5=cf.md5,
            content_type=cf.content_type,
            bucket_name=self.bucket,
            source=CloudFile.POSTGRES,
        )
        upload_uri = uuid.uuid4().hex

        async with self.storage.pool.acquire() as conn:
            async with conn.transaction():
                dl = PostgresFileDataLayer(conn)

                if self.field is not None and self.field.upload_uri != "":
                    # If there is a temporal url
                    await dl.delete_file(self.field.bucket_name, self.field.upload_uri)

                await dl.create_file(
                    kb_id=self.bucket,
                    file_id=upload_uri,
                    filename=cf.filename,
                    size=cf.size,
                    content_type=cf.content_type,
                )

                field.offset = 0
                field.upload_uri = upload_uri
                return field

    async def append(self, cf: CloudFile, iterable: AsyncIterator) -> int:
        if self.field is None:
            raise AttributeError()
        count = 0
        async with self.storage.pool.acquire() as conn:
            dl = PostgresFileDataLayer(conn)
            async for chunk in iterable:
                await dl.append_chunk(
                    kb_id=self.bucket, file_id=cf.upload_uri, data=chunk
                )
                size = len(chunk)
                count += size
                self.field.offset += len(chunk)
        return count

    async def finish(self):
        async with self.storage.pool.acquire() as conn, conn.transaction():
            dl = PostgresFileDataLayer(conn)
            if self.field.old_uri not in ("", None):
                # Already has a file
                await dl.delete_file(self.bucket, self.field.bucket_name)

            if self.field.upload_uri != self.key:
                await dl.move(
                    origin_key=self.field.upload_uri,
                    destination_key=self.key,
                    origin_kb=self.field.bucket_name,
                    destination_kb=self.bucket,
                )

        self.field.uri = self.key
        self.field.ClearField("offset")
        self.field.ClearField("upload_uri")

    async def exists(self) -> Optional[FileInfo]:  # type:ignore
        async with self.storage.pool.acquire() as conn:
            dl = PostgresFileDataLayer(conn)
            return await dl.get_file_info(self.bucket, self.key)

    async def upload(self, iterator: AsyncIterator, origin: CloudFile) -> CloudFile:
        self.field = await self.start(origin)
        await self.append(origin, iterator)
        await self.finish()
        return self.field

    def __repr__(self):
        return f"{self.storage.source}: {self.bucket}/{self.key}"


class PostgresStorage(Storage):
    field_klass = PostgresStorageField
    chunk_size = CHUNK_SIZE
    pool: asyncpg.pool.Pool

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.source = CloudFile.POSTGRES
        self._lock = asyncio.Lock()
        self.initialized = False

    async def initialize(self):
        async with self._lock:
            if self.initialized is False:
                self.pool = await asyncpg.create_pool(self.dsn)

                # check if table exists
                async with self.pool.acquire() as conn:
                    await conn.execute(CREATE_TABLE)

            self.initialized = True

    async def finalize(self):
        async with self._lock:
            await self.pool.close()
            self.initialized = False

    def get_bucket_name(self, kbid: str):
        return kbid

    async def create_kb(self, kbid: str):
        async with self.pool.acquire() as conn:
            dl = PostgresFileDataLayer(conn)
            return await dl.initialize_kb(kbid)

    async def delete_kb(self, kbid: str) -> tuple[bool, bool]:
        async with self.pool.acquire() as conn:
            dl = PostgresFileDataLayer(conn)
            return await dl.delete_kb(kbid), False

    async def delete_upload(self, uri: str, bucket_name: str):
        async with self.pool.acquire() as conn:
            dl = PostgresFileDataLayer(conn)
            await dl.delete_file(bucket_name, uri)

    async def schedule_delete_kb(self, kbid: str) -> bool:
        await self.delete_kb(kbid)
        return True

    async def iterate_bucket(self, bucket: str, prefix: str) -> AsyncIterator[Any]:
        async with self.pool.acquire() as conn:
            dl = PostgresFileDataLayer(conn)
            async for file_data in dl.iterate_kb(bucket, prefix):
                yield {"name": file_data["key"]}

    async def download(
        self, bucket_name: str, key: str, headers: Optional[dict[str, str]] = None
    ) -> AsyncIterator[bytes]:
        async with self.pool.acquire() as conn:
            dl = PostgresFileDataLayer(conn)
            async for chunk in dl.iterate_chunks(bucket_name, key):
                yield chunk["data"]
