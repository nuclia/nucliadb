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

from typing import AsyncIterator, Dict, List, Optional

from nucliadb_protos.resources_pb2 import CloudFile
from starlette.responses import StreamingResponse

from nucliadb_writer import logger
from nucliadb_writer.tus.dm import RedisFileDataManager
from nucliadb_writer.tus.exceptions import HTTPRangeNotSatisfiable


class BlobStore:
    bucket: str
    source: CloudFile.Source.V
    cached_buckets: List[str] = []

    async def create_bucket(self, bucket_name: str):
        raise NotImplementedError()

    async def get_bucket_name(self, kbid: str):
        bucket = self.bucket.format(kbid=kbid)
        if bucket in self.cached_buckets:
            return bucket
        found = False
        try:
            found = await self.create_bucket(bucket)
            self.cached_buckets.append(bucket)
        except Exception:
            logger.exception(f"Could not create bucket {bucket}", exc_info=True)
        if found is True:
            logger.info(f"Already exists {bucket}")
        return bucket


class FileStorageManager:
    chunk_size: int

    def __init__(self, storage):
        self.storage = storage

    def read_range(
        self, uri: str, kbid: str, start: int, end: int
    ) -> AsyncIterator[bytes]:
        raise NotImplementedError()

    def iter_data(
        self, uri: str, kbid: str, headers: Optional[Dict[str, str]] = None
    ) -> AsyncIterator[bytes]:
        raise NotImplementedError()

    async def start(self, dm: RedisFileDataManager, path: str, kbid: str):
        raise NotImplementedError()

    async def append(self, dm: RedisFileDataManager, iterable, offset) -> int:
        raise NotImplementedError()

    async def finish(self, dm: RedisFileDataManager):
        raise NotImplementedError()

    async def get_file_metadata(self, uri: str, kbid: str):
        raise NotImplementedError()

    async def full_download(self, content_length, content_type, upload_id):
        return StreamingResponse(
            self.iter_data(upload_id),
            media_type=content_type,
            headers={
                "Content-Length": str(content_length),
                "Content-Type": content_type,
            },
        )

    async def range_download(
        self, content_length, content_type, upload_id, range_header
    ):
        try:
            start, _, end = range_header.split("bytes=")[-1].partition("-")
            start = int(start)
            if len(end) == 0:
                # bytes=0- is valid
                end = content_length - 1
            end = int(end) + 1  # python is inclusive, http is exclusive
        except (IndexError, ValueError):
            # range errors fallback to full download
            raise HTTPRangeNotSatisfiable(detail=f"Range not parsable {range_header}")
        if start > end or start < 0:
            raise HTTPRangeNotSatisfiable(detail="Invalid range {start}-{end}")
        if end > content_length:
            raise HTTPRangeNotSatisfiable(
                detail="Invalid range {start}-{end}, too large end value"
            )

        logger.debug(f"Range request: {range_header}")
        headers = {
            "Content-Range": f"bytes {start}-{end - 1}/{content_length}",
            "Content-Type": content_type,
        }

        return StreamingResponse(
            self.read_range(upload_id, start, end),
            media_type=content_type,
            headers=headers,
        )

    async def iterate_body_chunks(self, request, chunk_size):
        partial = b""
        remaining = b""
        async for chunk in request.stream():
            if len(chunk) == 0:
                continue

            partial += remaining[:chunk_size]
            remaining = b""

            wanted = chunk_size - len(partial)
            partial += chunk[:wanted]

            remaining = chunk[wanted:]

            if len(partial) < chunk_size:
                continue

            yield partial
            partial = b""

        if partial or remaining:
            yield partial + remaining
