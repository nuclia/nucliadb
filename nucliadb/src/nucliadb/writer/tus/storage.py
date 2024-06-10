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

from typing import AsyncIterator, Optional

from nucliadb.writer.tus.dm import FileDataManager
from nucliadb_protos.resources_pb2 import CloudFile


class BlobStore:
    bucket: str
    source: CloudFile.Source.V

    async def initialize(self, *args, **kwargs):
        pass

    async def finalize(self):
        pass

    async def create_bucket(self, bucket_name: str) -> bool:
        raise NotImplementedError()

    async def check_exists(self, bucket_name: str) -> bool:
        raise NotImplementedError()

    def get_bucket_name(self, kbid: str) -> str:
        return self.bucket.format(kbid=kbid)


class FileStorageManager:
    chunk_size: int
    min_upload_size: Optional[int] = None

    def __init__(self, storage: BlobStore):
        self.storage = storage

    def iter_data(
        self, uri: str, kbid: str, headers: Optional[dict[str, str]] = None
    ) -> AsyncIterator[bytes]:
        raise NotImplementedError()

    async def start(self, dm: FileDataManager, path: str, kbid: str):
        raise NotImplementedError()

    async def append(self, dm: FileDataManager, iterable, offset) -> int:
        raise NotImplementedError()

    async def finish(self, dm: FileDataManager):
        raise NotImplementedError()

    async def delete_upload(self, uri, kbid):
        raise NotImplementedError()

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

    def validate_intermediate_chunk(self, uploaded_bytes: int):
        raise NotImplementedError()
