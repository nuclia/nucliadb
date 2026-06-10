# Copyright 2021 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import abc
from collections.abc import AsyncGenerator, AsyncIterator

from nucliadb_utils.storages.utils import ObjectInfo, ObjectMetadata, Range


class ObjectStore(abc.ABC, metaclass=abc.ABCMeta):
    """
    Generic interface for object storage services.
    This must NOT include any NucliaDB/Nuclia specific logic.
    """

    @abc.abstractmethod
    async def initialize(self) -> None: ...

    @abc.abstractmethod
    async def finalize(self) -> None: ...

    @abc.abstractmethod
    async def bucket_create(self, bucket: str, labels: dict[str, str] | None = None) -> bool:
        """
        Create a new bucket in the object storage. Labels the bucket with the given labels if provided.
        Returns True if the bucket was created, False if it already existed.
        """
        ...

    @abc.abstractmethod
    async def bucket_exists(self, bucket: str) -> bool:
        """
        Return True if the bucket exists, False otherwise.
        """
        ...

    @abc.abstractmethod
    async def bucket_delete(self, bucket: str) -> tuple[bool, bool]:
        """
        Delete a bucket in the object storage. Returns a tuple with two boolean values:
        - The first one indicates if the bucket was deleted.
        - The second one indicates if there was a conflict.
        """
        ...

    @abc.abstractmethod
    async def bucket_schedule_delete(self, bucket: str) -> None:
        """
        Mark a bucket for deletion. The bucket will be deleted asynchronously.
        """
        ...

    @abc.abstractmethod
    async def move(
        self,
        origin_bucket: str,
        origin_key: str,
        destination_bucket: str,
        destination_key: str,
    ) -> None: ...

    @abc.abstractmethod
    async def copy(
        self,
        origin_bucket: str,
        origin_key: str,
        destination_bucket: str,
        destination_key: str,
    ) -> None: ...

    @abc.abstractmethod
    async def delete(self, bucket: str, key: str) -> None: ...

    @abc.abstractmethod
    async def upload(
        self,
        bucket: str,
        key: str,
        data: bytes | AsyncGenerator[bytes, None],
        metadata: ObjectMetadata,
    ) -> None: ...

    @abc.abstractmethod
    async def insert(
        self,
        bucket: str,
        key: str,
        data: bytes,
    ) -> None:
        """
        Insert data to the object storage without any metadata
        """
        ...

    @abc.abstractmethod
    async def download(self, bucket: str, key: str) -> bytes: ...

    @abc.abstractmethod
    async def download_stream(
        self, bucket: str, key: str, range: Range | None = None
    ) -> AsyncGenerator[bytes, None]:
        raise NotImplementedError()
        yield b""

    @abc.abstractmethod
    async def iterate(
        self, bucket: str, prefix: str, start: str | None = None
    ) -> AsyncGenerator[ObjectInfo, None]:
        raise NotImplementedError()
        yield ObjectInfo(name="")

    @abc.abstractmethod
    async def get_metadata(self, bucket: str, key: str) -> ObjectMetadata: ...

    @abc.abstractmethod
    async def upload_multipart_start(
        self, bucket: str, key: str, metadata: ObjectMetadata
    ) -> str | None:
        """
        Start a multipart upload. May return the url for the resumable upload.
        """

    @abc.abstractmethod
    async def upload_multipart_append(
        self, bucket: str, key: str, iterable: AsyncIterator[bytes]
    ) -> int:
        """
        Append data to a multipart upload. Returns the number of bytes uploaded.
        """

    @abc.abstractmethod
    async def upload_multipart_finish(self, bucket: str, key: str) -> None: ...
