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

import abc
from typing import AsyncGenerator, AsyncIterator, Optional, Union

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
    async def bucket_create(self, bucket: str, labels: Optional[dict[str, str]] = None) -> bool:
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
        data: Union[bytes, AsyncGenerator[bytes, None]],
        metadata: ObjectMetadata,
    ) -> None: ...

    @abc.abstractmethod
    async def download(self, bucket: str, key: str) -> bytes: ...

    @abc.abstractmethod
    async def download_stream(
        self, bucket: str, key: str, range: Optional[Range] = None
    ) -> AsyncGenerator[bytes, None]:
        raise NotImplementedError()
        yield b""

    @abc.abstractmethod
    async def iterate(self, bucket: str, prefix: str) -> AsyncGenerator[ObjectInfo, None]:
        raise NotImplementedError()
        yield ObjectInfo(name="")

    @abc.abstractmethod
    async def get_metadata(self, bucket: str, key: str) -> ObjectMetadata: ...

    @abc.abstractmethod
    async def upload_multipart_start(
        self, bucket: str, key: str, metadata: ObjectMetadata
    ) -> Optional[str]:
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
