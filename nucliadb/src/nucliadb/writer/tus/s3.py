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

import uuid
from contextlib import AsyncExitStack
from typing import Optional

import aiobotocore  # type: ignore
import aiohttp
import backoff
import botocore  # type: ignore
from aiobotocore.session import AioSession  # type: ignore

from nucliadb.writer import logger
from nucliadb.writer.tus.dm import FileDataManager
from nucliadb.writer.tus.exceptions import ResumableURINotAvailable
from nucliadb.writer.tus.storage import BlobStore, FileStorageManager
from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_utils.storages.s3 import (
    CHUNK_SIZE,
    MIN_UPLOAD_SIZE,
    bucket_exists,
    create_bucket,
)

RETRIABLE_EXCEPTIONS = (
    botocore.exceptions.ClientError,
    aiohttp.client_exceptions.ClientPayloadError,
    botocore.exceptions.BotoCoreError,
)


class S3FileStorageManager(FileStorageManager):
    storage: S3BlobStore
    chunk_size = CHUNK_SIZE
    min_upload_size = MIN_UPLOAD_SIZE

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, jitter=backoff.random_jitter, max_tries=3)
    async def _abort_multipart(self, dm: FileDataManager):
        try:
            mpu = dm.get("mpu")
            upload_file_id = dm.get("upload_file_id")
            await self.storage._s3aioclient.abort_multipart_upload(
                Bucket=self.storage.bucket, Key=upload_file_id, UploadId=mpu["UploadId"]
            )
        except Exception:
            logger.warning("Could not abort multipart upload", exc_info=True)

    async def start(self, dm: FileDataManager, path: str, kbid: str):
        bucket = self.storage.get_bucket_name(kbid)
        upload_file_id = dm.get("upload_file_id", str(uuid.uuid4()))
        if dm.get("mpu") is not None:
            await self._abort_multipart(dm)

        custom_metadata: dict[str, str] = {
            "filename": dm.filename or "",
            "content_type": dm.content_type or "",
            "size": str(dm.size),
        }

        await dm.update(
            path=path,
            upload_file_id=upload_file_id,
            multipart={"Parts": []},
            block=1,
            mpu=await self._create_multipart(path, bucket, custom_metadata),
            bucket=bucket,
        )

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, jitter=backoff.random_jitter, max_tries=3)
    async def _create_multipart(self, path, bucket, custom_metadata: dict[str, str]):
        return await self.storage._s3aioclient.create_multipart_upload(
            Bucket=bucket, Key=path, Metadata=custom_metadata
        )

    async def append(self, dm: FileDataManager, iterable, offset) -> int:
        size = 0
        async for chunk in iterable:
            # It seems that starlette stream() finishes with an emtpy chunk of data
            size += len(chunk)
            part = await self._upload_part(dm, chunk)
            multipart = dm.get("multipart")
            multipart["Parts"].append({"PartNumber": dm.get("block"), "ETag": part["ETag"]})
            await dm.update(multipart=multipart, block=dm.get("block") + 1)

        return size

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, jitter=backoff.random_jitter, max_tries=3)
    async def _upload_part(self, dm: FileDataManager, data):
        mpu = dm.get("mpu")
        if mpu is None:
            # If we don't have an ongoing multipart upload for the current upload_id
            # we need to abort the request.
            raise ResumableURINotAvailable()

        return await self.storage._s3aioclient.upload_part(
            Bucket=dm.get("bucket"),
            Key=dm.get("path"),
            PartNumber=dm.get("block"),
            UploadId=dm.get("mpu")["UploadId"],
            Body=data,
        )

    async def finish(self, dm: FileDataManager):
        path = dm.get("path")
        if dm.get("mpu") is not None:
            await self._complete_multipart_upload(dm)
        await dm.finish()
        return path

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, jitter=backoff.random_jitter, max_tries=3)
    async def _complete_multipart_upload(self, dm: FileDataManager):
        # if blocks is 0, it means the file is of zero length so we need to
        # trick it to finish a multiple part with no data.
        if dm.get("block") == 1:
            part = await self._upload_part(dm, b"")
            multipart = dm.get("multipart")
            multipart["Parts"].append({"PartNumber": dm.get("block"), "ETag": part["ETag"]})
            await dm.update(multipart=multipart, block=dm.get("block") + 1)
        await self.storage._s3aioclient.complete_multipart_upload(
            Bucket=dm.get("bucket"),
            Key=dm.get("path"),
            UploadId=dm.get("mpu")["UploadId"],
            MultipartUpload=dm.get("multipart"),
        )

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, jitter=backoff.random_jitter, max_tries=3)
    async def _download(self, uri: str, kbid: str, **kwargs):
        bucket = self.storage.get_bucket_name(kbid)
        return await self.storage._s3aioclient.get_object(Bucket=bucket, Key=uri, **kwargs)

    async def delete_upload(self, uri: str, kbid: str):
        bucket = self.storage.get_bucket_name(kbid)
        if uri is not None:
            try:
                await self.storage._s3aioclient.delete_object(Bucket=bucket, Key=uri)
            except botocore.exceptions.ClientError:
                logger.warning("Error deleting object", exc_info=True)
        else:
            raise AttributeError("No valid uri")

    def validate_intermediate_chunk(self, uploaded_bytes: int):
        if uploaded_bytes % self.min_upload_size != 0:
            raise ValueError(f"Intermediate chunks need to be multiples of {self.min_upload_size} bytes")


class S3BlobStore(BlobStore):
    async def check_exists(self, bucket_name: str) -> bool:
        return await bucket_exists(self._s3aioclient, bucket_name)

    def get_bucket_name(self, kbid: str) -> str:
        bucket_name = super().get_bucket_name(kbid)
        if bucket_name is not None:
            return bucket_name.replace("_", "-")
        else:
            return bucket_name

    async def create_bucket(self, bucket):
        exists = await self.check_exists(bucket)
        if not exists:
            await create_bucket(self._s3aioclient, bucket, self.bucket_tags, self.region_name)
        return exists

    async def finalize(self):
        await self._exit_stack.__aexit__(None, None, None)

    async def initialize(
        self,
        client_id,
        client_secret,
        ssl,
        verify_ssl,
        max_pool_connections,
        endpoint_url,
        region_name,
        bucket,
        bucket_tags: Optional[dict[str, str]] = None,
    ):
        self.bucket = bucket
        self.bucket_tags = bucket_tags
        self.source = CloudFile.Source.S3
        self.region_name = region_name

        self._exit_stack = AsyncExitStack()

        self.opts = dict(
            aws_secret_access_key=client_secret,
            aws_access_key_id=client_id,
            endpoint_url=endpoint_url,
            verify=verify_ssl,
            use_ssl=ssl,
            region_name=region_name,
            config=aiobotocore.config.AioConfig(None, max_pool_connections=max_pool_connections),
        )
        session = AioSession()
        self._s3aioclient = await self._exit_stack.enter_async_context(
            session.create_client("s3", **self.opts)
        )
