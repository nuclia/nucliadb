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
from typing import AsyncIterator, Dict, Optional

import aiobotocore  # type: ignore
import aiohttp
import backoff  # type: ignore
import botocore  # type: ignore
from aiobotocore.session import AioSession  # type: ignore
from nucliadb_protos.resources_pb2 import CloudFile

from nucliadb.writer import logger
from nucliadb.writer.tus.dm import FileDataMangaer
from nucliadb.writer.tus.exceptions import CloudFileNotFound
from nucliadb.writer.tus.storage import BlobStore, FileStorageManager

RETRIABLE_EXCEPTIONS = (
    botocore.exceptions.ClientError,
    aiohttp.client_exceptions.ClientPayloadError,
    botocore.exceptions.BotoCoreError,
)
CHUNK_SIZE = 5 * 1024 * 1024


class S3FileStorageManager(FileStorageManager):
    storage: S3BlobStore
    chunk_size = CHUNK_SIZE

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=3)
    async def _abort_multipart(self, dm: FileDataMangaer):
        try:
            mpu = dm.get("mpu")
            upload_file_id = dm.get("upload_file_id")
            await self.storage._s3aioclient.abort_multipart_upload(
                Bucket=self.storage.bucket, Key=upload_file_id, UploadId=mpu["UploadId"]
            )
        except Exception:
            logger.warn("Could not abort multipart upload", exc_info=True)

    async def start(self, dm: FileDataMangaer, path: str, kbid: str):
        bucket = self.storage.get_bucket_name(kbid)
        upload_file_id = dm.get("upload_file_id", str(uuid.uuid4()))
        if dm.get("mpu") is not None:
            await self._abort_multipart(dm)

        await dm.update(
            path=path,
            upload_file_id=upload_file_id,
            multipart={"Parts": []},
            block=1,
            mpu=await self._create_multipart(path, bucket),
            bucket=bucket,
        )

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=3)
    async def _create_multipart(self, path, bucket):
        return await self.storage._s3aioclient.create_multipart_upload(
            Bucket=bucket, Key=path
        )

    async def append(self, dm: FileDataMangaer, iterable, offset) -> int:
        size = 0
        async for chunk in iterable:
            # It seems that starlette stream() finishes with an emtpy chunk of data
            size += len(chunk)
            part = await self._upload_part(dm, chunk)

            multipart = dm.get("multipart")
            multipart["Parts"].append(
                {"PartNumber": dm.get("block"), "ETag": part["ETag"]}
            )
            await dm.update(multipart=multipart, block=dm.get("block") + 1)

        return size

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=3)
    async def _upload_part(self, dm: FileDataMangaer, data):
        return await self.storage._s3aioclient.upload_part(
            Bucket=dm.get("bucket"),
            Key=dm.get("path"),
            PartNumber=dm.get("block"),
            UploadId=dm.get("mpu")["UploadId"],
            Body=data,
        )

    async def finish(self, dm: FileDataMangaer):
        if dm.get("mpu") is not None:
            await self._complete_multipart_upload(dm)
        await dm.finish()

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=3)
    async def _complete_multipart_upload(self, dm: FileDataMangaer):
        # if blocks is 0, it means the file is of zero length so we need to
        # trick it to finish a multiple part with no data.
        if dm.get("block") == 1:
            part = await self._upload_part(dm, b"")
            multipart = dm.get("multipart")
            multipart["Parts"].append(
                {"PartNumber": dm.get("block"), "ETag": part["ETag"]}
            )
            await dm.update(multipart=multipart, block=dm.get("block") + 1)
        await self.storage._s3aioclient.complete_multipart_upload(
            Bucket=dm.get("bucket"),
            Key=dm.get("path"),
            UploadId=dm.get("mpu")["UploadId"],
            MultipartUpload=dm.get("multipart"),
        )

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=3)
    async def _download(self, uri: str, kbid: str, **kwargs):
        bucket = self.storage.get_bucket_name(kbid)
        return await self.storage._s3aioclient.get_object(
            Bucket=bucket, Key=uri, **kwargs
        )

    async def iter_data(
        self, uri: str, kbid: str, headers: Optional[Dict[str, str]] = None
    ):
        if headers is None:
            headers = {}
        try:
            downloader = await self._download(uri, kbid, **headers)
        except self.storage._s3aioclient.exceptions.NoSuchKey:
            raise CloudFileNotFound()

        # we do not want to timeout ever from this...
        # downloader['Body'].set_socket_timeout(999999)
        stream = downloader["Body"]
        data = await stream.read(CHUNK_SIZE)
        while True:
            if not data:
                break
            yield data
            data = await stream.read(CHUNK_SIZE)

    async def read_range(
        self, uri, kbid: str, start: int, end: int
    ) -> AsyncIterator[bytes]:
        """
        Iterate through ranges of data
        """
        async for chunk in self.iter_data(
            uri, kbid, headers={"Range": f"bytes={start}-{end - 1}"}
        ):
            yield chunk

    async def delete_upload(self, uri: str, kbid: str):
        bucket = self.storage.get_bucket_name(kbid)
        if uri is not None:
            try:
                await self.storage._s3aioclient.delete_object(Bucket=bucket, Key=uri)
            except botocore.exceptions.ClientError:
                logger.warn("Error deleting object", exc_info=True)
        else:
            raise AttributeError("No valid uri")


class S3BlobStore(BlobStore):
    async def check_exists(self, bucket_name: str) -> bool:
        exists = True
        try:
            res = await self._s3aioclient.head_bucket(Bucket=bucket_name)
            if res["ResponseMetadata"]["HTTPStatusCode"] == 404:
                exists = False
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                exists = False
        return exists

    def get_bucket_name(self, kbid: str) -> str:
        bucket_name = super().get_bucket_name(kbid)
        if bucket_name is not None:
            return bucket_name.replace("_", "-")
        else:
            return bucket_name

    async def create_bucket(self, bucket):
        exists = await self.check_exists(bucket)
        if not exists:
            await self._s3aioclient.create_bucket(Bucket=bucket)
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
    ):
        self.bucket = bucket
        self.source = CloudFile.Source.GCS

        self._exit_stack = AsyncExitStack()

        self.opts = dict(
            aws_secret_access_key=client_secret,
            aws_access_key_id=client_id,
            endpoint_url=endpoint_url,
            verify=verify_ssl,
            use_ssl=ssl,
            region_name=region_name,
            config=aiobotocore.config.AioConfig(
                None, max_pool_connections=max_pool_connections
            ),
        )
        session = AioSession()
        self._s3aioclient = await self._exit_stack.enter_async_context(
            session.create_client("s3", **self.opts)
        )
