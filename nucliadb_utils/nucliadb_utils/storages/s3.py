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

from contextlib import AsyncExitStack
from datetime import datetime
from typing import Any, AsyncIterator, Optional

import aiobotocore  # type: ignore
import aiohttp
import backoff  # type: ignore
import botocore  # type: ignore
from aiobotocore.session import AioSession, get_session  # type: ignore
from nucliadb_protos.resources_pb2 import CloudFile

from nucliadb_utils import logger
from nucliadb_utils.storages.storage import Storage, StorageField

MAX_SIZE = 1073741824

MIN_UPLOAD_SIZE = 5 * 1024 * 1024
CHUNK_SIZE = MIN_UPLOAD_SIZE
MAX_RETRIES = 5

RETRIABLE_EXCEPTIONS = (
    botocore.exceptions.ClientError,
    aiohttp.client_exceptions.ClientPayloadError,
    botocore.exceptions.BotoCoreError,
)

POLICY_DELETE = {
    "Rules": [
        {
            "Expiration": {
                "Days": 0,
            },
            "Status": "Enabled",
        }
    ]
}


class S3StorageField(StorageField):
    storage: S3Storage

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=3)
    async def _download(self, uri, bucket, **kwargs):
        if "headers" in kwargs:
            for key, value in kwargs["headers"].items():
                kwargs[key] = value
            del kwargs["headers"]
        return await self.storage._s3aioclient.get_object(
            Bucket=bucket, Key=uri, **kwargs
        )

    async def iter_data(self, **kwargs):
        # Suports field and key based iter
        uri = self.field.uri if self.field else self.key
        if self.field is None:
            bucket = self.bucket
        else:
            bucket = self.field.bucket_name

        downloader = await self._download(uri, bucket, **kwargs)

        # we do not want to timeout ever from this...
        # downloader['Body'].set_socket_timeout(999999)
        stream = downloader["Body"]
        data = await stream.read(CHUNK_SIZE)
        while True:
            if not data:
                break
            yield data
            data = await stream.read(CHUNK_SIZE)

    async def read_range(self, start: int, end: int) -> AsyncIterator[bytes]:
        """
        Iterate through ranges of data
        """
        async for chunk in self.iter_data(Range=f"bytes={start}-{end - 1}"):
            yield chunk

    async def _abort_multipart(self):
        try:
            mpu = self.field.resumable_uri
            upload_file_id = self.field.upload_uri
            bucket_name = self.field.bucket_name
            await self.storage._s3aioclient.abort_multipart_upload(
                Bucket=bucket_name, Key=upload_file_id, UploadId=mpu["UploadId"]
            )
        except Exception:
            logger.warning("Could not abort multipart upload", exc_info=True)

    async def start(self, cf: CloudFile) -> CloudFile:
        if self.field is not None and self.field.upload_uri is not None:
            # Field has already a file beeing uploaded, cancel
            await self._abort_multipart()
        elif self.field is not None and self.field.uri is not None:
            # If exist the file copy the old url to delete
            field = CloudFile(
                filename=cf.filename,
                size=cf.size,
                content_type=cf.content_type,
                bucket_name=self.bucket,
                md5=cf.md5,
                source=CloudFile.GCS,
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
                source=CloudFile.GCS,
            )
            upload_uri = self.key

        field.offset = 1
        response = await self._create_multipart(self.bucket, upload_uri, field)
        field.resumable_uri = response["UploadId"]
        field.upload_uri = upload_uri
        return field

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=3)
    async def _create_multipart(self, bucket_name: str, upload_id: str, cf: CloudFile):
        return await self.storage._s3aioclient.create_multipart_upload(
            Bucket=bucket_name,
            Key=upload_id,
            Metadata={
                "FILENAME": cf.filename,
                "SIZE": str(cf.size),
                "CONTENT_TYPE": cf.content_type,
            },
        )

    async def append(self, cf: CloudFile, iterable: AsyncIterator) -> int:
        size = 0
        if self.field is None:
            raise AttributeError("No field configured")
        async for chunk in iterable:
            size += len(chunk)
            part = await self._upload_part(cf, chunk)
            self.field.parts.append(part["ETag"])
            self.field.offset += 1
        return size

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=3)
    async def _upload_part(self, cf: CloudFile, data: bytes):
        if self.field is None:
            raise AttributeError("No field configured")
        return await self.storage._s3aioclient.upload_part(
            Bucket=self.field.bucket_name,
            Key=self.field.upload_uri,
            PartNumber=self.field.offset,
            UploadId=self.field.resumable_uri,
            Body=data,
        )

    async def finish(self):
        if self.field is None:
            raise AttributeError("No field configured")
        if self.field.old_uri not in ("", None):
            # delete existing file
            try:
                await self.storage.delete_upload(
                    uri=self.field.old_uri, bucket=self.field.old_bucket
                )
                self.field.ClearField("old_uri")
                self.field.CkearField("old_bucket")
            except botocore.exceptions.ClientError:
                logger.error(
                    f"Referenced key {self.field.uri} could not be found", exc_info=True
                )
                logger.warning("Error deleting object", exc_info=True)

        if self.field.resumable_uri is not None:
            await self._complete_multipart_upload()

        self.field.uri = self.key
        self.field.ClearField("resumable_uri")
        self.field.ClearField("offset")
        self.field.ClearField("upload_uri")
        self.field.ClearField("parts")

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=3)
    async def _complete_multipart_upload(self):
        # if blocks is 0, it means the file is of zero length so we need to
        # trick it to finish a multiple part with no data.
        if self.field.offset == 1:
            part = await self._upload_part(None, b"")
            self.field.parts.append(part["ETag"])
            self.field.offset += 1
        part_info = {
            "Parts": [
                {"PartNumber": part + 1, "ETag": etag}
                for part, etag in enumerate(self.field.parts)
            ]
        }
        await self.storage._s3aioclient.complete_multipart_upload(
            Bucket=self.field.bucket_name,
            Key=self.field.upload_uri,
            UploadId=self.field.resumable_uri,
            MultipartUpload=part_info,
        )

    async def exists(self):
        """
        Existence can be checked either with a CloudFile data in the field attribute
        or own StorageField key and bucket. Field takes precendece
        """

        key = None
        bucket = None
        if self.field is not None and self.field.uri is not None:
            key = self.field.uri
            bucket = self.field.bucket_name
        elif self.key is not None:
            key = self.key
            bucket = self.bucket
        else:
            return None

        try:
            obj = await self.storage._s3aioclient.head_object(Bucket=bucket, Key=key)
            if obj is not None:
                metadata = obj.get("Metadata", {})
                return {
                    "SIZE": metadata.get("size") or obj.get("ContentLength"),
                    "CONTENT_TYPE": metadata.get("content_type")
                    or obj.get("ContentType"),
                    "FILENAME": metadata.get("filename") or key.split("/")[-1],
                }
            else:
                return None
        except botocore.exceptions.ClientError as ex:
            if ex.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    async def copy(
        self,
        origin_uri: str,
        destination_uri: str,
        origin_bucket_name: str,
        destination_bucket_name: str,
    ):
        await self.storage._s3aioclient.copy_object(
            CopySource={"Bucket": origin_bucket_name, "Key": origin_uri},
            Bucket=destination_bucket_name,
            Key=destination_uri,
        )

    async def upload(self, iterator: AsyncIterator, origin: CloudFile) -> CloudFile:
        self.field = await self.start(origin)
        await self.append(origin, iterator)
        await self.finish()
        return self.field

    def __repr__(self):
        return f"{self.storage.source}: {self.bucket}/{self.key}"


class S3Storage(Storage):
    field_klass = S3StorageField
    _session = None
    chunk_size = CHUNK_SIZE

    def __init__(
        self,
        aws_client_id: Optional[str] = None,
        aws_client_secret: Optional[str] = None,
        deadletter_bucket: Optional[str] = None,
        indexing_bucket: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        verify_ssl: bool = True,
        use_ssl: bool = True,
        region_name: Optional[str] = None,
        max_pool_connections: int = 30,
        bucket: Optional[str] = None,
    ):
        self.source = CloudFile.S3
        self.deadletter_bucket = deadletter_bucket
        self.indexing_bucket = indexing_bucket
        self._aws_access_key = aws_client_id
        self._aws_secret_key = aws_client_secret

        self.opts = dict(
            aws_secret_access_key=self._aws_secret_key,
            aws_access_key_id=self._aws_access_key,
            endpoint_url=endpoint_url,
            verify=verify_ssl,
            use_ssl=use_ssl,
            region_name=region_name,
            config=aiobotocore.config.AioConfig(
                None, max_pool_connections=max_pool_connections
            ),
        )
        self._exit_stack = AsyncExitStack()
        self.bucket = bucket

    def get_bucket_name(self, kbid: str):
        if self.bucket is None:
            raise AttributeError()
        return self.bucket.format(kbid=kbid)

    @property
    def session(self):
        if self._session is None:
            self._session = get_session()
        return self._session

    async def initialize(self):
        session = AioSession()
        self._s3aioclient = await self._exit_stack.enter_async_context(
            session.create_client("s3", **self.opts)
        )

    async def finalize(self):
        await self._exit_stack.__aexit__(None, None, None)

    async def delete_upload(self, uri: str, bucket: str):
        if uri is not None:
            try:
                await self._s3aioclient.delete_object(Bucket=bucket, Key=uri)
            except botocore.exceptions.ClientError:
                logger.warning("Error deleting object", exc_info=True)
        else:
            raise AttributeError("No valid uri")

    async def iterate_bucket(
        self, bucket: str, prefix: str = "/"
    ) -> AsyncIterator[Any]:
        paginator = self._s3aioclient.get_paginator("list_objects")
        async for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for item in result.get("Contents", []):
                item["name"] = item["Key"]
                yield item

    async def create_kb(self, kbid: str):
        bucket_name = self.get_bucket_name(kbid)
        missing = False
        created = False
        try:
            res = await self._s3aioclient.head_bucket(Bucket=bucket_name)
            if res["ResponseMetadata"]["HTTPStatusCode"] == 404:
                missing = True
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                missing = True

        if missing:
            await self._s3aioclient.create_bucket(Bucket=bucket_name)
            created = True
        return created

    async def schedule_delete_kb(self, kbid: str):
        bucket_name = self.get_bucket_name(kbid)

        missing = False
        deleted = False
        try:
            res = await self._s3aioclient.head_bucket(Bucket=bucket_name)
            if res["ResponseMetadata"]["HTTPStatusCode"] == 404:
                missing = True
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                missing = True

        if missing is False:
            await self._s3aioclient.put_bucket_lifecycle_configuration(
                Bucket=bucket_name, LifecycleConfiguration=POLICY_DELETE
            )
            deleted = True
        return deleted

    async def delete_kb(self, kbid: str):
        bucket_name = self.get_bucket_name(kbid)

        missing = False
        deleted = False
        conflict = False
        try:
            res = await self._s3aioclient.head_bucket(Bucket=bucket_name)
            if res["ResponseMetadata"]["HTTPStatusCode"] == 404:
                missing = True
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                missing = True

        if missing is False:
            try:
                res = await self._s3aioclient.delete_bucket(Bucket=bucket_name)
            except botocore.exceptions.ClientError as e:
                error_code = int(e.response["Error"]["Code"])
                if error_code == 409:
                    conflict = True
                if error_code in (200, 204):
                    deleted = True
        return deleted, conflict
