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

import base64
from contextlib import AsyncExitStack
from datetime import datetime
from typing import AsyncGenerator, AsyncIterator, Optional

import aiobotocore  # type: ignore
import aiohttp
import backoff
import botocore  # type: ignore
from aiobotocore.client import AioBaseClient  # type: ignore
from aiobotocore.session import AioSession, get_session  # type: ignore

from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_telemetry import errors, metrics
from nucliadb_utils import logger
from nucliadb_utils.storages.exceptions import UnparsableResponse
from nucliadb_utils.storages.storage import Storage, StorageField
from nucliadb_utils.storages.utils import ObjectInfo, ObjectMetadata, Range

s3_ops_observer = metrics.Observer("s3_ops", labels={"type": ""})


MB = 1024 * 1024
MIN_UPLOAD_SIZE = 5 * MB
CHUNK_SIZE = MIN_UPLOAD_SIZE
MAX_TRIES = 3

RETRIABLE_EXCEPTIONS = (
    botocore.exceptions.ClientError,
    aiohttp.client_exceptions.ClientPayloadError,
    botocore.exceptions.BotoCoreError,
)

POLICY_DELETE = {
    "Rules": [
        {
            "Expiration": {"Days": 1},
            "ID": "FullDelete",
            "Filter": {"Prefix": ""},
            "Status": "Enabled",
            "NoncurrentVersionExpiration": {"NoncurrentDays": 1},
            "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1},
        },
        {
            "Expiration": {"ExpiredObjectDeleteMarker": True},
            "ID": "DeleteMarkers",
            "Filter": {"Prefix": ""},
            "Status": "Enabled",
        },
    ]
}


class S3StorageField(StorageField):
    storage: S3Storage

    @backoff.on_exception(
        backoff.expo,
        RETRIABLE_EXCEPTIONS,
        jitter=backoff.random_jitter,
        max_tries=MAX_TRIES,
    )
    @s3_ops_observer.wrap({"type": "download"})
    async def _download(
        self,
        uri,
        bucket,
        range: Optional[Range] = None,
    ):
        range = range or Range()
        if range.any():
            coro = self.storage._s3aioclient.get_object(Bucket=bucket, Key=uri, Range=range.to_header())
        else:
            coro = self.storage._s3aioclient.get_object(Bucket=bucket, Key=uri)
        try:
            return await coro
        except botocore.exceptions.ClientError as e:
            error_code = parse_status_code(e)
            if error_code == 404:
                raise KeyError(f"S3 cloud file not found : {uri}")
            else:
                raise

    @s3_ops_observer.wrap({"type": "iter_data"})
    async def iter_data(self, range: Optional[Range] = None) -> AsyncGenerator[bytes, None]:
        # Suports field and key based iter
        uri = self.field.uri if self.field else self.key
        if self.field is None:
            bucket = self.bucket
        else:
            bucket = self.field.bucket_name
        downloader = await self._download(uri, bucket, range=range)
        stream = downloader["Body"]
        data = await stream.read(CHUNK_SIZE)
        while True:
            if not data:
                break
            yield data
            data = await stream.read(CHUNK_SIZE)

    @s3_ops_observer.wrap({"type": "abort_multipart"})
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

    @s3_ops_observer.wrap({"type": "start_upload"})
    async def start(self, cf: CloudFile) -> CloudFile:
        if self.field is not None and self.field.upload_uri != "":
            # Field has already a file beeing uploaded, cancel
            await self._abort_multipart()

        if self.field is not None and self.field.uri != "":
            # If exist the file copy the old url to delete
            field = CloudFile(
                filename=cf.filename,
                size=cf.size,
                content_type=cf.content_type,
                bucket_name=self.bucket,
                md5=cf.md5,
                source=CloudFile.S3,
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
                source=CloudFile.S3,
            )
            upload_uri = self.key

        field.offset = 1
        response = await self._create_multipart(self.bucket, upload_uri, field)
        field.resumable_uri = response["UploadId"]
        field.upload_uri = upload_uri
        return field

    @backoff.on_exception(
        backoff.expo,
        RETRIABLE_EXCEPTIONS,
        jitter=backoff.random_jitter,
        max_tries=MAX_TRIES,
    )
    @s3_ops_observer.wrap({"type": "create_multipart"})
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

    @s3_ops_observer.wrap({"type": "append_data"})
    async def append(self, cf: CloudFile, iterable: AsyncIterator) -> int:
        size = 0
        if self.field is None:
            raise AttributeError("No field configured")

        upload_chunk = b""  # s3 strict about chunk size
        async for chunk in iterable:
            size += len(chunk)
            upload_chunk += chunk
            if len(upload_chunk) >= MIN_UPLOAD_SIZE:
                part = await self._upload_part(cf, upload_chunk)
                self.field.parts.append(part["ETag"])
                self.field.offset += 1
                upload_chunk = b""
        if len(upload_chunk) > 0:
            part = await self._upload_part(cf, upload_chunk)
            self.field.parts.append(part["ETag"])
            self.field.offset += 1

        return size

    @backoff.on_exception(
        backoff.expo,
        RETRIABLE_EXCEPTIONS,
        jitter=backoff.random_jitter,
        max_tries=MAX_TRIES,
    )
    @s3_ops_observer.wrap({"type": "upload_part"})
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

    @s3_ops_observer.wrap({"type": "finish_upload"})
    async def finish(self):
        if self.field is None:
            raise AttributeError("No field configured")
        if self.field.old_uri not in ("", None):
            # delete existing file
            try:
                await self.storage.delete_upload(
                    uri=self.field.old_uri, bucket_name=self.field.old_bucket
                )
                self.field.ClearField("old_uri")
                self.field.ClearField("old_bucket")
            except botocore.exceptions.ClientError:
                logger.error(f"Referenced key {self.field.uri} could not be found", exc_info=True)
                logger.warning("Error deleting object", exc_info=True)

        if self.field.resumable_uri != "":
            await self._complete_multipart_upload()

        self.field.uri = self.key
        self.field.ClearField("resumable_uri")
        self.field.ClearField("offset")
        self.field.ClearField("upload_uri")
        self.field.ClearField("parts")

    @backoff.on_exception(
        backoff.expo,
        RETRIABLE_EXCEPTIONS,
        jitter=backoff.random_jitter,
        max_tries=MAX_TRIES,
    )
    @s3_ops_observer.wrap({"type": "complete_multipart"})
    async def _complete_multipart_upload(self):
        # if blocks is 0, it means the file is of zero length so we need to
        # trick it to finish a multiple part with no data.
        if self.field.offset == 1:
            part = await self._upload_part(None, b"")
            self.field.parts.append(part["ETag"])
            self.field.offset += 1
        part_info = {
            "Parts": [
                {"PartNumber": part + 1, "ETag": etag} for part, etag in enumerate(self.field.parts)
            ]
        }
        await self.storage._s3aioclient.complete_multipart_upload(
            Bucket=self.field.bucket_name,
            Key=self.field.upload_uri,
            UploadId=self.field.resumable_uri,
            MultipartUpload=part_info,
        )

    @s3_ops_observer.wrap({"type": "exists"})
    async def exists(self) -> Optional[ObjectMetadata]:
        """
        Existence can be checked either with a CloudFile data in the field attribute
        or own StorageField key and bucket. Field takes precendece
        """

        key = None
        bucket = None
        if self.field is not None and self.field.uri != "":
            key = self.field.uri
            bucket = self.field.bucket_name
        elif self.key != "":
            key = self.key
            bucket = self.bucket
        else:
            return None

        try:
            obj = await self.storage._s3aioclient.head_object(Bucket=bucket, Key=key)
            if obj is None:
                return None
            return parse_object_metadata(obj, key)
        except botocore.exceptions.ClientError as e:
            error_code = parse_status_code(e)
            if error_code == 404:
                return None
            raise

    @s3_ops_observer.wrap({"type": "copy"})
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

    @s3_ops_observer.wrap({"type": "move"})
    async def move(
        self,
        origin_uri: str,
        destination_uri: str,
        origin_bucket_name: str,
        destination_bucket_name: str,
    ):
        await self.copy(origin_uri, destination_uri, origin_bucket_name, destination_bucket_name)
        await self.storage.delete_upload(origin_uri, origin_bucket_name)

    @s3_ops_observer.wrap({"type": "upload"})
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
        kms_key_id: Optional[str] = None,
        max_pool_connections: int = 30,
        bucket: Optional[str] = None,
        bucket_tags: Optional[dict[str, str]] = None,
    ):
        self.source = CloudFile.S3
        self.deadletter_bucket = deadletter_bucket
        self.indexing_bucket = indexing_bucket
        self._aws_access_key = aws_client_id
        self._aws_secret_key = aws_client_secret
        self._region_name = region_name

        self._bucket_tags = bucket_tags

        self.opts = dict(
            aws_secret_access_key=self._aws_secret_key,
            aws_access_key_id=self._aws_access_key,
            endpoint_url=endpoint_url,
            verify=verify_ssl,
            use_ssl=use_ssl,
            region_name=region_name,
            config=aiobotocore.config.AioConfig(None, max_pool_connections=max_pool_connections),
        )
        self._exit_stack = AsyncExitStack()
        self.bucket = bucket
        self._kms_key_id = kms_key_id

    def get_bucket_name(self, kbid: str):
        if self.bucket is None:
            raise AttributeError()
        return self.bucket.format(kbid=kbid)

    @property
    def session(self):
        if self._session is None:
            self._session = get_session()
        return self._session

    async def initialize(self: "S3Storage") -> None:
        session = AioSession()
        self._s3aioclient: AioBaseClient = await self._exit_stack.enter_async_context(
            session.create_client("s3", **self.opts)
        )
        for bucket in (self.deadletter_bucket, self.indexing_bucket):
            if bucket is not None:
                await self._create_bucket_if_not_exists(bucket)

    async def _create_bucket_if_not_exists(self, bucket_name: str) -> bool:
        created = False
        bucket_exists = await self.bucket_exists(bucket_name)
        if not bucket_exists:
            created = True
            await self.create_bucket(bucket_name)
        return created

    async def finalize(self):
        await self._exit_stack.__aexit__(None, None, None)

    @s3_ops_observer.wrap({"type": "delete"})
    async def delete_upload(self, uri: str, bucket_name: str):
        if uri:
            try:
                await self._s3aioclient.delete_object(Bucket=bucket_name, Key=uri)
            except botocore.exceptions.ClientError:
                logger.warning("Error deleting object", exc_info=True)
        else:
            raise AttributeError("No valid uri")

    async def iterate_objects(
        self, bucket: str, prefix: str = "/", start: Optional[str] = None
    ) -> AsyncGenerator[ObjectInfo, None]:
        paginator = self._s3aioclient.get_paginator("list_objects")
        async for result in paginator.paginate(
            Bucket=bucket, Prefix=prefix, PaginationConfig={"StartingToken": start}
        ):
            for item in result.get("Contents") or []:
                yield ObjectInfo(name=item["Key"])

    async def create_kb(self, kbid: str):
        bucket_name = self.get_bucket_name(kbid)
        return await self._create_bucket_if_not_exists(bucket_name)

    async def bucket_exists(self, bucket_name: str) -> bool:
        return await bucket_exists(self._s3aioclient, bucket_name)

    async def create_bucket(self, bucket_name: str):
        await create_bucket(
            self._s3aioclient, bucket_name, self._bucket_tags, self._region_name, self._kms_key_id
        )

    async def schedule_delete_kb(self, kbid: str):
        bucket_name = self.get_bucket_name(kbid)

        missing = False
        deleted = False
        try:
            res = await self._s3aioclient.head_bucket(Bucket=bucket_name)
            if res["ResponseMetadata"]["HTTPStatusCode"] == 404:
                missing = True
        except botocore.exceptions.ClientError as e:
            error_code = parse_status_code(e)
            if error_code == 404:
                missing = True

        if missing is False:
            await self._s3aioclient.put_bucket_lifecycle_configuration(
                Bucket=bucket_name, LifecycleConfiguration=POLICY_DELETE
            )
            deleted = True
        return deleted

    async def delete_kb(self, kbid: str) -> tuple[bool, bool]:
        bucket_name = self.get_bucket_name(kbid)

        missing = False
        deleted = False
        conflict = False
        try:
            res = await self._s3aioclient.head_bucket(Bucket=bucket_name)
            if res["ResponseMetadata"]["HTTPStatusCode"] == 404:
                missing = True
        except botocore.exceptions.ClientError as e:
            error_code = parse_status_code(e)
            if error_code == 404:
                missing = True

        if missing is False:
            try:
                res = await self._s3aioclient.delete_bucket(Bucket=bucket_name)
            except botocore.exceptions.ClientError as e:
                error_code = parse_status_code(e)
                if error_code == 409:
                    conflict = True
                if error_code in (200, 204):
                    deleted = True
        return deleted, conflict

    @backoff.on_exception(
        backoff.expo,
        RETRIABLE_EXCEPTIONS,
        jitter=backoff.random_jitter,
        max_tries=MAX_TRIES,
    )
    @s3_ops_observer.wrap({"type": "insert_object"})
    async def insert_object(self, bucket_name: str, key: str, data: bytes) -> None:
        await self._s3aioclient.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            ContentType="application/octet-stream",
        )


async def bucket_exists(client: AioSession, bucket_name: str) -> bool:
    exists = True
    try:
        res = await client.head_bucket(Bucket=bucket_name)
        if res["ResponseMetadata"]["HTTPStatusCode"] == 404:
            exists = False
    except botocore.exceptions.ClientError as e:
        error_code = parse_status_code(e)
        if error_code == 404:
            exists = False
    return exists


async def create_bucket(
    client: AioSession,
    bucket_name: str,
    bucket_tags: Optional[dict[str, str]] = None,
    region_name: Optional[str] = None,
    kms_key_id: Optional[str] = None,
):
    bucket_creation_options = {}
    if region_name is not None:
        bucket_creation_options = {"CreateBucketConfiguration": {"LocationConstraint": region_name}}
    # Create the bucket
    await client.create_bucket(Bucket=bucket_name, **bucket_creation_options)

    if bucket_tags is not None and len(bucket_tags) > 0:
        # Set bucket tags
        await client.put_bucket_tagging(
            Bucket=bucket_name,
            Tagging={
                "TagSet": [
                    {"Key": tag_key, "Value": tag_value} for tag_key, tag_value in bucket_tags.items()
                ]
            },
        )
    if kms_key_id is not None:
        await client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "aws:kms",
                            "KMSMasterKeyID": kms_key_id,
                        },
                        "BucketKeyEnabled": True,
                    }
                ]
            },
        )


def parse_status_code(error: botocore.exceptions.ClientError) -> int:
    status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", None)
    if status_code is not None:
        return status_code

    error_code = error.response["Error"]["Code"]
    if error_code.isnumeric():
        return int(error_code)

    # See https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
    error_code_mappings = {
        "AccessDenied": 403,
        "NoSuchBucket": 404,
        "NoSuchKey": 404,
        "BucketNotEmpty": 409,
    }

    if error_code in error_code_mappings:
        return error_code_mappings[error_code]

    msg = f"Unexpected error status while parsing error response: {error_code}"
    with errors.push_scope() as scope:
        scope.set_extra("response", error.response)
        errors.capture_message(msg, "error", scope)

    raise UnparsableResponse(msg) from error


def parse_object_metadata(obj: dict, key: str) -> ObjectMetadata:
    custom_metadata = obj.get("Metadata") or {}
    # Parse size
    custom_size = custom_metadata.get("size")
    if custom_size is None or custom_size == "0":
        size = 0
        content_lenght = obj.get("ContentLength")
        if content_lenght is not None:
            size = int(content_lenght)
    else:
        size = int(custom_size)
    # Content type
    content_type = custom_metadata.get("content_type") or obj.get("ContentType") or ""
    # Filename
    base64_filename = custom_metadata.get("base64_filename")
    if base64_filename:
        filename = base64.b64decode(base64_filename).decode()
    else:
        filename = custom_metadata.get("filename") or key.split("/")[-1]

    return ObjectMetadata(size=size, content_type=content_type, filename=filename)
