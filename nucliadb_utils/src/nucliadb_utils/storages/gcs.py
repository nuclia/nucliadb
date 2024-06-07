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
import base64
import json
import socket
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime
from typing import Any, AsyncGenerator, AsyncIterator, Dict, List, Optional, Union, cast
from urllib.parse import quote_plus

import aiohttp
import aiohttp.client_exceptions
import backoff
import google.auth.transport.requests  # type: ignore
import yarl
from google.oauth2 import service_account  # type: ignore
from nucliadb_protos.resources_pb2 import CloudFile

from nucliadb_telemetry import metrics
from nucliadb_telemetry.utils import setup_telemetry
from nucliadb_utils import logger
from nucliadb_utils.storages import CHUNK_SIZE
from nucliadb_utils.storages.exceptions import (
    CouldNotCopyNotFound,
    CouldNotCreateBucket,
    InvalidOffset,
    ResumableUploadGone,
)
from nucliadb_utils.storages.object_store import ObjectStore
from nucliadb_utils.storages.storage import Storage, StorageField
from nucliadb_utils.storages.utils import ObjectInfo, ObjectMetadata, Range

storage_ops_observer = metrics.Observer("gcs_ops", labels={"type": ""})


def strip_query_params(url: yarl.URL) -> str:
    return str(url.with_query(None))


KB = 1024
MB = 1024 * KB

MIN_UPLOAD_SIZE = 256 * KB
OBJECT_DATA_CHUNK_SIZE = 1 * MB


DEFAULT_SCOPES = ["https://www.googleapis.com/auth/devstorage.read_write"]
MAX_TRIES = 4

POLICY_DELETE = {
    "lifecycle": {
        "rule": [
            {
                "action": {"type": "Delete"},
                "condition": {
                    "age": 0,
                },
            },
        ]
    }
}


class GoogleCloudException(Exception):
    pass


class ReadingResponseContentException(GoogleCloudException):
    pass


RETRIABLE_EXCEPTIONS = (
    GoogleCloudException,
    aiohttp.client_exceptions.ClientPayloadError,
    aiohttp.client_exceptions.ClientConnectorError,
    aiohttp.client_exceptions.ClientConnectionError,
    aiohttp.client_exceptions.ClientOSError,
    aiohttp.client_exceptions.ServerConnectionError,
    aiohttp.client_exceptions.ServerDisconnectedError,
    CouldNotCreateBucket,
    socket.gaierror,
)


class GCSStorageField(StorageField):
    storage: GCSStorage

    async def move(
        self,
        origin_uri: str,
        destination_uri: str,
        origin_bucket_name: str,
        destination_bucket_name: str,
    ):
        await self.copy(
            origin_uri, destination_uri, origin_bucket_name, destination_bucket_name
        )
        await self.storage.delete_upload(origin_uri, origin_bucket_name)

    @backoff.on_exception(
        backoff.expo,
        RETRIABLE_EXCEPTIONS,
        jitter=backoff.random_jitter,
        max_tries=MAX_TRIES,
    )
    @storage_ops_observer.wrap({"type": "copy"})
    async def copy(
        self,
        origin_uri: str,
        destination_uri: str,
        origin_bucket_name: str,
        destination_bucket_name: str,
    ):
        url = "{}/{}/o/{}/rewriteTo/b/{}/o/{}".format(
            self.storage.object_base_url,
            origin_bucket_name,
            quote_plus(origin_uri),
            destination_bucket_name,
            quote_plus(destination_uri),
        )
        headers = await self.storage.get_access_headers()
        headers.update({"Content-Type": "application/json"})
        async with self.storage.session.post(url, headers=headers) as resp:
            if resp.status == 404:
                text = await resp.text()
                raise CouldNotCopyNotFound(
                    origin_uri=origin_uri,
                    origin_bucket_name=origin_bucket_name,
                    destination_uri=destination_uri,
                    destination_bucket_name=destination_bucket_name,
                    text=text,
                )
            else:
                data = await resp.json()
                assert data["resource"]["name"] == destination_uri

    @storage_ops_observer.wrap({"type": "iter_data"})
    async def iter_data(
        self, range: Optional[Range] = None
    ) -> AsyncGenerator[bytes, None]:
        attempt = 1
        while True:
            try:
                async for chunk in self._inner_iter_data(range=range):
                    yield chunk
                break
            except ReadingResponseContentException:
                # Do not retry any exception that may happen in the middle of
                # reading the response chunks, as that could lead to duplicated
                # chunks and data corruption.
                raise
            except RETRIABLE_EXCEPTIONS as ex:
                if attempt >= MAX_TRIES:
                    raise
                wait_time = 2 ** (attempt - 1)
                logger.warning(
                    f"Error downloading from GCP. Retrying ({attempt} of {MAX_TRIES}) after {wait_time} seconds. Error: {ex}"  # noqa
                )
                await asyncio.sleep(wait_time)
                attempt += 1

    @storage_ops_observer.wrap({"type": "inner_iter_data"})
    async def _inner_iter_data(self, range: Optional[Range] = None):
        """
        Iterate through object data.
        """
        range = range or Range()
        assert self.storage.session is not None

        headers = await self.storage.get_access_headers()
        if range.any():
            headers["Range"] = range.to_header()
        key = self.field.uri if self.field else self.key
        if self.field is None:
            bucket = self.bucket
        else:
            bucket = self.field.bucket_name
        url = "{}/{}/o/{}".format(
            self.storage.object_base_url,
            bucket,
            quote_plus(key),
        )
        async with self.storage.session.get(
            url, headers=headers, params={"alt": "media"}, timeout=-1
        ) as api_resp:
            if api_resp.status not in (200, 206):
                text = await api_resp.text()
                if api_resp.status == 404:
                    raise KeyError(f"Google cloud file not found : \n {text}")
                raise GoogleCloudException(f"{api_resp.status}: {text}")
            while True:
                try:
                    chunk = await api_resp.content.read(OBJECT_DATA_CHUNK_SIZE)
                except Exception as ex:
                    raise ReadingResponseContentException() from ex
                if len(chunk) > 0:
                    yield chunk
                else:
                    break

    @backoff.on_exception(
        backoff.expo,
        RETRIABLE_EXCEPTIONS,
        jitter=backoff.random_jitter,
        max_tries=MAX_TRIES,
    )
    @storage_ops_observer.wrap({"type": "start_upload"})
    async def start(self, cf: CloudFile) -> CloudFile:
        """Init an upload.

        cf: New file to upload
        """
        if self.storage.session is None:
            raise AttributeError()

        if self.field is not None and self.field.upload_uri != "":
            # If there is a temporal url
            await self.storage.delete_upload(
                self.field.upload_uri, self.field.bucket_name
            )

        if self.field is not None and self.field.uri != "":
            field: CloudFile = CloudFile(
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

        init_url = "{}&name={}".format(
            self.storage._upload_url.format(bucket=self.bucket),
            quote_plus(upload_uri),
        )
        metadata = json.dumps(
            {
                "metadata": {
                    "FILENAME": cf.filename,
                    "SIZE": str(cf.size),
                    "CONTENT_TYPE": cf.content_type,
                },
            }
        )
        call_size = len(metadata)
        headers = await self.storage.get_access_headers()
        headers.update(
            {
                "X-Upload-Content-Type": cf.content_type,
                "X-Upload-Content-Length": str(cf.size),
                "Content-Type": "application/json; charset=UTF-8",
                "Content-Length": str(call_size),
            }
        )

        async with self.storage.session.post(
            init_url,
            headers=headers,
            data=metadata,
        ) as call:
            if call.status != 200:
                text = await call.text()
                raise GoogleCloudException(f"{call.status}: {text}")
            resumable_uri = call.headers["Location"]

        field.offset = 0
        field.resumable_uri = resumable_uri
        field.upload_uri = upload_uri

        return field

    @backoff.on_exception(
        backoff.constant,
        RETRIABLE_EXCEPTIONS,
        interval=1,
        max_tries=MAX_TRIES,
        jitter=backoff.random_jitter,
    )
    @storage_ops_observer.wrap({"type": "append_data"})
    async def _append(self, cf: CloudFile, data: bytes):
        if self.field is None:
            raise AttributeError()

        if self.storage.session is None:
            raise AttributeError()

        # size = 0 ==> size may be unset, as 0 is the default protobuffer value
        # Makes no sense to assume a file with size = 0 in upload
        if cf.size > 0:
            size = str(cf.size)
        else:
            # assuming size will come eventually
            size = "*"
        headers = {
            "Content-Length": str(len(data)),
            "Content-Type": cf.content_type,
        }
        if len(data) != size:
            content_range = "bytes {init}-{chunk}/{total}".format(
                init=self.field.offset,
                chunk=self.field.offset + len(data) - 1,
                total=size,
            )
            headers["Content-Range"] = content_range

        async with self.storage.session.put(
            self.field.resumable_uri, headers=headers, data=data
        ) as call:
            text = await call.text()  # noqa
            if call.status not in [200, 201, 308]:
                if call.status == 410:
                    raise ResumableUploadGone(text)
                logger.error(f"content-range: {content_range}")
                raise GoogleCloudException(f"{call.status}: {text}")
            return call

    async def append(self, cf: CloudFile, iterable: AsyncIterator) -> int:
        if self.field is None:
            raise AttributeError()
        count = 0
        async for chunk in iterable:
            resp = await self._append(cf, chunk)
            size = len(chunk)
            count += size
            self.field.offset += len(chunk)

            if resp.status == 308:
                # verify we're on track with google's resumable api...
                range_header = resp.headers["Range"]
                if self.field.offset - 1 != int(range_header.split("-")[-1]):
                    # range header is the byte range google has received,
                    # which is different from the total size--off by one
                    raise InvalidOffset(range_header, self.field.offset)
            elif resp.status in [200, 201]:
                # file manager will double check offsets and sizes match
                break
        return count

    async def finish(self):
        if self.field.old_uri not in ("", None):
            # Already has a file
            try:
                await self.storage.delete_upload(
                    self.field.old_uri, self.field.bucket_name
                )
            except GoogleCloudException as e:
                logger.warning(
                    f"Could not delete existing google cloud file "
                    f"with uri: {self.field.uri}: {e}"
                )
        if self.field.upload_uri != self.key:
            await self.move(
                self.field.upload_uri, self.key, self.field.bucket_name, self.bucket
            )

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
    @storage_ops_observer.wrap({"type": "exists"})
    async def exists(self) -> Optional[ObjectMetadata]:
        """
        Existence can be checked either with a CloudFile data in the field attribute
        or own StorageField key and bucket. Field takes precendece
        """
        if self.storage.session is None:
            raise AttributeError()
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

        url = "{}/{}/o/{}".format(
            self.storage.object_base_url,
            bucket,
            quote_plus(key),
        )
        headers = await self.storage.get_access_headers()
        async with self.storage.session.get(url, headers=headers) as api_resp:
            if api_resp.status == 200:
                data = await api_resp.json()
                data = cast(dict[str, Any], data)
                return parse_object_metadata(data, key)
            else:
                return None

    async def upload(self, iterator: AsyncIterator, origin: CloudFile) -> CloudFile:
        self.field = await self.start(origin)
        if self.field is None:
            raise AttributeError()
        await self.append(origin, iterator)
        await self.finish()
        return self.field

    def __repr__(self):
        return f"{self.storage.source}: {self.bucket}/{self.key}"


class GCSStorage(Storage):
    field_klass = GCSStorageField
    _credentials = None
    _json_credentials = None
    chunk_size = CHUNK_SIZE

    def __init__(
        self,
        account_credentials: Optional[str] = None,
        bucket: Optional[str] = None,
        location: Optional[str] = None,
        project: Optional[str] = None,
        executor: Optional[ThreadPoolExecutor] = None,
        deadletter_bucket: Optional[str] = None,
        indexing_bucket: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
        url: str = "https://www.googleapis.com",
        scopes: Optional[List[str]] = None,
    ):
        self.object_store = GCSObjectStore(
            account_credentials=account_credentials,
            location=location,
            project=project,
            executor=executor,
            bucket_labels=labels,
            url=url,
            scopes=scopes,
        )
        self.source = CloudFile.GCS
        self.deadletter_bucket = deadletter_bucket
        self.indexing_bucket = indexing_bucket
        self.bucket = bucket

    @storage_ops_observer.wrap({"type": "initialize"})
    async def initialize(self, service_name: Optional[str] = None):
        await setup_telemetry(service_name or "GCS_SERVICE")
        await self.object_store.initialize()
        for bucket in (self.deadletter_bucket, self.indexing_bucket):
            if bucket is None or bucket == "":
                continue
            try:
                await self.object_store.bucket_create(bucket)
            except Exception:
                logger.exception(f"Could not create bucket {bucket}", exc_info=True)

    async def finalize(self):
        await self.object_store.finalize()

    @property
    def session(self) -> aiohttp.ClientSession:
        return self.object_store.session

    @property
    def object_base_url(self) -> str:
        return self.object_store.object_base_url

    @property
    def _upload_url(self) -> str:
        return self.object_store._upload_url

    async def get_access_headers(self):
        return self.object_store.get_access_headers()

    @backoff.on_exception(
        backoff.expo,
        RETRIABLE_EXCEPTIONS,
        jitter=backoff.random_jitter,
        max_tries=MAX_TRIES,
    )
    @storage_ops_observer.wrap({"type": "delete"})
    async def delete_upload(self, uri: str, bucket_name: str):
        url = "{}/{}/o/{}".format(self.object_base_url, bucket_name, quote_plus(uri))
        headers = await self.get_access_headers()
        async with self.session.delete(url, headers=headers) as resp:
            if resp.status in (200, 204, 404):
                return
            try:
                data = await resp.json()
            except Exception:
                text = await resp.text()
                data = {"text": text}
            raise GoogleCloudException(f"{resp.status}: {json.dumps(data)}")

    def get_bucket_name(self, kbid: str):
        assert self.bucket is not None
        return self.bucket.format(kbid=kbid)

    @storage_ops_observer.wrap({"type": "create_bucket"})
    async def create_kb(self, kbid: str) -> bool:
        bucket_name = self.get_bucket_name(kbid)
        if await self.object_store.bucket_exists(bucket_name):
            return False
        return await self.object_store.bucket_create(
            bucket_name, labels={"kbid": kbid.lower()}
        )

    @storage_ops_observer.wrap({"type": "schedule_delete"})
    async def schedule_delete_kb(self, kbid: str):
        bucket_name = self.get_bucket_name(kbid)
        return await self.object_store.bucket_schedule_delete(bucket_name)

    @storage_ops_observer.wrap({"type": "delete"})
    async def delete_kb(self, kbid: str):
        bucket_name = self.get_bucket_name(kbid)
        return await self.object_store.bucket_delete(bucket_name)

    async def iterate_objects(
        self, bucket: str, prefix: str
    ) -> AsyncGenerator[ObjectInfo, None]:
        url = "{}/{}/o".format(self.object_base_url, bucket)
        headers = await self.get_access_headers()
        async with self.session.get(
            url,
            headers=headers,
            params={"prefix": prefix},
        ) as resp:
            assert resp.status == 200
            data = await resp.json()
            if "items" in data:
                for item in data["items"]:
                    yield ObjectInfo(name=item["name"])

        page_token = data.get("nextPageToken")
        while page_token is not None:
            headers = await self.get_access_headers()
            async with self.session.get(
                url,
                headers=headers,
                params={"prefix": prefix, "pageToken": page_token},
            ) as resp:
                data = await resp.json()
                items = data.get("items", [])
                if len(items) == 0:
                    break
                for item in items:
                    yield ObjectInfo(name=item["name"])
                page_token = data.get("nextPageToken")


class GCSObjectStore(ObjectStore):
    def __init__(
        self,
        account_credentials: Optional[str] = None,
        location: Optional[str] = None,
        project: Optional[str] = None,
        executor: Optional[ThreadPoolExecutor] = None,
        bucket_labels: Optional[dict[str, str]] = None,
        url: str = "https://www.googleapis.com",
        scopes: Optional[List[str]] = None,
    ):
        self._creation_access_token = datetime.now()
        self._upload_url = url + "/upload/storage/v1/b/{bucket}/o?uploadType=resumable"
        self.object_base_url = url + "/storage/v1/b"
        self._bucket_labels = bucket_labels or {}
        self._executor = executor
        self._location = location
        self._project = project
        self._scopes = scopes
        self._session: Optional[aiohttp.ClientSession] = None
        if account_credentials is not None:
            self._json_credentials = json.loads(base64.b64decode(account_credentials))
            self._credentials = service_account.Credentials.from_service_account_info(
                self._json_credentials, scopes=scopes or DEFAULT_SCOPES
            )

    async def get_access_headers(self):
        if self._credentials is None:
            return {}
        assert self._executor is not None
        loop = asyncio.get_event_loop()
        token = await loop.run_in_executor(self._executor, self._get_access_token)
        return {"AUTHORIZATION": f"Bearer {token}"}

    def _get_access_token(self):
        if self._credentials.valid is False:
            req = google.auth.transport.requests.Request()
            self._credentials.refresh(req)
            self._creation_access_token = datetime.now()
        return self._credentials.token

    @property
    def session(self) -> aiohttp.ClientSession:
        assert self._session is not None
        return self._session

    async def initialize(self) -> None:
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ttl_dns_cache=60 * 5)
        )

    async def finalize(self) -> None:
        if self._session is None:
            return
        try:
            await self.session.close()
        except Exception:
            logger.exception("Error closing GCS session", exc_info=True)
        self._session = None

    async def bucket_create(
        self, bucket: str, labels: dict[str, str] | None = None
    ) -> bool:
        headers = await self.get_access_headers()
        url = f"{self.object_base_url}?project={self._project}"
        bucket_labels = deepcopy(self._bucket_labels)
        if labels is not None:
            bucket_labels.update(labels)
        async with self.session.post(
            url,
            headers=headers,
            json={
                "name": bucket,
                "location": self._location,
                "labels": labels,
                "iamConfiguration": {
                    "publicAccessPrevention": "enforced",
                    "uniformBucketLevelAccess": {
                        "enabled": True,
                    },
                },
            },
        ) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.error(
                    "Creation of bucket error",
                    extra={
                        "bucket": bucket,
                        "status": resp.status,
                        "text": text,
                    },
                )
                raise CouldNotCreateBucket(text)
        return True

    async def bucket_delete(self, bucket: str) -> tuple[bool, bool]:
        headers = await self.get_access_headers()
        url = f"{self.object_base_url}/{bucket}"
        deleted = False
        conflict = False
        async with self.session.delete(url, headers=headers) as resp:
            if resp.status == 204:
                deleted = True
            elif resp.status == 409:
                details = await resp.text()
                logger.warning(
                    "Conflict on deleting bucket",
                    extra={
                        "bucket": bucket,
                        "details": details,
                    },
                )
                conflict = True
            elif resp.status == 404:
                deleted = False
            else:
                details = await resp.text()
                raise GoogleCloudException(f"{resp.status}: {details}")
        return deleted, conflict

    async def bucket_schedule_delete(self, bucket: str) -> None:
        headers = await self.get_access_headers()
        url = f"{self.object_base_url}/{bucket}?fields=lifecycle"
        async with self.session.patch(url, headers=headers, json=POLICY_DELETE) as resp:
            if resp.status in (200, 204):
                return
            if resp.status == 404:
                logger.info("Attempt to delete a bucket but was not found")
                return
            if resp.status == 405:
                # For testing purposes, gcs fixture doesn't have patch
                logger.error("Not implemented")
                return
            try:
                data = await resp.json()
            except Exception:
                text = await resp.text()
                data = {"text": text}
            raise GoogleCloudException(f"{resp.status}: {json.dumps(data)}")

    async def bucket_exists(self, bucket: str) -> bool:
        headers = await self.get_access_headers()
        url = f"{self.object_base_url}/{bucket}?project={self._project}"
        async with self.session.get(url, headers=headers) as resp:
            if resp.status == 200:
                return True
            elif resp.status == 404:
                return False
            text = await resp.text()
            raise GoogleCloudException(f"{resp.status}: {text}")

    async def move(
        self,
        origin_bucket: str,
        origin_key: str,
        destination_bucket: str,
        destination_key: str,
    ) -> None:
        raise NotImplementedError()

    async def copy(
        self,
        origin_bucket: str,
        origin_key: str,
        destination_bucket: str,
        destination_key: str,
    ) -> None:
        raise NotImplementedError()

    async def delete(self, bucket: str, key: str) -> None:
        raise NotImplementedError()

    async def upload(
        self,
        bucket: str,
        key: str,
        data: Union[bytes, AsyncGenerator[bytes, None]],
        metadata: ObjectMetadata,
    ) -> None:
        raise NotImplementedError()

    async def download(self, bucket: str, key: str) -> bytes:
        raise NotImplementedError()

    async def download_stream(
        self, bucket: str, key: str, range: Optional[Range] = None
    ) -> AsyncGenerator[bytes, None]:
        raise NotImplementedError()
        yield b""

    async def iterate(
        self, bucket: str, prefix: str
    ) -> AsyncGenerator[ObjectInfo, None]:
        raise NotImplementedError()
        yield ObjectInfo(name="")

    async def get_metadata(self, bucket: str, key: str) -> ObjectMetadata:
        raise NotImplementedError()

    async def upload_multipart_start(
        self, bucket: str, key: str, metadata: ObjectMetadata
    ) -> Optional[str]:
        """
        Start a multipart upload. May return the url for the resumable upload.
        """
        raise NotImplementedError()

    async def upload_multipart_append(
        self, bucket: str, key: str, iterable: AsyncIterator[bytes]
    ) -> int:
        """
        Append data to a multipart upload. Returns the number of bytes uploaded.
        """
        raise NotImplementedError()

    async def upload_multipart_finish(self, bucket: str, key: str) -> None:
        raise NotImplementedError()


def parse_object_metadata(object_data: dict[str, Any], key: str) -> ObjectMetadata:
    custom_metadata: dict[str, str] = object_data.get("metadata") or {}
    # Lowercase all keys for backwards compatibility with old custom metadata
    custom_metadata = {k.lower(): v for k, v in custom_metadata.items()}

    # Parse size
    custom_size = custom_metadata.get("size")
    if not custom_size or custom_size == "0":
        data_size = object_data.get("size")
        size = int(data_size) if data_size else 0
    else:
        size = int(custom_size)

    # Parse content-type
    content_type = (
        custom_metadata.get("content_type") or object_data.get("contentType") or ""
    )

    # Parse filename
    filename = custom_metadata.get("filename") or key.split("/")[-1]

    return ObjectMetadata(
        filename=filename,
        size=int(size),
        content_type=content_type,
    )
