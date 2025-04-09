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
from typing import Any, AsyncGenerator, AsyncIterator, Dict, List, Optional, cast
from urllib.parse import quote_plus

import aiohttp
import aiohttp.client_exceptions
import backoff
import google.auth.transport.requests  # type: ignore
import yarl
from google.auth.exceptions import DefaultCredentialsError  # type: ignore
from google.oauth2 import service_account  # type: ignore

from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_telemetry import errors, metrics
from nucliadb_utils import logger
from nucliadb_utils.storages import CHUNK_SIZE
from nucliadb_utils.storages.exceptions import (
    CouldNotCopyNotFound,
    CouldNotCreateBucket,
    InvalidOffset,
    ResumableUploadGone,
)
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

TIMEOUT = aiohttp.ClientTimeout(total=300, connect=30, sock_read=10)


class GCSStorageField(StorageField):
    storage: GCSStorage

    async def move(
        self,
        origin_uri: str,
        destination_uri: str,
        origin_bucket_name: str,
        destination_bucket_name: str,
    ):
        await self.copy(origin_uri, destination_uri, origin_bucket_name, destination_bucket_name)
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
        if self.storage.session is None:
            raise AttributeError()

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
    async def iter_data(self, range: Optional[Range] = None) -> AsyncGenerator[bytes, None]:
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
            await self.storage.delete_upload(self.field.upload_uri, self.field.bucket_name)

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
        bucket_upload_url = self.storage._upload_url.format(bucket=field.bucket_name)
        init_url = f"{bucket_upload_url}?uploadType=resumable&name={quote_plus(upload_uri)}"
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
                await self.storage.delete_upload(self.field.old_uri, self.field.bucket_name)
            except GoogleCloudException as e:
                logger.warning(
                    f"Could not delete existing google cloud file with uri: {self.field.uri}: {e}"
                )
        if self.field.upload_uri != self.key:
            await self.move(self.field.upload_uri, self.key, self.field.bucket_name, self.bucket)

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

    @storage_ops_observer.wrap({"type": "upload"})
    async def upload(self, iterator: AsyncIterator, origin: CloudFile) -> CloudFile:
        self.field = await self.start(origin)
        if self.field is None:
            raise AttributeError()
        await self.append(origin, iterator)
        await self.finish()
        return self.field

    def __repr__(self):
        return f"{self.storage.source}: {self.bucket}/{self.key}"


# Configuration Utility


class GCSStorage(Storage):
    field_klass = GCSStorageField
    session: Optional[aiohttp.ClientSession] = None
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
        anonymous: bool = False,
    ):
        if anonymous:
            self._json_credentials = None
            self._credentials = google.auth.credentials.AnonymousCredentials()
        else:
            if account_credentials is None:
                self._json_credentials = None
            elif isinstance(account_credentials, str) and account_credentials.strip() == "":
                self._json_credentials = None
            else:
                self._json_credentials = json.loads(base64.b64decode(account_credentials))

            if self._json_credentials is not None:
                self._credentials = service_account.Credentials.from_service_account_info(
                    self._json_credentials,
                    scopes=DEFAULT_SCOPES if scopes is None else scopes,
                )
            else:
                try:
                    self._credentials, self._project = google.auth.default()
                except DefaultCredentialsError:
                    logger.warning("Setting up without credentials as couldn't find workload identity")
                    self._credentials = None

        self.source = CloudFile.GCS
        self.deadletter_bucket = deadletter_bucket
        self.indexing_bucket = indexing_bucket
        self.bucket = bucket
        self._location = location
        self._project = project
        # https://cloud.google.com/storage/docs/bucket-locations
        self._bucket_labels = labels or {}
        self._executor = executor
        self._upload_url = url + "/upload/storage/v1/b/{bucket}/o"
        self.object_base_url = url + "/storage/v1/b"
        self._client = None

    def _get_access_token(self):
        if self._credentials.expired or self._credentials.valid is False:
            request = google.auth.transport.requests.Request()
            self._credentials.refresh(request)

        return self._credentials.token

    @storage_ops_observer.wrap({"type": "initialize"})
    async def initialize(self):
        loop = asyncio.get_event_loop()

        self.session = aiohttp.ClientSession(
            loop=loop, connector=aiohttp.TCPConnector(ttl_dns_cache=60 * 5), timeout=TIMEOUT
        )
        try:
            if self.deadletter_bucket is not None and self.deadletter_bucket != "":
                await self.create_bucket(self.deadletter_bucket)
        except Exception:  # pragma: no cover
            logger.exception(f"Could not create bucket {self.deadletter_bucket}", exc_info=True)

        try:
            if self.indexing_bucket is not None and self.indexing_bucket != "":
                await self.create_bucket(self.indexing_bucket)
        except Exception:  # pragma: no cover
            logger.exception(f"Could not create bucket {self.indexing_bucket}", exc_info=True)

    async def finalize(self):
        await self.session.close()

    async def get_access_headers(self):
        if self._credentials is None:
            return {}
        loop = asyncio.get_event_loop()
        token = await loop.run_in_executor(self._executor, self._get_access_token)
        return {"AUTHORIZATION": f"Bearer {token}"}

    @backoff.on_exception(
        backoff.expo,
        RETRIABLE_EXCEPTIONS,
        jitter=backoff.random_jitter,
        max_tries=MAX_TRIES,
    )
    @storage_ops_observer.wrap({"type": "delete"})
    async def delete_upload(self, uri: str, bucket_name: str):
        if self.session is None:
            raise AttributeError()
        if uri:
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
        else:
            raise AttributeError("No valid uri")

    @storage_ops_observer.wrap({"type": "check_bucket_exists"})
    async def check_exists(self, bucket_name: str):
        if self.session is None:
            raise AttributeError()

        headers = await self.get_access_headers()
        # Using object access url instead of bucket access to avoid
        # giving admin permission to the SA, needed to GET a bucket
        url = f"{self.object_base_url}/{bucket_name}/o"
        async with self.session.get(
            url,
            headers=headers,
        ) as resp:
            if resp.status == 200:
                logger.debug(f"Won't create bucket {bucket_name}, already exists")
                return True
        return False

    async def create_bucket(self, bucket_name: str, kbid: Optional[str] = None):
        if self.session is None:
            raise AttributeError()

        if await self.check_exists(bucket_name=bucket_name):
            return

        headers = await self.get_access_headers()

        url = f"{self.object_base_url}?project={self._project}"
        labels = deepcopy(self._bucket_labels)
        if kbid is not None:
            labels["kbid"] = kbid.lower()
        await self._create_bucket(url, headers, bucket_name, labels)

    @storage_ops_observer.wrap({"type": "create_bucket"})
    async def _create_bucket(self, url, headers, bucket_name, labels):
        async with self.session.post(
            url,
            headers=headers,
            json={
                "name": bucket_name,
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
            if resp.status != 200:  # pragma: no cover
                logger.info(f"Creation of bucket error: {resp.status}")
                text = await resp.text()
                logger.info(f"Bucket : {bucket_name}")
                logger.info(f"Location : {self._location}")
                logger.info(f"Labels : {labels}")
                logger.info(f"URL : {url}")
                logger.info(text)

                raise CouldNotCreateBucket(text)

    def get_bucket_name(self, kbid: str):
        if self.bucket is None:
            raise AttributeError()
        bucket_name = self.bucket.format(
            kbid=kbid,
        )
        return bucket_name

    async def create_kb(self, kbid: str) -> bool:
        bucket_name = self.get_bucket_name(kbid)
        created = False
        try:
            await self.create_bucket(bucket_name, kbid)
            created = True
        except Exception as e:
            logger.exception(f"Could not create bucket {kbid}", exc_info=e)
        return created

    @storage_ops_observer.wrap({"type": "schedule_delete"})
    async def schedule_delete_kb(self, kbid: str):
        if self.session is None:
            raise AttributeError()
        bucket_name = self.get_bucket_name(kbid)
        headers = await self.get_access_headers()
        url = f"{self.object_base_url}/{bucket_name}?fields=lifecycle"
        deleted = False
        async with self.session.patch(url, headers=headers, json=POLICY_DELETE) as resp:
            try:
                data = await resp.json()
            except Exception:
                text = await resp.text()
                data = {"text": text}
            if resp.status not in (200, 204, 404):
                if resp.status == 405:
                    # For testing purposes, gcs fixture doesn't have patch
                    logger.error("Not implemented")
                elif resp.status == 404:
                    logger.error(
                        f"Attempt to delete not found gcloud: {data}, status: {resp.status}",
                        exc_info=True,
                    )
                else:
                    raise GoogleCloudException(f"{resp.status}: {json.dumps(data)}")
            deleted = True
        return deleted

    @storage_ops_observer.wrap({"type": "delete"})
    async def delete_kb(self, kbid: str) -> tuple[bool, bool]:
        if self.session is None:
            raise AttributeError()
        bucket_name = self.get_bucket_name(kbid)
        headers = await self.get_access_headers()
        url = f"{self.object_base_url}/{bucket_name}"
        deleted = False
        conflict = False
        async with self.session.delete(url, headers=headers) as resp:
            if resp.status == 204:
                logger.info(f"Deleted bucket: {bucket_name}")
                deleted = True
            elif resp.status == 409:
                details = await resp.text()
                logger.info(f"Conflict on deleting bucket {bucket_name}: {details}")
                conflict = True
            elif resp.status == 404:
                logger.info(f"Does not exist on deleting: {bucket_name}")
            else:
                details = await resp.text()
                msg = f"Delete KB bucket returned an unexpected status {resp.status}: {details}"
                logger.error(msg, extra={"kbid": kbid})
                with errors.push_scope() as scope:
                    scope.set_extra("kbid", kbid)
                    scope.set_extra("status_code", resp.status)
                    errors.capture_message(msg, "error", scope)
        return deleted, conflict

    async def iterate_objects(
        self, bucket: str, prefix: str, start: Optional[str] = None
    ) -> AsyncGenerator[ObjectInfo, None]:
        if self.session is None:
            raise AttributeError()
        url = "{}/{}/o".format(self.object_base_url, bucket)
        headers = await self.get_access_headers()
        params = {"prefix": prefix}
        if start:
            params["startOffset"] = start
        async with self.session.get(
            url,
            headers=headers,
            params=params,
        ) as resp:
            assert resp.status == 200
            data = await resp.json()
            if "items" in data:
                for item in data["items"]:
                    if start is not None and item["name"] == start:
                        # Skip the start item to be compatible with all
                        # storage implementations
                        continue
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

    @backoff.on_exception(
        backoff.expo,
        RETRIABLE_EXCEPTIONS,
        jitter=backoff.random_jitter,
        max_tries=MAX_TRIES,
    )
    @storage_ops_observer.wrap({"type": "insert_object"})
    async def insert_object(self, bucket_name: str, key: str, data: bytes) -> None:
        """
        Put an object in the storage without any metadata.
        """
        if self.session is None:  # pragma: no cover
            raise AttributeError()
        bucket_upload_url = self._upload_url.format(bucket=bucket_name)
        url = f"{bucket_upload_url}?uploadType=media&name={quote_plus(key)}"
        headers = await self.get_access_headers()
        headers.update(
            {
                "Content-Length": str(len(data)),
                "Content-Type": "application/octet-stream",
            }
        )
        async with self.session.post(url, headers=headers, data=data) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise GoogleCloudException(f"{resp.status}: {text}")


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
    content_type = custom_metadata.get("content_type") or object_data.get("contentType") or ""

    # Parse filename
    filename = custom_metadata.get("filename") or key.split("/")[-1]

    return ObjectMetadata(
        filename=filename,
        size=int(size),
        content_type=content_type,
    )
