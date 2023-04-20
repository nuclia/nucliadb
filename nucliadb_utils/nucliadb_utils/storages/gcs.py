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
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Optional
from urllib.parse import quote_plus

import aiohttp
import backoff  # type: ignore
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
from nucliadb_utils.storages.storage import Storage, StorageField

storage_ops_observer = metrics.Observer("gcs_ops", labels={"type": ""})


def strip_query_params(url: yarl.URL) -> str:
    return str(url.with_query(None))


MAX_SIZE = 1073741824

DEFAULT_SCOPES = ["https://www.googleapis.com/auth/devstorage.read_write"]
MAX_RETRIES = 5

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


RETRIABLE_EXCEPTIONS = (
    GoogleCloudException,
    aiohttp.client_exceptions.ClientPayloadError,
    aiohttp.client_exceptions.ClientConnectorError,
    aiohttp.client_exceptions.ClientOSError,
    aiohttp.client_exceptions.ServerConnectionError,
    CouldNotCreateBucket,
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

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=4)
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

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=4)
    @storage_ops_observer.wrap({"type": "iter_data"})
    async def iter_data(self, headers=None):
        if headers is None:
            headers = {}

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
        headers.update(await self.storage.get_access_headers())

        async with self.storage.session.get(
            url, headers=headers, params={"alt": "media"}, timeout=-1
        ) as api_resp:
            if api_resp.status not in (200, 206):
                text = await api_resp.text()
                if api_resp.status == 404:
                    raise KeyError(f"Google cloud file not found : \n {text}")
                elif api_resp.status == 401:
                    logger.warning(f"Invalid google cloud credentials error: {text}")
                    raise KeyError(
                        content={f"Google cloud invalid credentials : \n {text}"}
                    )
                raise GoogleCloudException(f"{api_resp.status}: {text}")
            while True:
                chunk = await api_resp.content.read(1024 * 1024)
                if len(chunk) > 0:
                    yield chunk
                else:
                    break

    async def range_supported(self) -> bool:
        return True

    @storage_ops_observer.wrap({"type": "read_range"})
    async def read_range(self, start: int, end: int) -> AsyncIterator[bytes]:
        """
        Iterate through ranges of data
        """
        async for chunk in self.iter_data(
            headers={"Range": f"bytes={start}-{end - 1}"}
        ):
            yield chunk

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=4)
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
        max_tries=4,
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

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=4)
    @storage_ops_observer.wrap({"type": "exists"})
    async def exists(self) -> Optional[Dict[str, str]]:
        """
        Existence can be checked either with a CloudFile data in the field attribute
        or own StorageField key and bucket. Field takes precendece
        """
        if self.storage.session is None:
            raise AttributeError()
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

        url = "{}/{}/o/{}".format(
            self.storage.object_base_url,
            bucket,
            quote_plus(key),
        )
        headers = await self.storage.get_access_headers()
        async with self.storage.session.get(url, headers=headers) as api_resp:
            if api_resp.status == 200:
                data = await api_resp.json()
                metadata = data.get("metadata")
                if metadata is None:
                    metadata = {}
                if metadata.get("SIZE") is None:
                    metadata["SIZE"] = data.get("size")
                if metadata.get("CONTENT_TYPE") is None:
                    metadata["CONTENT_TYPE"] = data.get("contentType")
                if metadata.get("FILENAME") is None:
                    metadata["FILENAME"] = key.split("/")[-1]
                return metadata
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
    ):
        if account_credentials is not None:
            self._json_credentials = json.loads(base64.b64decode(account_credentials))
            self._credentials = service_account.Credentials.from_service_account_info(
                self._json_credentials,
                scopes=DEFAULT_SCOPES if scopes is None else scopes,
            )
        self.source = CloudFile.GCS
        self.deadletter_bucket = deadletter_bucket
        self.indexing_bucket = indexing_bucket
        self.bucket = bucket
        self._location = location
        self._project = project
        # https://cloud.google.com/storage/docs/bucket-locations
        self._bucket_labels = labels or {}
        self._executor = executor
        self._creation_access_token = datetime.now()
        self._upload_url = (
            url + "/upload/storage/v1/b/{bucket}/o?uploadType=resumable"
        )  # noqa
        self.object_base_url = url + "/storage/v1/b"
        self._client = None

    def _get_access_token(self):
        if self._credentials.valid is False:
            req = google.auth.transport.requests.Request()
            self._credentials.refresh(req)
            self._creation_access_token = datetime.now()
        return self._credentials.token

    @storage_ops_observer.wrap({"type": "initialize"})
    async def initialize(self, service_name: Optional[str] = None):
        loop = asyncio.get_event_loop()

        await setup_telemetry(service_name or "GCS_SERVICE")
        self.session = aiohttp.ClientSession(loop=loop)

        try:
            if self.deadletter_bucket is not None and self.deadletter_bucket != "":
                await self.create_bucket(self.deadletter_bucket)
        except Exception:  # pragma: no cover
            logger.exception(
                f"Could not create bucket {self.deadletter_bucket}", exc_info=True
            )

        try:
            if self.indexing_bucket is not None and self.indexing_bucket != "":
                await self.create_bucket(self.indexing_bucket)
        except Exception:  # pragma: no cover
            logger.exception(
                f"Could not create bucket {self.indexing_bucket}", exc_info=True
            )

    async def finalize(self):
        await self.session.close()

    async def get_access_headers(self):
        if self._credentials is None:
            return {}
        loop = asyncio.get_event_loop()
        token = await loop.run_in_executor(self._executor, self._get_access_token)
        return {"AUTHORIZATION": f"Bearer {token}"}

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=4)
    @storage_ops_observer.wrap({"type": "delete"})
    async def delete_upload(self, uri: str, bucket_name: str):
        if self.session is None:
            raise AttributeError()
        if uri is not None:
            url = "{}/{}/o/{}".format(
                self.object_base_url, bucket_name, quote_plus(uri)
            )
            headers = await self.get_access_headers()
            async with self.session.delete(url, headers=headers) as resp:
                try:
                    data = await resp.json()
                except Exception:
                    text = await resp.text()
                    data = {"text": text}
                if resp.status not in (200, 204, 404):
                    if resp.status == 404:
                        logger.error(
                            f"Attempt to delete not found gcloud: {data}, "
                            f"status: {resp.status}",
                            exc_info=True,
                        )
                    else:
                        raise GoogleCloudException(f"{resp.status}: {json.dumps(data)}")
        else:
            raise AttributeError("No valid uri")

    @storage_ops_observer.wrap({"type": "check_bucket_exists"})
    async def check_exists(self, bucket_name: str):
        if self.session is None:
            raise AttributeError()

        headers = await self.get_access_headers()
        url = f"{self.object_base_url}/{bucket_name}?project={self._project}"
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
        exists = await self.check_exists(bucket_name=bucket_name)
        if exists:
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
                        f"Attempt to delete not found gcloud: {data}, "
                        f"status: {resp.status}",
                        exc_info=True,
                    )
                else:
                    raise GoogleCloudException(f"{resp.status}: {json.dumps(data)}")
            deleted = True
        return deleted

    @storage_ops_observer.wrap({"type": "delete"})
    async def delete_kb(self, kbid: str):
        if self.session is None:
            raise AttributeError()
        bucket_name = self.get_bucket_name(kbid)
        headers = await self.get_access_headers()
        url = f"{self.object_base_url}/{bucket_name}"
        deleted = False
        conflict = False
        async with self.session.delete(url, headers=headers) as resp:
            if resp.status == 200:
                logger.info(f"Deleted bucket: {bucket_name}")
                deleted = True
            if resp.status == 409:
                logger.info(f"Conflict on deleting: {bucket_name}")
                conflict = True
            if resp.status == 404:
                logger.info(f"Does not exit on deleting: {bucket_name}")
        return deleted, conflict

    async def iterate_bucket(self, bucket: str, prefix: str) -> AsyncIterator[Any]:
        if self.session is None:
            raise AttributeError()
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
                    yield item

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
                    yield item
                page_token = data.get("nextPageToken")
