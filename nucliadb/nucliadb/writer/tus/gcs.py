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
import os
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime
from typing import AsyncIterator, Dict, Optional
from urllib.parse import quote_plus

import aiohttp
import backoff  # type: ignore
from nucliadb_protos.resources_pb2 import CloudFile
from oauth2client.service_account import ServiceAccountCredentials  # type: ignore

from nucliadb.writer import logger
from nucliadb.writer.tus.dm import FileDataMangaer
from nucliadb.writer.tus.exceptions import (
    CloudFileNotFound,
    HTTPBadRequest,
    HTTPNotFound,
    HTTPPreconditionFailed,
    ResumableURINotAvailable,
)
from nucliadb.writer.tus.storage import BlobStore, FileStorageManager
from nucliadb.writer.tus.utils import to_str


class GoogleCloudException(Exception):
    pass


SCOPES = ["https://www.googleapis.com/auth/devstorage.read_write"]
MAX_RETRIES = 5


RETRIABLE_EXCEPTIONS = (
    GoogleCloudException,
    aiohttp.client_exceptions.ClientPayloadError,
)
CHUNK_SIZE = 524288


class GCloudBlobStore(BlobStore):
    session: Optional[aiohttp.ClientSession] = None
    loop = None
    upload_url: str
    object_base_url: str
    json_credentials: str
    bucket: str
    location: str
    project: str
    executor = ThreadPoolExecutor(max_workers=5)

    async def get_access_headers(self):
        if self._credentials is None:
            return {}
        loop = asyncio.get_event_loop()
        token = await loop.run_in_executor(self.executor, self._get_access_token)
        return {"AUTHORIZATION": f"Bearer {token}"}

    def _get_access_token(self):
        access_token = self._credentials.get_access_token()
        self._creation_access_token = datetime.now()
        return access_token.access_token

    async def finalize(self):
        if self.session is not None:
            await self.session.close()

    async def initialize(
        self,
        bucket: str,
        location: str,
        project: str,
        bucket_labels,
        object_base_url: str,
        json_credentials: Optional[str],
    ):
        self.bucket = bucket
        self.source = CloudFile.Source.GCS
        self.location = location
        self.project = project
        self.bucket_labels = bucket_labels
        self.object_base_url = object_base_url + "/storage/v1/b"
        self.upload_url = (
            object_base_url + "/upload/storage/v1/b/{bucket}/o?uploadType=resumable"
        )  # noqa

        self._credentials = None

        if json_credentials is not None:
            self.json_credentials_file = os.path.join(
                tempfile.mkdtemp(), "gcs_credentials.json"
            )
            open(self.json_credentials_file, "w").write(
                base64.b64decode(json_credentials).decode("utf-8")
            )
            self._credentials = ServiceAccountCredentials.from_json_keyfile_name(
                self.json_credentials_file, SCOPES
            )

        loop = asyncio.get_event_loop()
        self.session = aiohttp.ClientSession(loop=loop)

    async def check_exists(self, bucket_name: str):
        if self.session is None:
            raise AttributeError()

        headers = await self.get_access_headers()
        url = f"{self.object_base_url}/{bucket_name}?project={self.project}"
        async with self.session.get(
            url,
            headers=headers,
        ) as resp:
            if resp.status == 200:
                logger.debug(f"Won't create bucket {bucket_name}, already exists")
                return True
        return False

    async def create_bucket(self, bucket_name: str):
        if self.session is None:
            raise AttributeError()
        headers = await self.get_access_headers()
        url = f"{self.object_base_url}?project={self.project}"

        found = False
        labels = deepcopy(self.bucket_labels)
        async with self.session.post(
            url,
            headers=headers,
            json={
                "name": bucket_name,
                "location": self.location,
                "labels": labels,
                "iamConfiguration.uniformBucketLevelAccess.enabled": True,
            },
        ) as resp:
            found = resp.status == 409
            if not found:
                assert resp.status == 200
        return found


class GCloudFileStorageManager(FileStorageManager):
    storage: GCloudBlobStore
    chunk_size = CHUNK_SIZE

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=4)
    async def start(self, dm: FileDataMangaer, path: str, kbid: str):
        """Init an upload.

        _uload_file_id : temporal url to image beeing uploaded
        _resumable_uri : uri to resumable upload
        _uri : finished uploaded image
        """
        if self.storage.session is None:
            raise AttributeError()

        upload_file_id = dm.get("upload_file_id", str(uuid.uuid4()))
        if upload_file_id is not None:
            await self.delete_upload(upload_file_id, kbid)

        bucket = self.storage.get_bucket_name(kbid)
        init_url = "{}&name={}".format(
            self.storage.upload_url.format(bucket=bucket),
            quote_plus(path),
        )
        if dm.filename == 0:
            filename = "file"
        else:
            filename = dm.filename
        metadata = json.dumps(
            {
                "metadata": {
                    "FILENAME": filename,
                    "CONTENT_TYPE": dm.content_type,
                    "SIZE": str(dm.size),
                }
            }
        )
        call_size = len(metadata)
        headers = await self.storage.get_access_headers()

        headers.update(
            {
                "Content-Type": "application/json; charset=UTF-8",
                "Content-Length": str(call_size),
            }
        )
        if dm.content_type:
            headers["X-Upload-Content-Type"] = dm.content_type

        if dm.size:
            headers["X-Upload-Content-Length"] = str(dm.size)

        async with self.storage.session.post(
            init_url,
            headers=headers,
            data=metadata,
        ) as call:
            if call.status != 200:
                text = await call.text()
                raise GoogleCloudException(text)
            resumable_uri = call.headers["Location"]

        await dm.update(
            resumable_uri=resumable_uri, upload_file_id=upload_file_id, path=path
        )

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=4)
    async def delete_upload(self, uri, kbid):
        bucket = self.storage.get_bucket_name(kbid)

        if uri is not None:
            url = "{}/{}/o/{}".format(
                self.storage.object_base_url,
                bucket,
                quote_plus(uri),
            )
            headers = await self.storage.get_access_headers()
            async with self.storage.session.delete(
                url,
                headers=headers,
            ) as resp:
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

    async def _append(self, dm: FileDataMangaer, data, offset):
        if self.storage.session is None:
            raise AttributeError()
        if dm.size:
            size = str(dm.size)
        else:
            # assuming size will come eventually
            size = "*"
        content_range = "bytes {init}-{chunk}/{total}".format(
            init=offset, chunk=offset + len(data) - 1, total=size
        )
        resumable_uri = dm.get("resumable_uri")
        if resumable_uri is None:
            raise ResumableURINotAvailable()

        content_type = dm.content_type
        if content_type is None:
            content_type = "application/octet-stream"
        else:
            content_type = to_str(dm.content_type)
        async with self.storage.session.put(
            resumable_uri,
            headers={
                "Content-Length": str(len(data)),
                "Content-Type": content_type,
                "Content-Range": content_range,
            },
            data=data,
        ) as call:
            text = await call.text()  # noqa
            if call.status not in [200, 201, 308]:
                logger.error(text)
            return call

    async def append(self, dm: FileDataMangaer, iterable, offset) -> int:
        count = 0

        async for chunk in iterable:
            resp = await self._append(dm, chunk, offset)
            size = len(chunk)
            count += size
            offset += len(chunk)

            if resp.status == 308:
                # verify we're on track with google's resumable api...
                range_header = resp.headers["Range"]
                if offset - 1 != int(range_header.split("-")[-1]):
                    # range header is the byte range google has received,
                    # which is different from the total size--off by one
                    raise HTTPPreconditionFailed(
                        detail=f"proxy and google cloud storage "
                        f"offsets do not match. Google: "
                        f"{range_header}, TUS(offset): {offset}"
                    )
            elif resp.status == 400:
                # Known error here is that we specfied a wrong chunk size
                # but any error should break the upload and be reported to the client
                raise HTTPBadRequest(detail=await resp.text())

            elif resp.status in [200, 201]:
                # file manager will double check offsets and sizes match
                break
        return count

    async def finish(self, dm: FileDataMangaer):
        if dm.size == 0:
            if self.storage.session is None:
                raise AttributeError()
            # If there is been no size finish the upload
            content_range = "bytes {init}-{chunk}/{total}".format(
                init=dm.offset, chunk=dm.offset, total=dm.offset
            )
            resumable_uri = dm.get("resumable_uri")
            async with self.storage.session.put(
                resumable_uri,
                headers={
                    "Content-Length": "0",
                    "Content-Range": content_range,
                },
                data="",
            ) as call:
                text = await call.text()  # noqa
                if call.status not in [200, 201, 308]:
                    logger.error(text)
                return call
        path = dm.get("path")
        await dm.finish()
        return path

    async def iter_data(self, uri, kbid: str, headers: Optional[Dict[str, str]] = None):
        if self.storage.session is None:
            raise AttributeError()
        if headers is None:
            headers = {}

        url = "{}/{}/o/{}".format(
            self.storage.object_base_url,
            self.storage.get_bucket_name(kbid),
            quote_plus(uri),
        )
        headers_auth = await self.storage.get_access_headers()
        headers.update(headers_auth)
        async with self.storage.session.get(
            url, headers=headers, params={"alt": "media"}, timeout=-1
        ) as api_resp:
            if api_resp.status not in (200, 206):
                text = await api_resp.text()
                if api_resp.status == 404:
                    raise CloudFileNotFound("Google cloud file not found")
                elif api_resp.status == 401:
                    logger.warning(f"Invalid google cloud credentials error: {text}")
                    raise HTTPNotFound(
                        detail=f"Google cloud invalid credentials: {text}"
                    )
                raise GoogleCloudException(f"{api_resp.status}: {text}")
            while True:
                chunk = await api_resp.content.read(1024 * 1024)
                if len(chunk) > 0:
                    yield chunk
                else:
                    break

    async def read_range(
        self, uri: str, kbid: str, start: int, end: int
    ) -> AsyncIterator[bytes]:
        """
        Iterate through ranges of data
        """
        async for chunk in self.iter_data(
            uri, kbid, headers={"Range": f"bytes={start}-{end - 1}"}
        ):
            yield chunk
