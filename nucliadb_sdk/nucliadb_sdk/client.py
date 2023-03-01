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

import base64
from enum import Enum
from typing import Optional

import httpx
import requests

from nucliadb_models.entities import KnowledgeBoxEntities
from nucliadb_models.labels import KnowledgeBoxLabels
from nucliadb_models.resource import Resource
from nucliadb_models.search import (
    KnowledgeboxCounters,
    KnowledgeboxSearchResults,
    SearchRequest,
)
from nucliadb_models.vectors import VectorSet, VectorSets
from nucliadb_models.writer import (
    CreateResourcePayload,
    ResourceCreated,
    UpdateResourcePayload,
)

RESOURCE_PATH = "/resource/{rid}"
RESOURCE_PATH_BY_SLUG = "/slug/{slug}"
SEARCH_PATH = "/search"
CREATE_RESOURCE_PATH = "/resources"
CREATE_VECTORSET = "/vectorset/{vectorset}"
VECTORSETS = "/vectorsets"
COUNTER = "/counters"
SEARCH_URL = "/search"
LABELS_URL = "/labelsets"
ENTITIES_URL = "/entitiesgroups"
DOWNLOAD_URL = "/{uri}"
TUS_UPLOAD_URL = "/resource/{rid}/file/{field}/tusupload"


class HTTPError(Exception):
    pass


class Environment(str, Enum):
    CLOUD = "CLOUD"
    OSS = "OSS"


class NucliaDBClient:
    api_key: Optional[str]
    environment: Environment
    session: httpx.Client
    url: Optional[str]

    def __init__(
        self,
        *,
        environment: Environment = Environment.CLOUD,
        url: Optional[str] = None,
        api_key: Optional[str] = None,
        writer_host: Optional[str] = None,
        reader_host: Optional[str] = None,
        search_host: Optional[str] = None,
        train_host: Optional[str] = None,
    ):
        self.api_key = api_key
        self.environment = environment
        self.url = url

        internal_hosts_set = all((writer_host, reader_host, search_host, train_host))
        url_set = bool(url)

        if not (url_set or internal_hosts_set):
            raise AttributeError("Either url or nucliadb services hosts must be set")

        if url_set:
            self.url = url
        elif internal_hosts_set:
            self.url = writer_host

        if environment == Environment.CLOUD and api_key is not None:
            reader_headers = {
                "X-STF-SERVICEACCOUNT": f"Bearer {api_key}",
            }
            writer_headers = {
                "X-STF-SERVICEACCOUNT": f"Bearer {api_key}",
            }
        elif environment == Environment.CLOUD and api_key is None:
            raise AttributeError("On Cloud you need to provide API Key")
        else:
            reader_headers = {
                "X-NUCLIADB-ROLES": f"READER",
            }
            writer_headers = {
                "X-NUCLIADB-ROLES": f"WRITER",
            }

        self.reader_session = httpx.Client(
            headers=reader_headers, base_url=reader_host or url  # type: ignore
        )
        self.async_reader_session = httpx.AsyncClient(
            headers=reader_headers, base_url=reader_host or url  # type: ignore
        )
        self.stream_session = requests.Session()
        self.stream_session.headers.update(reader_headers)
        self.writer_session = httpx.Client(
            headers=writer_headers, base_url=writer_host or url  # type: ignore
        )
        self.async_writer_session = httpx.AsyncClient(
            headers=writer_headers, base_url=writer_host or url  # type: ignore
        )
        self.search_session = httpx.Client(
            headers=reader_headers, base_url=search_host or url  # type: ignore
        )
        self.async_search_session = httpx.AsyncClient(
            headers=reader_headers, base_url=search_host or url  # type: ignore
        )
        self.train_session = httpx.Client(
            headers=reader_headers, base_url=train_host or url  # type: ignore
        )

    def get_resource(self, id: str):
        url = RESOURCE_PATH.format(rid=id)
        params = {
            "show": ["values", "relations", "origin", "basic"],
            "extracted": ["vectors", "text", "metadata", "link", "file"],
        }
        response = self.reader_session.get(
            url,
            params=params,
        )
        if response.status_code == 200:
            return Resource.parse_raw(response.content)
        elif response.status_code == 404:
            url = RESOURCE_PATH_BY_SLUG.format(slug=id)
            response = self.reader_session.get(url, params=params)
            if response.status_code == 200:
                return Resource.parse_raw(response.content)
            else:
                raise KeyError(f"No key {id}")
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_get_resource(self, id: str):
        url = RESOURCE_PATH.format(rid=id)
        params = {
            "show": ["values", "relations", "origin", "basic", "extracted"],
            "extracted": ["vectors", "text", "metadata", "link", "file"],
        }
        response = await self.async_reader_session.get(url, params=params)
        if response.status_code == 200:
            return Resource.parse_raw(response.content)
        elif response.status_code == 404:
            url = RESOURCE_PATH_BY_SLUG.format(slug=id)
            response = await self.async_reader_session.get(url, params=params)
            if response.status_code == 200:
                return Resource.parse_raw(response.content)
            else:
                raise KeyError(f"No key {id}")
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def del_resource(self, id: str):
        url = RESOURCE_PATH.format(rid=id)
        response = self.writer_session.delete(url)
        if response.status_code == 204:
            return
        elif response.status_code == 404:
            url = RESOURCE_PATH_BY_SLUG.format(slug=id)
            response = self.writer_session.delete(url)
            if response.status_code == 204:
                return
            elif response.status_code == 404:
                raise KeyError(f"No key {id}")
            else:
                raise HTTPError(f"Status code {response.status_code}: {response.text}")
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_del_resource(self, id: str):
        url = RESOURCE_PATH.format(rid=id)
        response = await self.async_writer_session.delete(url)
        if response.status_code == 204:
            return
        elif response.status_code == 404:
            url = RESOURCE_PATH_BY_SLUG.format(slug=id)
            response = await self.async_writer_session.delete(url)
            if response.status_code == 204:
                return
            elif response.status_code == 404:
                raise KeyError(f"No key {id}")
            else:
                raise HTTPError(f"Status code {response.status_code}: {response.text}")
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def list_resources(self):
        url = CREATE_RESOURCE_PATH
        response = self.reader_session.get(url)
        if response.status_code == 200:
            return Resource.parse_raw(response.content)
        elif response.status_code == 404:
            raise KeyError(f"No key {id}")
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_list_resources(self):
        url = CREATE_RESOURCE_PATH
        response = await self.async_reader_session.get(url)
        if response.status_code == 200:
            return Resource.parse_raw(response.content)
        elif response.status_code == 404:
            return KeyError(f"No key {id}")
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def set_vectorset(self, vectorset: str, payload: VectorSet):
        url = CREATE_VECTORSET.format(vectorset=vectorset)
        response = self.writer_session.post(url, content=payload.json())
        if response.status_code == 200:
            return
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_set_vectorset(self, vectorset: str, payload: VectorSet):
        url = CREATE_VECTORSET.format(vectorset=vectorset)
        response = await self.async_writer_session.post(url, content=payload.json())
        if response.status_code == 200:
            return
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def del_vectorset(self, vectorset: str):
        url = CREATE_VECTORSET.format(vectorset=vectorset)
        response = self.writer_session.delete(url)
        if response.status_code == 200:
            return
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_del_vectorset(self, vectorset: str):
        url = CREATE_VECTORSET.format(vectorset=vectorset)
        response = await self.async_writer_session.delete(url)
        if response.status_code == 200:
            return
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def get_vectorsets(self):
        url = VECTORSETS
        response = self.reader_session.get(url)
        if response.status_code == 200:
            return VectorSets.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_get_vectorsets(self):
        url = VECTORSETS
        response = await self.async_reader_session.get(url)
        if response.status_code == 200:
            return VectorSets.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def create_resource(self, payload: CreateResourcePayload) -> ResourceCreated:
        url = CREATE_RESOURCE_PATH
        response: httpx.Response = self.writer_session.post(url, content=payload.json())
        if response.status_code == 201:
            return ResourceCreated.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_create_resource(
        self, payload: CreateResourcePayload
    ) -> ResourceCreated:
        url = CREATE_RESOURCE_PATH
        response: httpx.Response = await self.async_writer_session.post(
            url, content=payload.json()
        )
        if response.status_code == 201:
            return ResourceCreated.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def update_resource(self, id: str, payload: UpdateResourcePayload):
        url = RESOURCE_PATH.format(rid=id)
        response: httpx.Response = self.writer_session.post(url, content=payload.json())
        if response.status_code == 200:
            return
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_update_resource(self, id: str, payload: UpdateResourcePayload):
        url = RESOURCE_PATH.format(rid=id)
        response: httpx.Response = await self.async_writer_session.post(
            url, content=payload.json()
        )
        if response.status_code == 200:
            return
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def length(self) -> KnowledgeboxCounters:
        url = COUNTER
        response: httpx.Response = self.search_session.get(url)
        if response.status_code == 200:
            return KnowledgeboxCounters.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_length(self) -> KnowledgeboxCounters:
        url = COUNTER
        response: httpx.Response = await self.async_search_session.get(url)
        if response.status_code == 200:
            return KnowledgeboxCounters.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def get_entities(self) -> KnowledgeBoxEntities:
        url = ENTITIES_URL
        response: httpx.Response = self.reader_session.get(url)
        if response.status_code == 200:
            return KnowledgeBoxEntities.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def get_labels(self) -> KnowledgeBoxLabels:
        url = LABELS_URL
        response: httpx.Response = self.reader_session.get(url)
        if response.status_code == 200:
            return KnowledgeBoxLabels.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def search(self, request: SearchRequest):
        url = SEARCH_URL
        response: httpx.Response = self.search_session.post(url, content=request.json())
        if response.status_code == 200:
            return KnowledgeboxSearchResults.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_search(self, request: SearchRequest):
        url = SEARCH_URL
        response: httpx.Response = await self.async_search_session.post(
            url, content=request.json()
        )
        if response.status_code == 200:
            return KnowledgeboxSearchResults.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def download(self, uri: str) -> bytes:
        # uri has format
        # /kb/2a00d5b4-cfcc-48eb-85ac-d70bfd38b26d/resource/41d02aac4ade48098b23e38141807738/file/file/download/field
        # we need to remove the kb url

        uri_parts = uri.split("/")
        if len(uri_parts) < 9:
            raise AttributeError("Not a valid download uri")

        new_uri = "/".join(uri_parts[3:])
        url = DOWNLOAD_URL.format(uri=new_uri)
        response: httpx.Response = self.reader_session.get(url)
        if response.status_code == 200:
            return response.content
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def start_tus_upload(
        self,
        rid: str,
        field: str,
        size: int,
        filename: str,
        content_type: str = "application/octet-stream",
    ):
        url = TUS_UPLOAD_URL.format(rid=rid, field=field)
        encoded_filename = base64.b64encode(filename.encode()).decode()
        headers = {
            "upload-length": str(size),
            "tus-resumable": "1.0.0",
            "upload-metadata": f"filename {encoded_filename}",
            "content-type": content_type,
        }
        response: httpx.Response = self.writer_session.post(url, headers=headers)
        if response.status_code == 201:
            return response.headers.get("Location")
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def patch_tus_upload(self, upload_url: str, data: bytes, offset: int) -> int:
        headers = {
            "upload-offset": str(offset),
        }
        # upload url has all path, we should remove /kb/kbid/
        upload_url = "/" + "/".join(upload_url.split("/")[3:])
        response: httpx.Response = self.writer_session.patch(
            upload_url, headers=headers, content=data
        )
        if response.status_code == 200:
            return int(response.headers.get("Upload-Offset"))
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")
