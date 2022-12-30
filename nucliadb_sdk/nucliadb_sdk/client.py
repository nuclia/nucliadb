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

RESOURCE_PATH = "{kburl}/resource/{rid}"
RESOURCE_PATH_BY_SLUG = "{kburl}/slug/{slug}"
SEARCH_PATH = "{kburl}/search"
CREATE_RESOURCE_PATH = "{kburl}/resources"
CREATE_VECTORSET = "{kburl}/vectorset/{vectorset}"
VECTORSETS = "{kburl}/vectorsets"
COUNTER = "{kburl}/counters"
SEARCH_URL = "{kburl}/search"
LABELS_URL = "{kburl}/labelsets"
ENTITIES_URL = "{kburl}/entitiesgroups"
DOWNLOAD_URL = "{kburl}/{uri}"


class HTTPError(Exception):
    pass


class Environment(str, Enum):
    CLOUD = "CLOUD"
    OSS = "OSS"


class NucliaDBClient:
    api_key: Optional[str] = None
    environment: Environment
    session: httpx.Client
    url: str

    def __init__(
        self, *, environment: Environment, url: str, api_key: Optional[str] = None
    ):
        self.api_key = api_key
        self.environment = environment
        self.url = url
        if environment == Environment.CLOUD and api_key is not None:
            reader_headers = {"X-STF-SERVICEACCOUNT": f"Bearer {api_key}"}
            writer_headers = {"X-STF-SERVICEACCOUNT": f"Bearer {api_key}"}
        elif environment == Environment.CLOUD and api_key is None:
            raise AttributeError("On Cloud you need to provide API Key")
        else:
            reader_headers = {"X-NUCLIADB-ROLES": f"READER"}
            writer_headers = {"X-NUCLIADB-ROLES": f"WRITER"}

        self.reader_session = httpx.Client(headers=reader_headers)
        self.async_reader_session = httpx.AsyncClient(headers=reader_headers)
        self.stream_session = requests.Session()
        self.stream_session.headers.update(reader_headers)
        self.writer_session = httpx.Client(headers=writer_headers)
        self.async_writer_session = httpx.AsyncClient(headers=writer_headers)

    def get_resource(self, id: str):
        url = RESOURCE_PATH.format(kburl=self.url, rid=id)
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
            url = RESOURCE_PATH_BY_SLUG.format(kburl=self.url, slug=id)
            response = self.reader_session.get(url, params=params)
            if response.status_code == 200:
                return Resource.parse_raw(response.content)
            else:
                raise KeyError(f"No key {id}")
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_get_resource(self, id: str):
        url = RESOURCE_PATH.format(kburl=self.url, rid=id)
        params = {
            "show": ["values", "relations", "origin", "basic"],
            "extracted": ["vectors", "text", "metadata", "link", "file"],
        }
        response = await self.async_reader_session.get(url, params=params)
        if response.status_code == 200:
            return Resource.parse_raw(response.content)
        elif response.status_code == 404:
            url = RESOURCE_PATH_BY_SLUG.format(kburl=self.url, slug=id)
            response = await self.async_reader_session.get(url, params=params)
            if response.status_code == 200:
                return Resource.parse_raw(response.content)
            else:
                raise KeyError(f"No key {id}")
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def del_resource(self, id: str):
        url = RESOURCE_PATH.format(kburl=self.url, rid=id)
        response = self.writer_session.delete(url)
        if response.status_code == 204:
            return
        elif response.status_code == 404:
            url = RESOURCE_PATH_BY_SLUG.format(kburl=self.url, slug=id)
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
        url = RESOURCE_PATH.format(kburl=self.url, rid=id)
        response = await self.async_writer_session.delete(url)
        if response.status_code == 204:
            return
        elif response.status_code == 404:
            url = RESOURCE_PATH_BY_SLUG.format(kburl=self.url, slug=id)
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
        url = CREATE_RESOURCE_PATH.format(kburl=self.url)
        response = self.reader_session.get(url)
        if response.status_code == 200:
            return Resource.parse_raw(response.content)
        elif response.status_code == 404:
            raise KeyError(f"No key {id}")
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_list_resources(self):
        url = CREATE_RESOURCE_PATH.format(kburl=self.url)
        response = await self.async_reader_session.get(url)
        if response.status_code == 200:
            return Resource.parse_raw(response.content)
        elif response.status_code == 404:
            return KeyError(f"No key {id}")
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def set_vectorset(self, vectorset: str, payload: VectorSet):
        url = CREATE_VECTORSET.format(kburl=self.url, vectorset=vectorset)
        response = self.writer_session.post(url, content=payload.json())
        if response.status_code == 200:
            return
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_set_vectorset(self, vectorset: str, payload: VectorSet):
        url = CREATE_VECTORSET.format(kburl=self.url, vectorset=vectorset)
        response = await self.async_writer_session.post(url, content=payload.json())
        if response.status_code == 200:
            return
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def del_vectorset(self, vectorset: str):
        url = CREATE_VECTORSET.format(kburl=self.url, vectorset=vectorset)
        response = self.writer_session.delete(url)
        if response.status_code == 200:
            return
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_del_vectorset(self, vectorset: str):
        url = CREATE_VECTORSET.format(kburl=self.url, vectorset=vectorset)
        response = await self.async_writer_session.delete(url)
        if response.status_code == 200:
            return
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def get_vectorsets(self):
        url = VECTORSETS.format(kburl=self.url)
        response = self.reader_session.get(url)
        if response.status_code == 200:
            return VectorSets.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_get_vectorsets(self):
        url = VECTORSETS.format(kburl=self.url)
        response = await self.async_reader_session.get(url)
        if response.status_code == 200:
            return VectorSets.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def create_resource(self, payload: CreateResourcePayload) -> ResourceCreated:
        url = CREATE_RESOURCE_PATH.format(kburl=self.url)
        response: httpx.Response = self.writer_session.post(url, content=payload.json())
        if response.status_code == 201:
            return ResourceCreated.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_create_resource(
        self, payload: CreateResourcePayload
    ) -> ResourceCreated:
        url = CREATE_RESOURCE_PATH.format(kburl=self.url)
        response: httpx.Response = await self.async_writer_session.post(
            url, content=payload.json()
        )
        if response.status_code == 201:
            return ResourceCreated.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def update_resource(self, id: str, payload: UpdateResourcePayload):
        url = RESOURCE_PATH.format(kburl=self.url, rid=id)
        response: httpx.Response = self.writer_session.post(url, content=payload.json())
        if response.status_code == 200:
            return
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_update_resource(self, id: str, payload: UpdateResourcePayload):
        url = RESOURCE_PATH.format(kburl=self.url, rid=id)
        response: httpx.Response = await self.async_writer_session.post(
            url, content=payload.json()
        )
        if response.status_code == 200:
            return
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def lenght(self) -> KnowledgeboxCounters:
        url = COUNTER.format(kburl=self.url)
        response: httpx.Response = self.reader_session.get(url)
        if response.status_code == 200:
            return KnowledgeboxCounters.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_lenght(self) -> KnowledgeboxCounters:
        url = COUNTER.format(kburl=self.url)
        response: httpx.Response = await self.async_reader_session.get(url)
        if response.status_code == 200:
            return KnowledgeboxCounters.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def get_entities(self) -> KnowledgeBoxEntities:
        url = ENTITIES_URL.format(kburl=self.url)
        response: httpx.Response = self.reader_session.get(url)
        if response.status_code == 200:
            return KnowledgeBoxEntities.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def get_labels(self) -> KnowledgeBoxLabels:
        url = LABELS_URL.format(kburl=self.url)
        response: httpx.Response = self.reader_session.get(url)
        if response.status_code == 200:
            return KnowledgeBoxLabels.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    def search(self, request: SearchRequest):
        url = SEARCH_URL.format(kburl=self.url)
        response: httpx.Response = self.reader_session.post(url, content=request.json())
        if response.status_code == 200:
            return KnowledgeboxSearchResults.parse_raw(response.content)
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")

    async def async_search(self, request: SearchRequest):
        url = SEARCH_URL.format(kburl=self.url)
        response: httpx.Response = await self.async_reader_session.post(
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
        url = DOWNLOAD_URL.format(kburl=self.url, uri=new_uri)
        response: httpx.Response = self.reader_session.get(url)
        if response.status_code == 200:
            return response.content
        else:
            raise HTTPError(f"Status code {response.status_code}: {response.text}")
