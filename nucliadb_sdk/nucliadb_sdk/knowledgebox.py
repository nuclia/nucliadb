import os
from typing import Any, AsyncIterable, Dict, Iterable, List, Optional
from urllib.parse import urlparse

from nucliadb_models.labels import Label as NDBLabel
from nucliadb_models.resource import Resource
from nucliadb_models.search import (
    KnowledgeboxSearchResults,
    SearchOptions,
    SearchRequest,
)
from nucliadb_models.vectors import VectorSet, VectorSets
from nucliadb_sdk.client import Environment, NucliaDBClient
from nucliadb_sdk.entities import Entities
from nucliadb_sdk.file import File
from nucliadb_sdk.labels import Label, Labels, LabelSet, LabelType
from nucliadb_sdk.resource import (
    create_resource,
    from_resource_to_payload,
    update_resource,
)
from nucliadb_sdk.vectors import Vector, Vectors

NUCLIA_CLOUD = os.environ.get("NUCLIA_CLOUD_URL", ".nuclia.cloud")


class KnowledgeBox:
    environment: Environment
    vectorsets: Optional[VectorSets] = None
    api_key: Optional[str] = None
    id: str

    def __init__(self, url: str, api_key: Optional[str] = None):
        self.id = url.split("/")[-1]
        url_obj = urlparse(url)
        if url_obj.hostname is not None and url_obj.hostname.endswith(NUCLIA_CLOUD):
            env = Environment.CLOUD
            self.api_key = api_key
        else:
            env = Environment.OSS
        self.client = NucliaDBClient(environment=env, url=url, api_key=self.api_key)

    def __iter__(self) -> Iterable[Resource]:
        for batch_resources in self.client.list_resources():
            for resource in batch_resources:
                yield resource

    async def __aiter__(self) -> AsyncIterable[Resource]:
        async for batch_resources in self.client.async_list_resources():
            for resource in batch_resources:
                yield resource

    def __len__(self):
        return self.client.lenght().resources

    async def async_len(self) -> int:
        return (await self.client.async_lenght()).resources

    def __getitem__(self, key: str) -> Resource:
        return self.client.get_resource(id=key)

    def download(self, uri: str) -> bytes:
        return self.client.download(uri=uri)

    def get(self, key: str, default: Optional[Resource] = None) -> Resource:
        try:
            result = self.client.get_resource(id=key)
        except KeyError:
            result = default
        return result

    async def async_get(self, key: str, default: Optional[Resource] = None) -> Resource:
        try:
            result = await self.client.async_get_resource(id=key)
        except KeyError:
            result = default
        return result

    def __setitem__(self, key: str, item: Resource):
        resource = self.get(key)
        if resource is None:
            item.id = key
            self.client.create_resource(
                from_resource_to_payload(item, download=self.download)
            )
        else:
            self.client.update_resource(
                key, from_resource_to_payload(item, download=self.download, update=True)
            )

    def __delitem__(self, key):
        self.client.del_resource(id=key)

    async def async_del(self, key):
        await self.client.async_del_resource(id=key)

    async def async_new_vectorset(self, key: str, dimension: int):
        await self.client.async_set_vectorset(key, VectorSet(dimension=dimension))

    def new_vectorset(self, key: str, dimension: int):
        self.client.set_vectorset(key, VectorSet(dimension=dimension))

    async def async_list_vectorset(self) -> VectorSets:
        return self.client.async_get_vectorsets()

    def list_vectorset(self) -> VectorSets:
        return self.client.get_vectorsets()

    async def async_del_vectorset(self, key: str):
        await self.client.async_del_vectorset(key)

    def del_vectorset(self, key: str):
        self.client.del_vectorset(key)

    async def async_upload(
        self,
        key: Optional[str] = None,
        binary: Optional[File] = None,
        text: Optional[str] = None,
        labels: Optional[Labels] = None,
        entities: Optional[Entities] = None,
        vectors: Optional[Vectors] = None,
    ):
        resource: Optional[Resource] = None

        if key is None:
            creating = True
        else:
            resource = await self.async_get(key)
            if resource is None:
                creating = True
            else:
                creating = False

        if vectors is not None and self.vectorsets is None:
            self.vectorsets = await self.client.async_get_vectorsets()

        if creating:
            create_payload = create_resource(
                key=key,
                text=text,
                binary=binary,
                labels=labels,
                entities=entities,
                vectors=vectors,
                vectorsets=self.vectorsets,
            )
            resp = await self.client.async_create_resource(create_payload)
            rid = resp.uuid

        else:
            assert resource is not None

            update_payload = update_resource(
                text=text,
                binary=binary,
                labels=labels,
                entities=entities,
                vectors=vectors,
                resource=resource,
                vectorsets=self.vectorsets,
            )
            rid = resource.id
            await self.client.async_update_resource(rid, update_payload)
        return rid

    def upload(
        self,
        key: Optional[str] = None,
        binary: Optional[File] = None,
        text: Optional[str] = None,
        labels: Optional[Labels] = None,
        entities: Optional[Entities] = None,
        vectors: Optional[Vectors] = None,
    ) -> str:
        resource: Optional[Resource] = None

        if key is None:
            creating = True
        else:
            resource = self.get(key)
            if resource is None:
                creating = True
            else:
                creating = False

        if vectors is not None and self.vectorsets is None:
            self.vectorsets = self.client.get_vectorsets()

        if creating:
            create_payload = create_resource(
                key=key,
                text=text,
                binary=binary,
                labels=labels,
                entities=entities,
                vectors=vectors,
                vectorsets=self.vectorsets,
            )
            resp = self.client.create_resource(create_payload)
            rid = resp.uuid

        else:
            assert resource is not None
            update_payload = update_resource(
                text=text,
                binary=binary,
                labels=labels,
                entities=entities,
                vectors=vectors,
                resource=resource,
                vectorsets=self.vectorsets,
            )
            rid = resource.id
            self.client.update_resource(rid, update_payload)
        return rid

    def process_uploaded_labels_from_search(
        self, search_result: KnowledgeboxSearchResults
    ) -> Dict[str, LabelSet]:

        response: Dict[str, LabelSet] = {}
        if search_result.fulltext is None or search_result.fulltext.facets is None:
            return response
        for labelset, count in search_result.fulltext.facets.get("/l", {}).items():
            real_labelset = labelset[3:]  # removing /l/
            response[real_labelset] = LabelSet(count=count)

        for labelset, labelset_obj in response.items():
            base_label = f"/l/{labelset}"
            search_result = self.client.search(
                SearchRequest(features=["document"], faceted=[base_label], page_size=0)
            )
            if search_result.fulltext is None or search_result.fulltext.facets is None:
                raise Exception("Search error")
            for label, count in search_result.fulltext.facets.get(
                base_label, {}
            ).items():
                labelset_obj.labels[label.replace(base_label + "/", "")] = count
        return response

    def get_uploaded_labels(self) -> Dict[str, LabelSet]:
        # Search for fulltext with faceted for labelsets
        search_result = self.client.search(
            SearchRequest(features=["document"], faceted=["/l"], page_size=0)
        )

        return self.process_uploaded_labels_from_search(search_result)

    async def async_get_uploaded_labels(self) -> Dict[str, LabelSet]:
        # Search for fulltext with faceted for labelsets
        search_result = await self.client.async_search(
            SearchRequest(features=["document"], faceted=["/l"], page_size=0)
        )

        return self.process_uploaded_labels_from_search(search_result)

    def set_labels(self, labelset: str, labels: List[str], labelset_type: LabelType):
        resp = self.client.writer_session.post(
            f"{self.client.url}/labelset/{labelset}",
            json={
                "title": labelset,
                "labels": [NDBLabel(title=label).dict() for label in labels],
                "kind": [labelset_type.value],
            },
        )
        assert resp.status_code == 200

    def set_entities(self, entity_group: str, entities: List[str]):
        resp = self.client.writer_session.post(
            f"{self.client.url}/entitiesgroup/{entity_group}",
            json={
                "title": entity_group,
                "entities": {entity: {"value": entity} for entity in entities},
            },
        )
        assert resp.status_code == 200

    def search(
        self,
        text: Optional[str] = None,
        filter: Optional[List[Label]] = None,
        vector: Optional[Vector] = None,
    ):
        args: Dict[str, Any] = {"features": []}
        if filter is not None:
            filter_list = [f"/l/{label.labelset}/{label.label}" for label in filter]
            args["filters"] = filter_list

        if text is not None:
            args["query"] = text
            args["features"].append(SearchOptions.DOCUMENT)
            args["features"].append(SearchOptions.PARAGRAPH)

        if vector is not None:
            args["vector"] = vector.value
            args["vectorset"] = vector.vectorset
            args["features"].append(SearchOptions.VECTOR)

        request = SearchRequest(**args)
        return self.client.search(request)

    async def async_search(
        self,
        text: Optional[str] = None,
        filter: Optional[List[Label]] = None,
        vector: Optional[Vector] = None,
    ):
        args: Dict[str, Any] = {"features": []}
        if filter is not None:
            filter_list = [f"/l/{label.labelset}/{label.label}" for label in filter]
            args["filters"] = filter_list

        if text is not None:
            args["query"] = text
            args["features"].append(SearchOptions.DOCUMENT)
            args["features"].append(SearchOptions.PARAGRAPH)

        if vector is not None:
            args["vector"] = vector.value
            args["vectorset"] = vector.vectorset
            args["features"].append(SearchOptions.VECTOR)

        request = SearchRequest(**args)
        return await self.client.async_search(request)
