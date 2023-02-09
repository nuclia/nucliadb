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

import os
from typing import Any, AsyncIterable, Dict, Iterable, List, Optional, Union

import numpy as np

from nucliadb_models.labels import KnowledgeBoxLabels
from nucliadb_models.labels import Label as NDBLabel
from nucliadb_models.resource import Resource
from nucliadb_models.search import (
    KnowledgeboxSearchResults,
    SearchOptions,
    SearchRequest,
)
from nucliadb_models.vectors import VectorSet, VectorSets
from nucliadb_sdk import DEFAULT_LABELSET
from nucliadb_sdk.client import NucliaDBClient
from nucliadb_sdk.entities import Entities
from nucliadb_sdk.file import File
from nucliadb_sdk.labels import Label, Labels, LabelSet, LabelType
from nucliadb_sdk.resource import (
    create_resource,
    from_resource_to_payload,
    update_resource,
)
from nucliadb_sdk.search import SearchResult
from nucliadb_sdk.vectors import Vectors, convert_vector

NUCLIA_CLOUD = os.environ.get("NUCLIA_CLOUD_URL", ".nuclia.cloud")


class KnowledgeBox:
    vectorsets: Optional[VectorSets] = None
    id: str

    def __init__(self, client: NucliaDBClient):
        self.client = client
        self.id = self.client.reader_session.base_url.path.strip("/").split("/")[-1]

    def __iter__(self) -> Iterable[Resource]:
        for batch_resources in self.client.list_resources():
            for resource in batch_resources:
                yield resource

    async def __aiter__(self) -> AsyncIterable[Resource]:
        async for batch_resources in self.client.async_list_resources():
            for resource in batch_resources:
                yield resource

    def __len__(self):
        return self.client.length().resources

    async def async_len(self) -> int:
        return (await self.client.async_length()).resources

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
            item.slug = key
            self.client.create_resource(
                from_resource_to_payload(item, download=self.download)
            )
        else:
            self.client.update_resource(
                resource.id,
                from_resource_to_payload(item, download=self.download, update=True),
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
        binary: Optional[Union[File, str]] = None,
        text: Optional[str] = None,
        labels: Optional[Labels] = None,
        entities: Optional[Entities] = None,
        vectors: Optional[
            Union[Vectors, Dict[str, Union[np.ndarray, List[float]]]]
        ] = None,
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
            f"/labelset/{labelset}",
            json={
                "title": labelset,
                "labels": [NDBLabel(title=label).dict() for label in labels],
                "kind": [labelset_type.value],
            },
        )
        assert resp.status_code == 200

    def get_labels(self) -> KnowledgeBoxLabels:
        resp = self.client.get_labels()
        return resp

    def set_entities(self, entity_group: str, entities: List[str]):
        resp = self.client.writer_session.post(
            f"/entitiesgroup/{entity_group}",
            json={
                "title": entity_group,
                "entities": {entity: {"value": entity} for entity in entities},
            },
        )
        assert resp.status_code == 200

    def search(
        self,
        text: Optional[str] = None,
        filter: Optional[List[Union[Label, str]]] = None,
        vector: Optional[Union[np.ndarray, List[float]]] = None,
        vectorset: Optional[str] = None,
        min_score: Optional[float] = 0.0,
    ):
        result = self.client.search(
            self.build_search_request(text, filter, vector, vectorset, min_score)
        )
        return SearchResult(result, self.client)

    async def async_search(
        self,
        text: Optional[str] = None,
        filter: Optional[List[Union[Label, str]]] = None,
        vector: Optional[Union[np.ndarray, List[float]]] = None,
        vectorset: Optional[str] = None,
        min_score: Optional[float] = 0.0,
    ):
        result = await self.client.async_search(
            self.build_search_request(text, filter, vector, vectorset, min_score)
        )
        return SearchResult(result, self.client)

    def build_search_request(
        self,
        text: Optional[str] = None,
        filter: Optional[List[Union[Label, str]]] = None,
        vector: Optional[Union[np.ndarray, List[float]]] = None,
        vectorset: Optional[str] = None,
        min_score: Optional[float] = 0.0,
    ) -> SearchRequest:
        args: Dict[str, Any] = {"features": []}
        if filter is not None:
            new_filter: List[Label] = []
            for fil in filter:
                if isinstance(fil, str):
                    if len(fil.split("/")) == 1:
                        lset = DEFAULT_LABELSET
                        lab = fil
                    else:
                        lset, lab = fil.split("/")
                    new_filter.append(Label(label=lab, labelset=lset))
                else:
                    new_filter.append(fil)
            filter_list = [f"/l/{label.labelset}/{label.label}" for label in new_filter]
            args["filters"] = filter_list

        if text is not None:
            args["query"] = text
            args["features"].append(SearchOptions.DOCUMENT)
            args["features"].append(SearchOptions.PARAGRAPH)

        if vector is not None and vectorset is not None:
            vector = convert_vector(vector)

            args["vector"] = vector
            args["vectorset"] = vectorset
            args["features"].append(SearchOptions.VECTOR)

        if len(args["features"]) == 0:
            args["features"].append(SearchOptions.DOCUMENT)
            args["features"].append(SearchOptions.PARAGRAPH)

        args["min_score"] = min_score
        request = SearchRequest(**args)
        return request
