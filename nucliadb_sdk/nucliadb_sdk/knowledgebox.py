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

import asyncio
import base64
import os
from datetime import datetime
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

from nucliadb_models.common import FieldTypeName
from nucliadb_models.entities import (
    CreateEntitiesGroupPayload,
    EntitiesGroup,
    Entity,
    UpdateEntitiesGroupPayload,
)
from nucliadb_models.labels import KnowledgeBoxLabels
from nucliadb_models.labels import Label as NDBLabel
from nucliadb_models.resource import ExtractedDataTypeName, Resource
from nucliadb_models.search import (
    ChatContextMessage,
    ChatOptions,
    ChatRequest,
    FindRequest,
    KnowledgeboxFindResults,
    KnowledgeboxSearchResults,
    Relations,
    ResourceProperties,
    SearchOptions,
    SearchRequest,
)
from nucliadb_models.text import TextFormat
from nucliadb_models.vectors import VectorSet, VectorSets
from nucliadb_sdk.client import NucliaDBClient
from nucliadb_sdk.entities import Entities
from nucliadb_sdk.file import File
from nucliadb_sdk.find import FindResult
from nucliadb_sdk.labels import DEFAULT_LABELSET, Label, Labels, LabelSet, LabelType
from nucliadb_sdk.resource import (
    build_create_resource_payload,
    build_update_resource_payload,
    from_resource_to_payload,
)
from nucliadb_sdk.search import SearchResult
from nucliadb_sdk.vectors import Vectors, convert_vector

if TYPE_CHECKING:  # pragma: no cover
    from numpy import ndarray
else:
    ndarray = None

NUCLIA_CLOUD = os.environ.get("NUCLIA_CLOUD_URL", ".nuclia.cloud")


class KnowledgeBox:
    vectorsets: Optional[VectorSets] = None
    id: str

    def __init__(self, client: NucliaDBClient):
        self.client = client
        self.id = self.client.reader_session.base_url.path.strip("/").split("/")[-1]

    def __iter__(self) -> Iterable[Resource]:
        page = 0
        while True:
            resources_resp = self.client.list_resources(page)
            for resource in resources_resp.resources:
                yield resource
            if resources_resp.pagination.last:
                return
            page += 1

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

    def new_vectorset(self, key: str, dimension: int, similarity: Optional[str] = None):
        self.client.set_vectorset(
            key, VectorSet(dimension=dimension, similarity=similarity)  # type: ignore
        )

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
        format: Optional[TextFormat] = None,
        labels: Optional[Labels] = None,
        entities: Optional[Entities] = None,
        vectors: Optional[Vectors] = None,
        title: Optional[str] = None,
        summary: Optional[str] = None,
    ):
        """
        Backward compatible method for uploading a resource.

        Use the `create_resource` and `update_resource` methods instead.
        """
        return await asyncio.get_event_loop().run_in_executor(
            None,
            partial(
                self.upload,
                key,
                binary=binary,
                text=text,
                format=format,
                labels=labels,
                entities=entities,
                vectors=vectors,
                title=title,
                summary=summary,
            ),
        )

    def upload(
        self,
        key: Optional[str] = None,
        binary: Optional[Union[File, str]] = None,
        text: Optional[str] = None,
        format: Optional[TextFormat] = None,
        labels: Optional[Labels] = None,
        entities: Optional[Entities] = None,
        vectors: Optional[
            Union[Vectors, Dict[str, Union[ndarray, List[float]]]]
        ] = None,
        title: Optional[str] = None,
        summary: Optional[str] = None,
    ) -> str:
        """
        Backward compatible method for uploading a resource.

        Use the `create_resource` and `update_resource` methods instead.
        """
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
            return self.create_resource(
                key=key,
                binary=binary,
                text=text,
                format=format,
                labels=labels,
                entities=entities,
                vectors=vectors,
                title=title,
                summary=summary,
            )
        else:
            return self.update_resource(
                resource=resource,  # type: ignore
                binary=binary,
                text=text,
                format=format,
                labels=labels,
                entities=entities,
                vectors=vectors,
                title=title,
                summary=summary,
            )

    def create_resource(
        self,
        key: Optional[str] = None,
        binary: Optional[Union[File, str]] = None,
        text: Optional[str] = None,
        format: Optional[TextFormat] = None,
        labels: Optional[Labels] = None,
        entities: Optional[Entities] = None,
        vectors: Optional[
            Union[Vectors, Dict[str, Union[ndarray, List[float]]]]
        ] = None,
        title: Optional[str] = None,
        summary: Optional[str] = None,
    ) -> str:
        if vectors is not None and self.vectorsets is None:
            self.vectorsets = self.client.get_vectorsets()

        create_payload = build_create_resource_payload(
            key=key,
            text=text,
            format=format,
            binary=binary,
            labels=labels,
            entities=entities,
            vectors=vectors,
            vectorsets=self.vectorsets,
            title=title,
            summary=summary,
        )
        resp = self.client.create_resource(create_payload)
        return resp.uuid

    def update_resource(
        self,
        resource: Resource,
        binary: Optional[Union[File, str]] = None,
        text: Optional[str] = None,
        format: Optional[TextFormat] = None,
        labels: Optional[Labels] = None,
        entities: Optional[Entities] = None,
        vectors: Optional[
            Union[Vectors, Dict[str, Union[ndarray, List[float]]]]
        ] = None,
        title: Optional[str] = None,
        summary: Optional[str] = None,
    ) -> str:
        if vectors is not None and self.vectorsets is None:
            self.vectorsets = self.client.get_vectorsets()

        update_payload = build_update_resource_payload(
            resource=resource,
            text=text,
            format=format,
            binary=binary,
            labels=labels,
            entities=entities,
            vectors=vectors,
            vectorsets=self.vectorsets,
            title=title,
            summary=summary,
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

        label_facets = {}
        facet_prefix = "/l/"
        if "/l" in search_result.fulltext.facets:
            label_facets = search_result.fulltext.facets.get("/l", {})
        elif "/classification.labels" in search_result.fulltext.facets:
            facet_prefix = "/classification.labels/"
            label_facets = search_result.fulltext.facets.get(
                "/classification.labels", {}
            )

        for labelset, count in label_facets.items():
            real_labelset = labelset[len(facet_prefix) :]  # removing /l/
            response[real_labelset] = LabelSet(count=count)

        for labelset, labelset_obj in response.items():
            base_label = f"{facet_prefix}{labelset}"
            fsearch_result = self.client.search(
                SearchRequest(features=["document"], faceted=[base_label], page_size=0)  # type: ignore
            )
            if (
                fsearch_result.fulltext is None
                or fsearch_result.fulltext.facets is None
            ):
                raise Exception("Search error")

            for label, count in fsearch_result.fulltext.facets.get(
                base_label, {}
            ).items():
                labelset_obj.labels[label.replace(base_label + "/", "")] = count
        return response

    def get_uploaded_labels(self) -> Dict[str, LabelSet]:
        # Search for fulltext with faceted for labelsets
        search_result = self.client.search(
            SearchRequest(features=["document"], faceted=["/l"], page_size=0)  # type: ignore
        )

        return self.process_uploaded_labels_from_search(search_result)

    async def async_get_uploaded_labels(self) -> Dict[str, LabelSet]:
        # Search for fulltext with faceted for labelsets
        search_result = await self.client.async_search(
            SearchRequest(features=["document"], faceted=["/l"], page_size=0)  # type: ignore
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
        assert resp.status_code == 200, f"{resp.status_code}: {resp.text}"

    def get_labels(self) -> KnowledgeBoxLabels:
        resp = self.client.get_labels()
        return resp

    def set_entities(self, entity_group: str, entities: List[str]):
        # Old set API has been deprecated. Now we only have create and update
        # operations. For bw/ compat, use new endpoints to provide the same
        # functionality

        resp = self.client.reader_session.get(f"/entitiesgroup/{entity_group}")
        assert resp.status_code in (200, 404)

        if resp.status_code == 404:
            payload = CreateEntitiesGroupPayload(
                group=entity_group,
                title=entity_group,
                entities={entity: {"value": entity} for entity in entities},  # type: ignore
            )
            resp = self.client.writer_session.post(
                "/entitiesgroups",
                json=payload.dict(),
            )
            assert resp.status_code == 200

        else:
            current = EntitiesGroup.parse_obj(resp.json())
            update = UpdateEntitiesGroupPayload()
            update.title = entity_group

            for entity in entities:
                if entity in current.entities:
                    update.update[entity] = Entity(value=entity)
                else:
                    update.add[entity] = Entity(value=entity)

            for entity in current.entities:
                if entity not in entities:
                    update.delete.append(entity)

            resp = self.client.writer_session.patch(
                f"/entitiesgroup/{entity_group}",
                json=update.dict(),
            )
            assert resp.status_code == 200

    def search(
        self,
        text: Optional[str] = None,
        filter: Optional[List[Union[Label, str]]] = None,
        vector: Optional[Union[ndarray, List[float]]] = None,
        vectorset: Optional[str] = None,
        min_score: Optional[float] = 0.0,
        page_number: Optional[int] = None,
        page_size: Optional[int] = None,
    ):
        result = self.client.search(
            self.build_search_request(
                text, filter, vector, vectorset, min_score, page_number, page_size
            )
        )
        return SearchResult(result, self.client)

    async def async_search(
        self,
        text: Optional[str] = None,
        filter: Optional[List[Union[Label, str]]] = None,
        vector: Optional[Union[ndarray, List[float]]] = None,
        vectorset: Optional[str] = None,
        min_score: Optional[float] = 0.0,
    ):
        result = await self.client.async_search(
            self.build_search_request(text, filter, vector, vectorset, min_score)
        )
        return SearchResult(result, self.client)

    def find(
        self,
        text: Optional[str] = None,
        filter: Optional[List[Union[Label, str]]] = None,
        vector: Optional[Union[ndarray, List[float]]] = None,
        vectorset: Optional[str] = None,
        min_score: Optional[float] = 0.0,
    ):
        result = self.client.find(
            self.build_find_request(text, filter, vector, vectorset, min_score)
        )
        return FindResult(result)

    async def async_find(
        self,
        text: Optional[str] = None,
        filter: Optional[List[Union[Label, str]]] = None,
        vector: Optional[Union[ndarray, List[float]]] = None,
        vectorset: Optional[str] = None,
        min_score: Optional[float] = 0.0,
    ):
        result = await self.client.async_find(
            self.build_find_request(text, filter, vector, vectorset, min_score)
        )
        return FindResult(result)

    def chat(
        self,
        text: str,
        context: Optional[List[ChatContextMessage]] = None,
        filter: Optional[List[Union[Label, str]]] = None,
    ) -> Tuple[KnowledgeboxFindResults, str, Optional[Relations], Optional[str]]:
        response = self.client.chat(self.build_chat_request(text, filter))
        header = response.raw.read(4, decode_content=True)
        payload_size = int.from_bytes(header, byteorder="big", signed=False)
        data = response.raw.read(payload_size)
        find_result = KnowledgeboxFindResults.parse_raw(base64.b64decode(data))
        data = response.raw.read(decode_content=True)
        answer, relations_payload = data.split(b"_END_")

        learning_id = response.headers.get("NUCLIA-LEARNING-ID")
        relations_result = None
        if len(relations_payload) > 0:
            relations_result = Relations.parse_raw(base64.b64decode(relations_payload))

        return find_result, answer, relations_result, learning_id

    def build_chat_request(
        self,
        text: str,
        filter: Optional[List[Union[Label, str]]] = None,
        min_score: Optional[float] = 0.70,
        show: List[ResourceProperties] = [ResourceProperties.BASIC],
        field_type_filter: List[FieldTypeName] = list(FieldTypeName),
        extracted: List[ExtractedDataTypeName] = list(ExtractedDataTypeName),
        context: Optional[List[ChatContextMessage]] = None,
        fields: Optional[List[str]] = None,
        range_creation_start: Optional[datetime] = None,
        range_creation_end: Optional[datetime] = None,
        range_modification_start: Optional[datetime] = None,
        range_modification_end: Optional[datetime] = None,
    ) -> ChatRequest:
        args: Dict[str, Any] = {
            "features": [ChatOptions.PARAGRAPHS, ChatOptions.RELATIONS]
        }
        args["query"] = text

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

        args["min_score"] = min_score
        args["fields"] = fields or []
        args["context"] = context
        args["extracted"] = extracted
        args["field_type_filter"] = field_type_filter
        args["show"] = show
        args["range_creation_start"] = range_creation_start
        args["range_creation_end"] = range_creation_end
        args["range_modification_start"] = range_modification_start
        args["range_modification_end"] = range_modification_end

        request = ChatRequest(**args)
        return request

    def build_find_request(
        self,
        text: Optional[str] = None,
        filter: Optional[List[Union[Label, str]]] = None,
        vector: Optional[Union[ndarray, List[float]]] = None,
        vectorset: Optional[str] = None,
        min_score: Optional[float] = 0.0,
    ) -> FindRequest:
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
            args["features"].append(SearchOptions.PARAGRAPH)

        if vector is not None and vectorset is not None:
            vector = convert_vector(vector)

            args["vector"] = vector
            args["vectorset"] = vectorset
            args["features"].append(SearchOptions.VECTOR)

        if len(args["features"]) == 0:
            args["features"].append(SearchOptions.PARAGRAPH)

        args["min_score"] = min_score
        request = FindRequest(**args)
        return request

    def build_search_request(
        self,
        text: Optional[str] = None,
        filter: Optional[List[Union[Label, str]]] = None,
        vector: Optional[Union[ndarray, List[float]]] = None,
        vectorset: Optional[str] = None,
        min_score: Optional[float] = 0.0,
        page_number: Optional[int] = None,
        page_size: Optional[int] = None,
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

        if page_number is not None:
            args["page_number"] = page_number
        if page_size is not None:
            args["page_size"] = page_size

        args["min_score"] = min_score
        request = SearchRequest(**args)
        return request
