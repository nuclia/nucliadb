# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import enum
import importlib.metadata
import inspect
import io
import warnings
from dataclasses import dataclass
from json import JSONDecodeError
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterable,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import httpx
import orjson
from nuclia_models.predict.run_agents import RunTextAgentsRequest, RunTextAgentsResponse
from pydantic import BaseModel, ValidationError

from nucliadb_models.conversation import InputMessage
from nucliadb_models.entities import (
    CreateEntitiesGroupPayload,
    EntitiesGroup,
    KnowledgeBoxEntities,
    UpdateEntitiesGroupPayload,
)
from nucliadb_models.export_import import (
    CreateExportResponse,
    CreateImportResponse,
    NewImportedKbResponse,
    StatusResponse,
)
from nucliadb_models.graph.requests import (
    GraphNodesSearchRequest,
    GraphRelationsSearchRequest,
    GraphSearchRequest,
)
from nucliadb_models.graph.responses import (
    GraphNodesSearchResponse,
    GraphRelationsSearchResponse,
    GraphSearchResponse,
)
from nucliadb_models.labels import KnowledgeBoxLabels, LabelSet
from nucliadb_models.resource import (
    KnowledgeBoxConfig,
    KnowledgeBoxList,
    KnowledgeBoxObj,
    Resource,
    ResourceField,
    ResourceList,
)
from nucliadb_models.search import (
    AnswerAskResponseItem,
    AskRequest,
    AskResponseItem,
    AugmentedContext,
    AugmentedContextResponseItem,
    CatalogFacetsRequest,
    CatalogFacetsResponse,
    CatalogRequest,
    CatalogResponse,
    CitationsAskResponseItem,
    DebugAskResponseItem,
    ErrorAskResponseItem,
    FeedbackRequest,
    FindRequest,
    JSONAskResponseItem,
    KnowledgeboxFindResults,
    KnowledgeboxSearchResults,
    MetadataAskResponseItem,
    RelationsAskResponseItem,
    RetrievalAskResponseItem,
    SearchRequest,
    StatusAskResponseItem,
    SummarizedResponse,
    SummarizeRequest,
    SyncAskMetadata,
    SyncAskResponse,
)
from nucliadb_models.synonyms import KnowledgeBoxSynonyms
from nucliadb_models.trainset import TrainSetPartitions
from nucliadb_models.vectorsets import CreatedVectorSet, VectorSetList
from nucliadb_models.writer import (
    CreateResourcePayload,
    ResourceCreated,
    ResourceFieldAdded,
    ResourceUpdated,
    UpdateResourcePayload,
)
from nucliadb_sdk.v2 import exceptions

# Generics
OUTPUT_TYPE = TypeVar("OUTPUT_TYPE", bound=Union[BaseModel, None])

RawRequestContent = Union[str, bytes, Iterable[bytes], AsyncIterable[bytes], dict[str, Any]]

INPUT_TYPE = TypeVar("INPUT_TYPE", BaseModel, List[InputMessage], RawRequestContent, object, None)
USER_AGENT = f"nucliadb-sdk/{importlib.metadata.version('nucliadb_sdk')}"


class Region(enum.Enum):
    EUROPE1 = "europe-1"
    ON_PREM = "on-prem"
    AWS_US_EAST_2_1 = "aws-us-east-2-1"


ASK_STATUS_CODE_ERROR = "-1"


@dataclass
class SdkEndpointDefinition:
    method: str
    path_template: str
    path_params: Tuple[str, ...]


SDK_DEFINITION = {
    # Knowledge Box Endpoints
    "create_knowledge_box": SdkEndpointDefinition(
        path_template="/v1/kbs",
        method="POST",
        path_params=(),
    ),
    "delete_knowledge_box": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}",
        method="DELETE",
        path_params=("kbid",),
    ),
    "get_knowledge_box": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}",
        method="GET",
        path_params=("kbid",),
    ),
    "get_knowledge_box_by_slug": SdkEndpointDefinition(
        path_template="/v1/kb/s/{slug}",
        method="GET",
        path_params=("slug",),
    ),
    "list_knowledge_boxes": SdkEndpointDefinition(
        path_template="/v1/kbs",
        method="GET",
        path_params=(),
    ),
    # Resource Endpoints
    "create_resource": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/resources",
        method="POST",
        path_params=("kbid",),
    ),
    "update_resource": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/resource/{rid}",
        method="PATCH",
        path_params=("kbid", "rid"),
    ),
    "update_resource_by_slug": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/slug/{rslug}",
        method="PATCH",
        path_params=("kbid", "rslug"),
    ),
    "delete_resource": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/resource/{rid}",
        method="DELETE",
        path_params=("kbid", "rid"),
    ),
    "delete_resource_by_slug": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/slug/{rslug}",
        method="DELETE",
        path_params=("kbid", "rslug"),
    ),
    "get_resource_by_slug": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/slug/{slug}",
        method="GET",
        path_params=("kbid", "slug"),
    ),
    "get_resource_by_id": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/resource/{rid}",
        method="GET",
        path_params=("kbid", "rid"),
    ),
    "list_resources": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/resources",
        method="GET",
        path_params=("kbid",),
    ),
    "catalog": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/catalog",
        method="POST",
        path_params=("kbid",),
    ),
    "catalog_facets": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/catalog/facets",
        method="POST",
        path_params=("kbid",),
    ),
    # reindex/reprocess
    "reindex_resource": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/resource/{rid}/reindex",
        method="POST",
        path_params=("kbid", "rid"),
    ),
    "reindex_resource_by_slug": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/slug/{slug}/reindex",
        method="POST",
        path_params=("kbid", "slug"),
    ),
    "reprocess_resource": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/resource/{rid}/reprocess",
        method="POST",
        path_params=("kbid", "rid"),
    ),
    "reprocess_resource_by_slug": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/slug/{slug}/reprocess",
        method="POST",
        path_params=("kbid", "slug"),
    ),
    # Field endpoints
    "delete_field_by_id": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/resource/{rid}/{field_type}/{field_id}",
        method="DELETE",
        path_params=("kbid", "rid", "field_type", "field_id"),
    ),
    # Conversation endpoints
    "add_conversation_message": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/resource/{rid}/conversation/{field_id}/messages",
        method="PUT",
        path_params=("kbid", "rid", "field_id"),
    ),
    "add_conversation_message_by_slug": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/slug/{slug}/conversation/{field_id}/messages",
        method="PUT",
        path_params=("kbid", "slug", "field_id"),
    ),
    "get_resource_field": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/resource/{rid}/{field_type}/{field_id}",
        method="GET",
        path_params=("kbid", "rid", "field_type", "field_id"),
    ),
    "get_resource_field_by_slug": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/slug/{slug}/{field_type}/{field_id}",
        method="GET",
        path_params=("kbid", "slug", "field_type", "field_id"),
    ),
    # Labels
    "set_labelset": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/labelset/{labelset}",
        method="POST",
        path_params=("kbid", "labelset"),
    ),
    "delete_labelset": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/labelset/{labelset}",
        method="DELETE",
        path_params=("kbid", "labelset"),
    ),
    "get_labelsets": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/labelsets",
        method="GET",
        path_params=("kbid",),
    ),
    "get_labelset": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/labelset/{labelset}",
        method="GET",
        path_params=("kbid", "labelset"),
    ),
    # Entity Groups
    "create_entitygroup": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/entitiesgroups",
        method="POST",
        path_params=("kbid",),
    ),
    "update_entitygroup": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/entitiesgroup/{group}",
        method="PATCH",
        path_params=("kbid", "group"),
    ),
    "delete_entitygroup": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/entitiesgroup/{group}",
        method="DELETE",
        path_params=("kbid", "group"),
    ),
    "get_entitygroups": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/entitiesgroups",
        method="GET",
        path_params=("kbid",),
    ),
    "get_entitygroup": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/entitiesgroup/{group}",
        method="GET",
        path_params=("kbid", "group"),
    ),
    # Search / Find Endpoints
    "find": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/find",
        method="POST",
        path_params=("kbid",),
    ),
    "search": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/search",
        method="POST",
        path_params=("kbid",),
    ),
    "ask": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/ask",
        method="POST",
        path_params=("kbid",),
    ),
    "ask_on_resource": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/resource/{rid}/ask",
        method="POST",
        path_params=("kbid", "rid"),
    ),
    "ask_on_resource_by_slug": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/slug/{slug}/ask",
        method="POST",
        path_params=("kbid", "slug"),
    ),
    "graph_path_search": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/graph",
        method="POST",
        path_params=("kbid",),
    ),
    "graph_node_search": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/graph/nodes",
        method="POST",
        path_params=("kbid",),
    ),
    "graph_relation_search": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/graph/relations",
        method="POST",
        path_params=("kbid",),
    ),
    "summarize": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/summarize",
        method="POST",
        path_params=("kbid",),
    ),
    "feedback": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/feedback",
        method="POST",
        path_params=("kbid",),
    ),
    "start_export": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/export",
        method="POST",
        path_params=("kbid",),
    ),
    "export_status": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/export/{export_id}/status",
        method="GET",
        path_params=("kbid", "export_id"),
    ),
    "download_export": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/export/{export_id}",
        method="GET",
        path_params=("kbid", "export_id"),
    ),
    "create_kb_from_import": SdkEndpointDefinition(
        path_template="/v1/kbs/import",
        method="POST",
        path_params=(),
    ),
    "start_import": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/import",
        method="POST",
        path_params=("kbid",),
    ),
    "import_status": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/import/{import_id}/status",
        method="GET",
        path_params=("kbid", "import_id"),
    ),
    "trainset": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/trainset",
        method="GET",
        path_params=("kbid",),
    ),
    # Learning Configuration
    "get_configuration": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/configuration",
        method="GET",
        path_params=("kbid",),
    ),
    "set_configuration": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/configuration",
        method="POST",
        path_params=("kbid",),
    ),
    "update_configuration": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/configuration",
        method="PATCH",
        path_params=("kbid",),
    ),
    # Learning models
    "download_model": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/models/{model_id}/{filename}",
        method="GET",
        path_params=("kbid", "model_id", "filename"),
    ),
    "get_models": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/models",
        method="GET",
        path_params=("kbid",),
    ),
    "get_model": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/model/{model_id}",
        method="GET",
        path_params=("kbid", "model_id"),
    ),
    # Learning config schema
    "get_configuration_schema": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/schema",
        method="GET",
        path_params=("kbid",),
    ),
    # Custom synonyms
    "set_custom_synonyms": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/custom-synonyms",
        method="PUT",
        path_params=("kbid",),
    ),
    "get_custom_synonyms": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/custom-synonyms",
        method="GET",
        path_params=("kbid",),
    ),
    # Vectorsets
    "add_vector_set": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/vectorsets/{vectorset_id}",
        method="POST",
        path_params=("kbid", "vectorset_id"),
    ),
    "delete_vector_set": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/vectorsets/{vectorset_id}",
        method="DELETE",
        path_params=("kbid", "vectorset_id"),
    ),
    "list_vector_sets": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/vectorsets",
        method="GET",
        path_params=("kbid",),
    ),
    # Predict proxy endpoints
    "run_text_agents": SdkEndpointDefinition(
        path_template="/v1/kb/{kbid}/predict/run-agents-text",
        method="POST",
        path_params=("kbid",),
    ),
}


def ask_response_parser(response_type: Type[BaseModel], response: httpx.Response) -> BaseModel:
    content_type = response.headers.get("Content-Type")
    if content_type not in ("application/json", "application/x-ndjson"):
        raise ValueError(f"Unknown content type in response: {content_type}")

    if content_type == "application/json":
        # This comes from a request with the X-Synchronous header set to true
        return response_type.model_validate_json(response.content)

    answer = ""
    answer_json = None
    status = ""
    retrieval_results = None
    relations = None
    learning_id = response.headers.get("NUCLIA-LEARNING-ID")
    citations: dict[str, Any] = {}
    tokens = None
    timings = None
    error: Optional[str] = None
    debug = None
    augmented_context: Optional[AugmentedContext] = None
    for line in response.iter_lines():
        try:
            item = AskResponseItem.model_validate_json(line).item
            if isinstance(item, AnswerAskResponseItem):
                answer += item.text
            elif isinstance(item, JSONAskResponseItem):
                answer_json = item.object
            elif isinstance(item, RelationsAskResponseItem):
                relations = item.relations
            elif isinstance(item, StatusAskResponseItem):
                status = item.status
                if item.code == ASK_STATUS_CODE_ERROR:
                    error = item.details or "Unknown error"
            elif isinstance(item, RetrievalAskResponseItem):
                retrieval_results = item.results
            elif isinstance(item, MetadataAskResponseItem):
                tokens = item.tokens
                timings = item.timings
            elif isinstance(item, CitationsAskResponseItem):
                citations = item.citations
            elif isinstance(item, ErrorAskResponseItem):
                error = item.error
            elif isinstance(item, DebugAskResponseItem):
                debug = item.metadata
            elif isinstance(item, AugmentedContextResponseItem):
                augmented_context = item.augmented
            else:
                warnings.warn(f"Unknown item in ask endpoint response: {item}")
        except ValidationError:
            warnings.warn(f"Unknown line in ask endpoint response: {line}")
            continue

    if error is not None:
        raise exceptions.AskResponseError(error)

    if retrieval_results is None:
        warnings.warn("No retrieval results found in ask response")
        retrieval_results = KnowledgeboxFindResults(resources={})

    return response_type.model_validate(
        {
            "answer": answer,
            "answer_json": answer_json,
            "status": status,
            "retrieval_results": retrieval_results,
            "relations": relations,
            "learning_id": learning_id,
            "citations": citations,
            "debug": debug,
            "metadata": SyncAskMetadata(tokens=tokens, timings=timings),
            "augmented_context": augmented_context,
        }
    )


def _parse_list_of_pydantic(
    data: List[Any],
) -> str:
    output = []
    for item in data:
        if isinstance(item, BaseModel):
            output.append(item.model_dump(by_alias=True, exclude_unset=True))
        else:
            output.append(item)
    return orjson.dumps(output).decode("utf-8")


def is_raw_request_content(content: Any) -> bool:
    return (
        isinstance(content, str)
        or isinstance(content, dict)
        or isinstance(content, bytes)
        or inspect.isgenerator(content)
        or inspect.isasyncgen(content)
        or isinstance(content, io.IOBase)
    )


def prepare_request_base(
    path_template: str,
    path_params: Tuple[str, ...],
    kwargs: Dict[str, Any],
):
    path_data = {}
    for param in path_params:
        if param not in kwargs:
            raise TypeError(f"Missing required parameter {param}")
        path_data[param] = kwargs.pop(param)

    path = path_template.format(**path_data)

    return path


def prepare_request(
    path_template: str,
    path_params: Tuple[str, ...],
    request_type: Optional[Type[INPUT_TYPE]],
    content: Optional[INPUT_TYPE] = None,
    **kwargs,
):
    path = prepare_request_base(path_template, path_params, kwargs)
    data: Optional[RawRequestContent] = None
    if request_type is not None and request_type is not type(None):
        if content is not None:
            if isinstance(content, list):
                data = _parse_list_of_pydantic(content)
            elif not isinstance(content, request_type):
                raise TypeError(f"Expected {request_type}, got {type(content)}")
            elif isinstance(content, BaseModel):
                data = content.model_dump_json(by_alias=True, exclude_unset=True)
            else:
                raise TypeError(f"Unknown type {type(content)}")
        else:
            # pull properties out of kwargs now
            content_data: Dict[str, str] = {}
            if issubclass(request_type, BaseModel):
                for key in list(kwargs.keys()):
                    if key in request_type.model_fields:
                        content_data[key] = kwargs.pop(key)
                data = request_type.model_validate(content_data).model_dump_json(
                    by_alias=True, exclude_unset=True
                )
    elif (
        isinstance(content, str)
        or isinstance(content, dict)
        or isinstance(content, bytes)
        or inspect.isgenerator(content)
        or inspect.isasyncgen(content)
        or isinstance(content, io.IOBase)
    ):
        data = content

    query_params = kwargs.pop("query_params", None)
    if len(kwargs) > 0:
        raise TypeError(f"Invalid arguments provided: {kwargs}")

    return path, data, query_params


def _request_sync_builder(
    name: str,
    request_type: Type[INPUT_TYPE],
    response_type: Type[OUTPUT_TYPE],
):
    """
    SYNC standard Pydantic response builder
    """
    sdk_def = SDK_DEFINITION[name]
    method = sdk_def.method
    path_template = sdk_def.path_template
    path_params = sdk_def.path_params

    def _func(
        self: NucliaDB,
        content: Optional[INPUT_TYPE] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> OUTPUT_TYPE:
        path, data, query_params = prepare_request(
            path_template=path_template,
            path_params=path_params,
            request_type=request_type,
            content=content,
            **kwargs,
        )
        resp = self._request(
            path, method, content=data, query_params=query_params, extra_headers=headers
        )
        if response_type is not None:
            if issubclass(response_type, SyncAskResponse):
                return ask_response_parser(response_type, resp)  # type: ignore
            elif issubclass(response_type, BaseModel):
                return response_type.model_validate_json(resp.content)  # type: ignore
        return None  # type: ignore

    return _func


def _request_json_sync_builder(
    name: str,
):
    """
    SYNC JSON call without payload
    """
    sdk_def = SDK_DEFINITION[name]
    method = sdk_def.method
    path_template = sdk_def.path_template
    path_params = sdk_def.path_params

    def _func(
        self: NucliaDB, content: Optional[Dict[str, Any]] = None, **kwargs
    ) -> Optional[Dict[str, Any]]:
        path = prepare_request_base(
            path_template=path_template,
            path_params=path_params,
            kwargs=kwargs,
        )
        query_params = kwargs.pop("query_params", None)
        resp = self._request(path, method, query_params=query_params, content=content)
        try:
            return orjson.loads(resp.content.decode())
        except orjson.JSONDecodeError:
            if resp.status_code != 204:
                warnings.warn(f"Response content is not valid JSON: {resp.content}")
            return None

    return _func


def _request_iterator_sync_builder(
    name: str,
):
    """
    SYNC Bytes Stream call without payload
    """

    sdk_def = SDK_DEFINITION[name]
    method = sdk_def.method
    path_template = sdk_def.path_template
    path_params = sdk_def.path_params

    def _func(self: NucliaDB, **kwargs) -> Callable[[Optional[int]], Iterator[bytes]]:
        path = prepare_request_base(
            path_template=path_template,
            path_params=path_params,
            kwargs=kwargs,
        )
        query_params = kwargs.pop("query_params", None)
        return self._stream_request(path, method, query_params=query_params)

    return _func


def _request_async_builder(
    name: str,
    request_type: Type[INPUT_TYPE],
    response_type: Type[OUTPUT_TYPE],
):
    """
    ASYNC standard Pydantic response builder
    """

    sdk_def = SDK_DEFINITION[name]
    method = sdk_def.method
    path_template = sdk_def.path_template
    path_params = sdk_def.path_params

    async def _func(
        self: NucliaDBAsync,
        content: Optional[INPUT_TYPE] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> OUTPUT_TYPE:
        path, data, query_params = prepare_request(
            path_template=path_template,
            path_params=path_params,
            request_type=request_type,
            content=content,
            **kwargs,
        )
        resp = await self._request(
            path, method, content=data, query_params=query_params, extra_headers=headers
        )
        if response_type is not None:
            if isinstance(response_type, type) and issubclass(response_type, SyncAskResponse):
                return ask_response_parser(response_type, resp)  # type: ignore
            elif isinstance(response_type, type) and issubclass(response_type, BaseModel):
                return response_type.model_validate_json(resp.content)  # type: ignore
        return None  # type: ignore

    return _func


def _request_json_async_builder(
    name: str,
):
    """
    ASYNC JSON call without payload
    """

    sdk_def = SDK_DEFINITION[name]
    method = sdk_def.method
    path_template = sdk_def.path_template
    path_params = sdk_def.path_params

    async def _func(
        self: NucliaDBAsync, content: Optional[Dict[str, Any]] = None, **kwargs
    ) -> Optional[Dict[str, Any]]:
        path = prepare_request_base(
            path_template=path_template,
            path_params=path_params,
            kwargs=kwargs,
        )
        query_params = kwargs.pop("query_params", None)
        resp = await self._request(path, method, query_params=query_params, content=content)
        try:
            return orjson.loads(resp.content.decode())
        except orjson.JSONDecodeError:
            if resp.status_code != 204:
                warnings.warn(f"Response content is not valid JSON: {resp.content}")
            return None

    return _func


def _request_iterator_async_builder(
    name: str,
):
    """
    SYNC Bytes Stream call without payload
    """

    sdk_def = SDK_DEFINITION[name]
    method = sdk_def.method
    path_template = sdk_def.path_template
    path_params = sdk_def.path_params

    def _func(self: NucliaDBAsync, **kwargs) -> Callable[[Optional[int]], AsyncGenerator[bytes, None]]:
        path = prepare_request_base(
            path_template=path_template,
            path_params=path_params,
            kwargs=kwargs,
        )
        query_params = kwargs.pop("query_params", None)
        return self._stream_request(path, method, query_params=query_params)

    return _func


class _NucliaDBBase:
    sync: bool = True

    def __init__(
        self,
        *,
        region: Union[str, Region] = "europe-1",
        api_key: Optional[str] = None,
        url: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ):
        if isinstance(region, Region):
            warnings.warn(f"Passing Region enum is deprecated. Use the string instead: {region.value}")
            region = str(region.value)
        self.region: str = region
        self.api_key = api_key
        headers = headers or {}
        if self.region == Region.ON_PREM.value:
            if url is None:
                raise ValueError("url must be provided for on-prem")
            self.base_url: str = url.rstrip("/")
            # By default, on prem should utilize all headers available
            # For custom auth schemes, the user will need to provide custom
            # auth headers
            headers["X-NUCLIADB-ROLES"] = "MANAGER;WRITER;READER"
        else:
            if url is None:
                self.base_url = f"https://{self.region}.nuclia.cloud/api"
            else:
                self.base_url = url.rstrip("/")
            if api_key is not None:
                headers["X-STF-SERVICEACCOUNT"] = f"Bearer {api_key}"

        self.headers = {"User-Agent": USER_AGENT, **headers}

    def _request(
        self,
        path,
        method: str,
        query_params: Optional[Dict[str, str]] = None,
        content: Optional[RawRequestContent] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ):
        raise NotImplementedError

    def _stream_request(
        self,
        path,
        method: str,
        data: Optional[Union[str, bytes]] = None,
        query_params: Optional[Dict[str, str]] = None,
    ):
        raise NotImplementedError

    def _check_response(self, response: httpx.Response):
        if response.status_code < 300:
            return response
        elif response.status_code in (401, 403):
            raise exceptions.AuthError(f"Auth error {response.status_code}: {response.text}")
        elif response.status_code == 402:
            raise exceptions.AccountLimitError(
                f"Account limits exceeded error {response.status_code}: {response.text}"
            )
        elif response.status_code == 429:
            try_after: Optional[float] = None
            try:
                body = response.json()
                try_after = body.get("detail", {}).get("try_after")
            except JSONDecodeError:
                pass
            raise exceptions.RateLimitError(response.text, try_after=try_after)
        elif response.status_code in (
            409,
            419,
        ):  # 419 is a custom error code for kb creation conflict
            raise exceptions.ConflictError(response.text)
        elif response.status_code == 404:
            raise exceptions.NotFoundError(f"Resource not found at url {response.url}: {response.text}")
        else:
            raise exceptions.UnknownError(
                f"Unknown error connecting to API: {response.status_code}: {response.text}"
            )


class NucliaDB(_NucliaDBBase):
    """
    Example usage

    >>> from nucliadb_sdk import *
    >>> sdk = NucliaDB(region="europe-1", api_key="api-key")
    >>> sdk.list_resources(kbid='my-kbid')
    """

    def __init__(
        self,
        *,
        region: Union[str, Region] = "europe-1",
        api_key: Optional[str] = None,
        url: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = 60.0 * 5,
    ):
        """
        Create a new instance of the NucliaDB client

        :param region: The region to connect to.
        :type region: nucliadb_sdk.Region
        :param api_key: The API key to use for authentication
        :type api_key: str
        :param url: The base URL to use for the NucliaDB API
        :type url: str
        :param headers: Any additional headers to include in each request
        :type headers: Dict[str, str]
        :param timeout: The timeout in seconds to use for requests
        :type timeout: float

        When connecting to the managed NucliaDB service, you can simply configure the client with your API key:

        >>> from nucliadb_sdk import NucliaDB
        >>> sdk = NucliaDB(api_key="api-key")

        If the Knowledge Box you are interacting with is public, you don't even need the api key:

        >>> from nucliadb_sdk import NucliaDB
        >>> sdk = NucliaDB()

        If you are connecting to a NucliaDB on-prem instance, you will need to specify the URL as follows:

        >>> from nucliadb_sdk import NucliaDB, Region
        >>> sdk = NucliaDB(api_key="api-key", url=\"http://localhost:8080\")
        """  # noqa
        super().__init__(region=region, api_key=api_key, url=url, headers=headers)
        self.session = httpx.Client(headers=self.headers, base_url=self.base_url, timeout=timeout)

    def _request(
        self,
        path,
        method: str,
        query_params: Optional[Dict[str, str]] = None,
        content: Optional[RawRequestContent] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ):
        url = f"{self.base_url}{path}"
        opts: Dict[str, Any] = {}
        if content is not None:
            if isinstance(content, dict):
                content = orjson.dumps(content)
            opts["content"] = content
        if query_params is not None:
            opts["params"] = query_params
        if extra_headers is not None:
            opts["headers"] = extra_headers
        response: httpx.Response = getattr(self.session, method.lower())(url, **opts)
        return self._check_response(response)

    def _stream_request(
        self,
        path,
        method: str,
        data: Optional[Union[str, bytes]] = None,
        query_params: Optional[Dict[str, str]] = None,
    ) -> Callable[[Optional[int]], Iterator[bytes]]:
        url = f"{self.base_url}{path}"
        opts: Dict[str, Any] = {}
        if data is not None:
            opts["data"] = data
        if query_params is not None:
            opts["params"] = query_params

        def iter_bytes(chunk_size=None) -> Iterator[bytes]:
            with self.session.stream(method.lower(), url=url, **opts, timeout=30.0) as response:
                self._check_response(response)
                for chunk in response.iter_raw(chunk_size=chunk_size):
                    yield chunk

        return iter_bytes

    create_knowledge_box = _request_sync_builder(
        "create_knowledge_box", KnowledgeBoxConfig, KnowledgeBoxObj
    )
    delete_knowledge_box = _request_sync_builder("delete_knowledge_box", type(None), KnowledgeBoxObj)
    get_knowledge_box = _request_sync_builder("get_knowledge_box", type(None), KnowledgeBoxObj)
    get_knowledge_box_by_slug = _request_sync_builder(
        "get_knowledge_box_by_slug", type(None), KnowledgeBoxObj
    )
    list_knowledge_boxes = _request_sync_builder("list_knowledge_boxes", type(None), KnowledgeBoxList)
    # Resource Endpoints
    create_resource = _request_sync_builder("create_resource", CreateResourcePayload, ResourceCreated)
    update_resource = _request_sync_builder("update_resource", UpdateResourcePayload, ResourceUpdated)
    update_resource_by_slug = _request_sync_builder(
        "update_resource_by_slug", UpdateResourcePayload, ResourceUpdated
    )
    delete_resource = _request_sync_builder("delete_resource", type(None), type(None))
    delete_resource_by_slug = _request_sync_builder("delete_resource_by_slug", type(None), type(None))
    get_resource_by_slug = _request_sync_builder("get_resource_by_slug", type(None), Resource)
    get_resource_by_id = _request_sync_builder("get_resource_by_id", type(None), Resource)
    list_resources = _request_sync_builder("list_resources", type(None), ResourceList)
    catalog = _request_sync_builder("catalog", CatalogRequest, CatalogResponse)
    catalog_facets = _request_sync_builder("catalog_facets", CatalogFacetsRequest, CatalogFacetsResponse)
    # reindex/reprocess
    reindex_resource = _request_sync_builder("reindex_resource", type(None), type(None))
    reindex_resource_by_slug = _request_sync_builder("reindex_resource_by_slug", type(None), type(None))
    reprocess_resource = _request_sync_builder("reprocess_resource", type(None), type(None))
    reprocess_resource_by_slug = _request_sync_builder(
        "reprocess_resource_by_slug", type(None), type(None)
    )
    # Field endpoints
    delete_field_by_id = _request_sync_builder("delete_field_by_id", type(None), type(None))
    # Conversation endpoints
    add_conversation_message = _request_sync_builder(
        "add_conversation_message", List[InputMessage], ResourceFieldAdded
    )
    add_conversation_message_by_slug = _request_sync_builder(
        "add_conversation_message_by_slug", List[InputMessage], ResourceFieldAdded
    )
    get_resource_field = _request_sync_builder("get_resource_field", type(None), ResourceField)
    get_resource_field_by_slug = _request_sync_builder(
        "get_resource_field_by_slug", type(None), ResourceField
    )
    # Labels
    set_labelset = _request_sync_builder("set_labelset", LabelSet, type(None))
    delete_labelset = _request_sync_builder("delete_labelset", type(None), type(None))
    get_labelsets = _request_sync_builder("get_labelsets", type(None), KnowledgeBoxLabels)
    get_labelset = _request_sync_builder("get_labelset", type(None), LabelSet)
    # Entity Groups
    create_entitygroup = _request_sync_builder(
        "create_entitygroup", CreateEntitiesGroupPayload, type(None)
    )
    update_entitygroup = _request_sync_builder(
        "update_entitygroup", UpdateEntitiesGroupPayload, type(None)
    )
    delete_entitygroup = _request_sync_builder("delete_entitygroup", type(None), type(None))
    get_entitygroups = _request_sync_builder("get_entitygroups", type(None), KnowledgeBoxEntities)
    get_entitygroup = _request_sync_builder("get_entitygroup", type(None), EntitiesGroup)
    # Search / Find Endpoints
    find = _request_sync_builder("find", FindRequest, KnowledgeboxFindResults)
    search = _request_sync_builder("search", SearchRequest, KnowledgeboxSearchResults)
    ask = _request_sync_builder("ask", AskRequest, SyncAskResponse)
    ask_on_resource = _request_sync_builder("ask_on_resource", AskRequest, SyncAskResponse)
    ask_on_resource_by_slug = _request_sync_builder(
        "ask_on_resource_by_slug", AskRequest, SyncAskResponse
    )
    graph_search = _request_sync_builder("graph_path_search", GraphSearchRequest, GraphSearchResponse)
    graph_nodes = _request_sync_builder(
        "graph_node_search", GraphNodesSearchRequest, GraphNodesSearchResponse
    )
    graph_relations = _request_sync_builder(
        "graph_relation_search", GraphRelationsSearchRequest, GraphRelationsSearchResponse
    )

    summarize = _request_sync_builder("summarize", SummarizeRequest, SummarizedResponse)
    feedback = _request_sync_builder("feedback", FeedbackRequest, type(None))
    start_export = _request_sync_builder("start_export", type(None), CreateExportResponse)
    export_status = _request_sync_builder("export_status", type(None), StatusResponse)
    download_export = _request_iterator_sync_builder("download_export")
    create_kb_from_import = _request_sync_builder(
        "create_kb_from_import", type(None), NewImportedKbResponse
    )
    start_import = _request_sync_builder("start_import", type(None), CreateImportResponse)
    import_status = _request_sync_builder("import_status", type(None), StatusResponse)
    trainset = _request_sync_builder("trainset", type(None), TrainSetPartitions)
    # Learning Configuration
    get_configuration = _request_json_sync_builder("get_configuration")
    set_configuration = _request_json_sync_builder("set_configuration")
    update_configuration = _request_json_sync_builder("update_configuration")

    # Learning models
    download_model = _request_iterator_sync_builder("download_model")
    get_models = _request_json_sync_builder("get_models")
    get_model = _request_json_sync_builder("get_model")

    # Learning config schema
    get_configuration_schema = _request_json_sync_builder("get_configuration_schema")
    # Custom synonyms
    set_custom_synonyms = _request_sync_builder("set_custom_synonyms", KnowledgeBoxSynonyms, type(None))
    get_custom_synonyms = _request_sync_builder("get_custom_synonyms", type(None), KnowledgeBoxSynonyms)

    # Vectorsets
    add_vector_set = _request_sync_builder("add_vector_set", type(None), CreatedVectorSet)
    delete_vector_set = _request_sync_builder("delete_vector_set", type(None), type(None))
    list_vector_sets = _request_sync_builder("list_vector_sets", type(None), VectorSetList)

    # Predict proxy endpoints
    run_text_agents = _request_sync_builder(
        "run_text_agents", RunTextAgentsRequest, RunTextAgentsResponse
    )


class NucliaDBAsync(_NucliaDBBase):
    """
    Example usage

    >>> from nucliadb_sdk import *
    >>> sdk = NucliaDBAsync(region="europe-1", api_key="api-key")
    >>> await sdk.list_resources(kbid='my-kbid')
    """

    def __init__(
        self,
        *,
        region: Union[str, Region] = "europe-1",
        api_key: Optional[str] = None,
        url: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = 60.0,
    ):
        """
        Create a new instance of the NucliaDB client

        :param region: The region to connect to.
        :type region: nucliadb_sdk.Region
        :param api_key: The API key to use for authentication
        :type api_key: str
        :param url: The base URL to use for the NucliaDB API
        :type url: str
        :param headers: Any additional headers to include in each request
        :type headers: Dict[str, str]
        :param timeout: The timeout in seconds to use for requests
        :type timeout: float

        When connecting to the managed NucliaDB cloud service, you can simply configure the SDK with your API key

        >>> from nucliadb_sdk import *
        >>> sdk = NucliaDBAsync(api_key="api-key", region="aws-us-east-2-1")

        If the Knowledge Box you are interacting with is public, you don't even need the api key

        >>> sdk = NucliaDBAsync(region="europe-1")

        If you are connecting to a NucliaDB on-prem instance, you will need to specify the URL

        >>> sdk = NucliaDBAsync(api_key="api-key", url="https://mycompany.api.com/api/nucliadb")
        """  # noqa
        super().__init__(region=region, api_key=api_key, url=url, headers=headers)
        self.session = httpx.AsyncClient(headers=self.headers, base_url=self.base_url, timeout=timeout)

    async def _request(
        self,
        path,
        method: str,
        query_params: Optional[Dict[str, str]] = None,
        content: Optional[RawRequestContent] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ):
        url = f"{self.base_url}{path}"
        opts: Dict[str, Any] = {}
        if content is not None:
            if isinstance(content, dict):
                content = orjson.dumps(content)
            opts["content"] = content
        if query_params is not None:
            opts["params"] = query_params
        if extra_headers is not None:
            opts["headers"] = extra_headers
        response: httpx.Response = await getattr(self.session, method.lower())(url, **opts)
        return self._check_response(response)

    def _stream_request(
        self,
        path,
        method: str,
        data: Optional[Union[str, bytes]] = None,
        query_params: Optional[Dict[str, str]] = None,
    ) -> Callable[[Optional[int]], AsyncGenerator[bytes, None]]:
        url = f"{self.base_url}{path}"
        opts: Dict[str, Any] = {}
        if data is not None:
            opts["data"] = data
        if query_params is not None:
            opts["params"] = query_params

        async def iter_bytes(chunk_size=None) -> AsyncGenerator[bytes, None]:
            async with self.session.stream(method.lower(), url=url, **opts) as response:
                self._check_response(response)
                async for chunk in response.aiter_raw(chunk_size=chunk_size):
                    yield chunk

        return iter_bytes

    create_knowledge_box = _request_async_builder(
        "create_knowledge_box", KnowledgeBoxConfig, KnowledgeBoxObj
    )
    delete_knowledge_box = _request_async_builder("delete_knowledge_box", type(None), KnowledgeBoxObj)
    get_knowledge_box = _request_async_builder("get_knowledge_box", type(None), KnowledgeBoxObj)
    get_knowledge_box_by_slug = _request_async_builder(
        "get_knowledge_box_by_slug", type(None), KnowledgeBoxObj
    )
    list_knowledge_boxes = _request_async_builder("list_knowledge_boxes", type(None), KnowledgeBoxList)
    # Resource Endpoints
    create_resource = _request_async_builder("create_resource", CreateResourcePayload, ResourceCreated)
    update_resource = _request_async_builder("update_resource", UpdateResourcePayload, ResourceUpdated)
    update_resource_by_slug = _request_async_builder(
        "update_resource_by_slug", UpdateResourcePayload, ResourceUpdated
    )
    delete_resource = _request_async_builder("delete_resource", type(None), type(None))
    delete_resource_by_slug = _request_async_builder("delete_resource_by_slug", type(None), type(None))
    get_resource_by_slug = _request_async_builder("get_resource_by_slug", type(None), Resource)
    get_resource_by_id = _request_async_builder("get_resource_by_id", type(None), Resource)
    list_resources = _request_async_builder("list_resources", type(None), ResourceList)
    catalog = _request_async_builder("catalog", CatalogRequest, CatalogResponse)
    catalog_facets = _request_async_builder(
        "catalog_facets", CatalogFacetsRequest, CatalogFacetsResponse
    )
    # reindex/reprocess
    reindex_resource = _request_async_builder("reindex_resource", type(None), type(None))
    reindex_resource_by_slug = _request_async_builder("reindex_resource_by_slug", type(None), type(None))
    reprocess_resource = _request_async_builder("reprocess_resource", type(None), type(None))
    reprocess_resource_by_slug = _request_async_builder(
        "reprocess_resource_by_slug", type(None), type(None)
    )
    # Field endpoints
    delete_field_by_id = _request_async_builder("delete_field_by_id", type(None), type(None))
    # Conversation endpoints
    add_conversation_message = _request_async_builder(
        "add_conversation_message", List[InputMessage], ResourceFieldAdded
    )
    add_conversation_message_by_slug = _request_async_builder(
        "add_conversation_message_by_slug", List[InputMessage], ResourceFieldAdded
    )
    get_resource_field = _request_async_builder("get_resource_field", type(None), ResourceField)
    get_resource_field_by_slug = _request_async_builder(
        "get_resource_field_by_slug", type(None), ResourceField
    )
    # Labels
    set_labelset = _request_async_builder("set_labelset", LabelSet, type(None))
    delete_labelset = _request_async_builder("delete_labelset", type(None), type(None))
    get_labelsets = _request_async_builder("get_labelsets", type(None), KnowledgeBoxLabels)
    get_labelset = _request_async_builder("get_labelset", type(None), LabelSet)
    # Entity Groups
    create_entitygroup = _request_async_builder(
        "create_entitygroup", CreateEntitiesGroupPayload, type(None)
    )
    update_entitygroup = _request_async_builder(
        "update_entitygroup", UpdateEntitiesGroupPayload, type(None)
    )
    delete_entitygroup = _request_async_builder("delete_entitygroup", type(None), type(None))
    get_entitygroups = _request_async_builder("get_entitygroups", type(None), KnowledgeBoxEntities)
    get_entitygroup = _request_async_builder("get_entitygroup", type(None), EntitiesGroup)
    # Search / Find Endpoints
    find = _request_async_builder("find", FindRequest, KnowledgeboxFindResults)
    search = _request_async_builder("search", SearchRequest, KnowledgeboxSearchResults)
    ask = _request_async_builder("ask", AskRequest, SyncAskResponse)
    ask_on_resource = _request_async_builder("ask_on_resource", AskRequest, SyncAskResponse)
    ask_on_resource_by_slug = _request_async_builder(
        "ask_on_resource_by_slug", AskRequest, SyncAskResponse
    )
    graph_search = _request_async_builder("graph_path_search", GraphSearchRequest, GraphSearchResponse)
    graph_nodes = _request_async_builder(
        "graph_node_search", GraphNodesSearchRequest, GraphNodesSearchResponse
    )
    graph_relations = _request_async_builder(
        "graph_relation_search", GraphRelationsSearchRequest, GraphRelationsSearchResponse
    )

    summarize = _request_async_builder("summarize", SummarizeRequest, SummarizedResponse)
    feedback = _request_async_builder("feedback", FeedbackRequest, type(None))
    start_export = _request_async_builder("start_export", type(None), CreateExportResponse)
    export_status = _request_async_builder("export_status", type(None), StatusResponse)
    download_export = _request_iterator_async_builder("download_export")
    create_kb_from_import = _request_async_builder(
        "create_kb_from_import", type(None), NewImportedKbResponse
    )
    start_import = _request_async_builder("start_import", type(None), CreateImportResponse)
    import_status = _request_async_builder("import_status", type(None), StatusResponse)
    trainset = _request_async_builder("trainset", type(None), TrainSetPartitions)
    # Learning Configuration
    get_configuration = _request_json_async_builder("get_configuration")
    set_configuration = _request_json_async_builder("set_configuration")
    update_configuration = _request_json_async_builder("update_configuration")

    # Learning models
    download_model = _request_iterator_async_builder("download_model")
    get_models = _request_json_async_builder("get_models")
    get_model = _request_json_async_builder("get_model")

    # Learning config schema
    get_configuration_schema = _request_json_async_builder("get_configuration_schema")
    # Custom synonyms
    set_custom_synonyms = _request_async_builder("set_custom_synonyms", KnowledgeBoxSynonyms, type(None))
    get_custom_synonyms = _request_async_builder("get_custom_synonyms", type(None), KnowledgeBoxSynonyms)

    # Vectorsets
    add_vector_set = _request_async_builder("add_vector_set", type(None), CreatedVectorSet)
    delete_vector_set = _request_async_builder("delete_vector_set", type(None), type(None))
    list_vector_sets = _request_async_builder("list_vector_sets", type(None), VectorSetList)

    # Predict proxy endpoints
    run_text_agents = _request_async_builder(
        "run_text_agents", RunTextAgentsRequest, RunTextAgentsResponse
    )
