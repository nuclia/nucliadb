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
import enum
import inspect
import io
import warnings
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
    Union,
)

import httpx
import orjson
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
    StatusResponse,
)
from nucliadb_models.labels import KnowledgeBoxLabels, LabelSet
from nucliadb_models.resource import (
    KnowledgeBoxConfig,
    KnowledgeBoxList,
    KnowledgeBoxObj,
    Resource,
    ResourceList,
)
from nucliadb_models.search import (
    AnswerAskResponseItem,
    AskRequest,
    AskResponseItem,
    ChatRequest,
    CitationsAskResponseItem,
    ErrorAskResponseItem,
    FeedbackRequest,
    FindRequest,
    JSONAskResponseItem,
    KnowledgeboxFindResults,
    KnowledgeboxSearchResults,
    MetadataAskResponseItem,
    Relations,
    RelationsAskResponseItem,
    RetrievalAskResponseItem,
    SearchRequest,
    StatusAskResponseItem,
    SummarizedResponse,
    SummarizeRequest,
    SyncAskMetadata,
    SyncAskResponse,
)
from nucliadb_models.trainset import TrainSetPartitions
from nucliadb_models.writer import (
    CreateResourcePayload,
    ResourceCreated,
    ResourceFieldAdded,
    ResourceUpdated,
    UpdateResourcePayload,
)
from nucliadb_sdk.v2 import exceptions


class Region(enum.Enum):
    EUROPE1 = "europe-1"
    ON_PREM = "on-prem"
    AWS_US_EAST_2_1 = "aws-us-east-2-1"


class ChatResponse(BaseModel):
    result: KnowledgeboxFindResults
    answer: str
    relations: Optional[Relations] = None
    learning_id: Optional[str] = None
    citations: dict[str, Any] = {}


RawRequestContent = Union[str, bytes, Iterable[bytes], AsyncIterable[bytes]]


ASK_STATUS_CODE_ERROR = "-1"


def json_response_parser(response: httpx.Response) -> Any:
    return orjson.loads(response.content.decode())


def chat_response_parser(response: httpx.Response) -> ChatResponse:
    raw = io.BytesIO(response.content)
    header = raw.read(4)
    payload_size = int.from_bytes(header, byteorder="big", signed=False)
    data = raw.read(payload_size)
    find_result = KnowledgeboxFindResults.model_validate_json(base64.b64decode(data))
    data = raw.read()
    try:
        answer, relations_payload = data.split(b"_END_")
    except ValueError:
        answer = data
        relations_payload = b""
    learning_id = response.headers.get("NUCLIA-LEARNING-ID")
    relations_result = None
    if len(relations_payload) > 0:
        relations_result = Relations.model_validate_json(base64.b64decode(relations_payload))
    try:
        answer, tail = answer.split(b"_CIT_")
        citations_length = int.from_bytes(tail[:4], byteorder="big", signed=False)
        citations_bytes = tail[4 : 4 + citations_length]
        citations = orjson.loads(base64.b64decode(citations_bytes).decode())
    except ValueError:
        citations = {}
    return ChatResponse(
        result=find_result,
        answer=answer.decode("utf-8"),
        relations=relations_result,
        learning_id=learning_id,
        citations=citations,
    )


def ask_response_parser(response: httpx.Response) -> SyncAskResponse:
    content_type = response.headers.get("Content-Type")
    if content_type not in ("application/json", "application/x-ndjson"):
        raise ValueError(f"Unknown content type in response: {content_type}")

    if content_type == "application/json":
        # This comes from a request with the X-Synchronous header set to true
        return SyncAskResponse.model_validate_json(response.content)

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

    return SyncAskResponse(
        answer=answer,
        answer_json=answer_json,
        status=status,
        retrieval_results=retrieval_results,
        relations=relations,
        learning_id=learning_id,
        citations=citations,
        metadata=SyncAskMetadata(tokens=tokens, timings=timings),
    )


def _parse_list_of_pydantic(
    data: List[Any],
) -> str:
    output = []
    for item in data:
        if isinstance(item, BaseModel):
            output.append(item.model_dump())
        else:
            output.append(item)
    return orjson.dumps(output).decode("utf-8")


def _parse_response(response_type, resp: httpx.Response) -> Any:
    if response_type is not None:
        if isinstance(response_type, type) and issubclass(response_type, BaseModel):
            return response_type.model_validate_json(resp.content)
        else:
            return response_type(resp)
    else:
        return resp.content


def is_raw_request_content(content: Any) -> bool:
    return (
        isinstance(content, str)
        or isinstance(content, dict)
        or isinstance(content, bytes)
        or inspect.isgenerator(content)
        or inspect.isasyncgen(content)
        or isinstance(content, io.IOBase)
    )


def _request_builder(
    *,
    name: str,
    method: str,
    path_template: str,
    path_params: Tuple[str, ...],
    request_type: Optional[Union[Type[BaseModel], List[Any]]],
    response_type: Optional[
        Union[
            Type[BaseModel],
            Callable[[httpx.Response], BaseModel],
            Callable[[httpx.Response], Iterator[bytes]],
        ]
    ],
    stream_response: bool = False,
):
    def _func(self: "NucliaDB | NucliaDBAsync", content: Optional[Any] = None, **kwargs):
        path_data = {}
        for param in path_params:
            if param not in kwargs:
                raise TypeError(f"Missing required parameter {param}")
            path_data[param] = kwargs.pop(param)

        path = path_template.format(**path_data)
        data = None
        raw_content: Optional[RawRequestContent] = None
        if request_type is not None:
            if content is not None:
                try:
                    if not isinstance(content, request_type):  # type: ignore
                        raise TypeError(f"Expected {request_type}, got {type(content)}")
                    else:
                        data = content.json(by_alias=True)
                except TypeError:
                    if not isinstance(content, list):
                        raise
                    data = _parse_list_of_pydantic(content)
            else:
                # pull properties out of kwargs now
                content_data = {}
                for key in list(kwargs.keys()):
                    if key in request_type.model_fields:  # type: ignore
                        content_data[key] = kwargs.pop(key)
                data = request_type.model_validate(content_data).json(by_alias=True)  # type: ignore
        elif is_raw_request_content(content):
            raw_content = content

        query_params = kwargs.pop("query_params", None)
        if len(kwargs) > 0:
            raise TypeError(f"Invalid arguments provided: {kwargs}")

        if not stream_response:
            resp = self._request(path, method, data=data, query_params=query_params, content=raw_content)
            if asyncio.iscoroutine(resp):

                async def _wrapped_resp():
                    real_resp = await resp
                    return _parse_response(response_type, real_resp)

                return _wrapped_resp()
            else:
                return _parse_response(response_type, resp)  # type: ignore
        else:
            resp = self._stream_request(path, method, data=data, query_params=query_params)
            return resp

    return _func


class _NucliaDBBase:
    def __init__(
        self,
        *,
        region: Region = Region.EUROPE1,
        api_key: Optional[str] = None,
        url: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ):
        try:
            self.region: str = Region(region).value
        except ValueError:
            warnings.warn(
                f"Unknown region '{region}'. Supported regions are: {[r.value for r in Region]}"
            )
            self.region = region  # type: ignore
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

        self.headers = headers

    def _request(
        self,
        path,
        method: str,
        data: Optional[Union[str, bytes]] = None,
        query_params: Optional[Dict[str, str]] = None,
        content: Optional[RawRequestContent] = None,
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
            raise exceptions.RateLimitError(response.text)
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

    # Knowledge Box Endpoints
    create_knowledge_box = _request_builder(
        name="create_knowledge_box",
        path_template="/v1/kbs",
        method="POST",
        path_params=(),
        request_type=KnowledgeBoxConfig,
        response_type=KnowledgeBoxObj,
    )
    delete_knowledge_box = _request_builder(
        name="delete_knowledge_box",
        path_template="/v1/kb/{kbid}",
        method="DELETE",
        path_params=("kbid",),
        request_type=None,
        response_type=KnowledgeBoxObj,
    )
    get_knowledge_box = _request_builder(
        name="get_knowledge_box",
        path_template="/v1/kb/{kbid}",
        method="GET",
        path_params=("kbid",),
        request_type=None,
        response_type=KnowledgeBoxObj,
    )
    get_knowledge_box_by_slug = _request_builder(
        name="get_knowledge_box_by_slug",
        path_template="/v1/kb/s/{slug}",
        method="GET",
        path_params=("slug",),
        request_type=None,
        response_type=KnowledgeBoxObj,
    )
    list_knowledge_boxes = _request_builder(
        name="list_knowledge_boxes",
        path_template="/v1/kbs",
        method="GET",
        path_params=(),
        request_type=None,
        response_type=KnowledgeBoxList,
    )

    # Resource Endpoints
    create_resource = _request_builder(
        name="create_resource",
        path_template="/v1/kb/{kbid}/resources",
        method="POST",
        path_params=("kbid",),
        request_type=CreateResourcePayload,
        response_type=ResourceCreated,
    )
    update_resource = _request_builder(
        name="update_resource",
        path_template="/v1/kb/{kbid}/resource/{rid}",
        method="PATCH",
        path_params=("kbid", "rid"),
        request_type=UpdateResourcePayload,
        response_type=ResourceUpdated,
    )
    update_resource_by_slug = _request_builder(
        name="update_resource_by_slug",
        path_template="/v1/kb/{kbid}/slug/{rslug}",
        method="PATCH",
        path_params=("kbid", "rslug"),
        request_type=UpdateResourcePayload,
        response_type=ResourceUpdated,
    )
    delete_resource = _request_builder(
        name="delete_resource",
        path_template="/v1/kb/{kbid}/resource/{rid}",
        method="DELETE",
        path_params=("kbid", "rid"),
        request_type=None,
        response_type=None,
    )
    delete_resource_by_slug = _request_builder(
        name="delete_resource_by_slug",
        path_template="/v1/kb/{kbid}/slug/{rslug}",
        method="DELETE",
        path_params=("kbid", "rslug"),
        request_type=None,
        response_type=None,
    )
    get_resource_by_slug = _request_builder(
        name="get_resource_by_slug",
        path_template="/v1/kb/{kbid}/slug/{slug}",
        method="GET",
        path_params=("kbid", "slug"),
        request_type=None,
        response_type=Resource,
    )
    get_resource_by_id = _request_builder(
        name="get_resource_by_id",
        path_template="/v1/kb/{kbid}/resource/{rid}",
        method="GET",
        path_params=("kbid", "rid"),
        request_type=None,
        response_type=Resource,
    )
    list_resources = _request_builder(
        name="list_resources",
        path_template="/v1/kb/{kbid}/resources",
        method="GET",
        path_params=("kbid",),
        request_type=None,
        response_type=ResourceList,
    )

    # reindex/reprocess
    reindex_resource = _request_builder(
        name="reindex_resource",
        path_template="/v1/kb/{kbid}/resource/{rid}/reindex",
        method="POST",
        path_params=("kbid", "rid"),
        request_type=None,
        response_type=None,
    )
    reindex_resource_by_slug = _request_builder(
        name="reindex_resource_by_slug",
        path_template="/v1/kb/{kbid}/slug/{slug}/reindex",
        method="POST",
        path_params=("kbid", "slug"),
        request_type=None,
        response_type=None,
    )
    reprocess_resource = _request_builder(
        name="reprocess_resource",
        path_template="/v1/kb/{kbid}/resource/{rid}/reprocess",
        method="POST",
        path_params=("kbid", "rid"),
        request_type=None,
        response_type=None,
    )
    reprocess_resource_by_slug = _request_builder(
        name="reprocess_resource_by_slug",
        path_template="/v1/kb/{kbid}/slug/{slug}/reprocess",
        method="POST",
        path_params=("kbid", "slug"),
        request_type=None,
        response_type=None,
    )

    # Conversation endpoints
    add_conversation_message = _request_builder(
        name="add_conversation_message",
        path_template="/v1/kb/{kbid}/resource/{rid}/conversation/{field_id}/messages",
        method="PUT",
        path_params=("kbid", "rid", "field_id"),
        request_type=List[InputMessage],  # type: ignore
        response_type=ResourceFieldAdded,
    )

    # Labels
    set_labelset = _request_builder(
        name="set_labelset",
        path_template="/v1/kb/{kbid}/labelset/{labelset}",
        method="POST",
        path_params=("kbid", "labelset"),
        request_type=LabelSet,
        response_type=None,
    )
    delete_labelset = _request_builder(
        name="delete_labelset",
        path_template="/v1/kb/{kbid}/labelset/{labelset}",
        method="DELETE",
        path_params=("kbid", "labelset"),
        request_type=None,
        response_type=None,
    )
    get_labelsets = _request_builder(
        name="get_labelsets",
        path_template="/v1/kb/{kbid}/labelsets",
        method="GET",
        path_params=("kbid",),
        request_type=None,
        response_type=KnowledgeBoxLabels,
    )
    get_labelset = _request_builder(
        name="get_labelset",
        path_template="/v1/kb/{kbid}/labelset/{labelset}",
        method="GET",
        path_params=("kbid", "labelset"),
        request_type=None,
        response_type=LabelSet,
    )

    # Entity Groups
    create_entitygroup = _request_builder(
        name="create_entitygroup",
        path_template="/v1/kb/{kbid}/entitiesgroups",
        method="POST",
        path_params=("kbid",),
        request_type=CreateEntitiesGroupPayload,
        response_type=None,
    )
    update_entitygroup = _request_builder(
        name="update_entitygroup",
        path_template="/v1/kb/{kbid}/entitiesgroup/{group}",
        method="PATCH",
        path_params=("kbid", "group"),
        request_type=UpdateEntitiesGroupPayload,
        response_type=None,
    )
    delete_entitygroup = _request_builder(
        name="delete_entitygroup",
        path_template="/v1/kb/{kbid}/entitiesgroup/{group}",
        method="DELETE",
        path_params=("kbid", "group"),
        request_type=None,
        response_type=None,
    )
    get_entitygroups = _request_builder(
        name="get_entitygroups",
        path_template="/v1/kb/{kbid}/entitiesgroups",
        method="GET",
        path_params=("kbid",),
        request_type=None,
        response_type=KnowledgeBoxEntities,
    )
    get_entitygroup = _request_builder(
        name="get_entitygroup",
        path_template="/v1/kb/{kbid}/entitiesgroup/{group}",
        method="GET",
        path_params=("kbid", "group"),
        request_type=None,
        response_type=EntitiesGroup,
    )

    # Search / Find Endpoints
    find = _request_builder(
        name="find",
        path_template="/v1/kb/{kbid}/find",
        method="POST",
        path_params=("kbid",),
        request_type=FindRequest,
        response_type=KnowledgeboxFindResults,
    )
    search = _request_builder(
        name="search",
        path_template="/v1/kb/{kbid}/search",
        method="POST",
        path_params=("kbid",),
        request_type=SearchRequest,
        response_type=KnowledgeboxSearchResults,
    )
    chat = _request_builder(
        name="chat",
        path_template="/v1/kb/{kbid}/chat",
        method="POST",
        path_params=("kbid",),
        request_type=ChatRequest,
        response_type=chat_response_parser,
    )

    ask = _request_builder(
        name="ask",
        path_template="/v1/kb/{kbid}/ask",
        method="POST",
        path_params=("kbid",),
        request_type=AskRequest,
        response_type=ask_response_parser,
    )

    chat_on_resource = _request_builder(
        name="chat_on_resource",
        path_template="/v1/kb/{kbid}/resource/{rid}/chat",
        method="POST",
        path_params=("kbid", "rid"),
        request_type=ChatRequest,
        response_type=chat_response_parser,
    )

    chat_on_resource_by_slug = _request_builder(
        name="chat_on_resource_by_slug",
        path_template="/v1/kb/{kbid}/slug/{slug}/chat",
        method="POST",
        path_params=("kbid", "slug"),
        request_type=ChatRequest,
        response_type=chat_response_parser,
    )

    ask_on_resource = _request_builder(
        name="ask_on_resource",
        path_template="/v1/kb/{kbid}/resource/{rid}/ask",
        method="POST",
        path_params=("kbid", "rid"),
        request_type=AskRequest,
        response_type=ask_response_parser,
    )

    ask_on_resource_by_slug = _request_builder(
        name="ask_on_resource_by_slug",
        path_template="/v1/kb/{kbid}/slug/{slug}/ask",
        method="POST",
        path_params=("kbid", "slug"),
        request_type=AskRequest,
        response_type=ask_response_parser,
    )

    summarize = _request_builder(
        name="summarize",
        path_template="/v1/kb/{kbid}/summarize",
        method="POST",
        path_params=("kbid",),
        request_type=SummarizeRequest,
        response_type=SummarizedResponse,
    )

    feedback = _request_builder(
        name="feedback",
        path_template="/v1/kb/{kbid}/feedback",
        method="POST",
        path_params=("kbid",),
        request_type=FeedbackRequest,
        response_type=None,
    )

    start_export = _request_builder(
        name="start_export",
        path_template="/v1/kb/{kbid}/export",
        method="POST",
        path_params=("kbid",),
        request_type=None,
        response_type=CreateExportResponse,
    )

    export_status = _request_builder(
        name="export_status",
        path_template="/v1/kb/{kbid}/export/{export_id}/status",
        method="GET",
        path_params=("kbid", "export_id"),
        request_type=None,
        response_type=StatusResponse,
    )

    download_export = _request_builder(
        name="download_export",
        path_template="/v1/kb/{kbid}/export/{export_id}",
        method="GET",
        path_params=("kbid", "export_id"),
        request_type=None,
        response_type=None,
        stream_response=True,
    )

    start_import = _request_builder(
        name="start_import",
        path_template="/v1/kb/{kbid}/import",
        method="POST",
        path_params=("kbid",),
        request_type=None,
        response_type=CreateImportResponse,
    )

    import_status = _request_builder(
        name="import_status",
        path_template="/v1/kb/{kbid}/import/{import_id}/status",
        method="GET",
        path_params=("kbid", "import_id"),
        request_type=None,
        response_type=StatusResponse,
    )

    trainset = _request_builder(
        name="trainset",
        path_template="/v1/kb/{kbid}/trainset",
        method="GET",
        path_params=("kbid",),
        request_type=None,
        response_type=TrainSetPartitions,
    )

    # Learning Configuration
    get_configuration = _request_builder(
        name="get_configuration",
        path_template="/v1/kb/{kbid}/configuration",
        method="GET",
        path_params=("kbid",),
        request_type=None,
        response_type=json_response_parser,
    )
    set_configuration = _request_builder(
        name="set_configuration",
        path_template="/v1/kb/{kbid}/configuration",
        method="POST",
        path_params=("kbid",),
        request_type=None,
        response_type=None,
    )

    # Learning models
    download_model = _request_builder(
        name="download_model",
        path_template="/v1/kb/{kbid}/models/{model_id}/{filename}",
        method="GET",
        path_params=("kbid", "model_id", "filename"),
        stream_response=True,
        request_type=None,
        response_type=None,
    )

    get_models = _request_builder(
        name="get_models",
        path_template="/v1/kb/{kbid}/models",
        method="GET",
        path_params=("kbid",),
        request_type=None,
        response_type=json_response_parser,
    )

    get_model = _request_builder(
        name="get_model",
        path_template="/v1/kb/{kbid}/model/{model_id}",
        method="GET",
        path_params=("kbid", "model_id"),
        request_type=None,
        response_type=json_response_parser,
    )

    # Learning config schema
    get_configuration_schema = _request_builder(
        name="get_configuration_schema",
        path_template="/v1/kb/{kbid}/schema",
        method="GET",
        path_params=("kbid",),
        request_type=None,
        response_type=json_response_parser,
    )


class NucliaDB(_NucliaDBBase):
    """
    Example usage

    >>> from nucliadb_sdk import *
    >>> sdk = NucliaDB(region=Region.EUROPE1, api_key="api-key")
    >>> sdk.list_resources(kbid='my-kbid')
    """

    def __init__(
        self,
        *,
        region: Region = Region.EUROPE1,
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
        >>> sdk = NucliaDB(api_key="api-key", region=Region.ON_PREM, url=\"http://localhost:8080\")
        """  # noqa
        super().__init__(region=region, api_key=api_key, url=url, headers=headers)
        self.session = httpx.Client(headers=self.headers, base_url=self.base_url, timeout=timeout)

    def _request(
        self,
        path,
        method: str,
        data: Optional[Union[str, bytes]] = None,
        query_params: Optional[Dict[str, str]] = None,
        content: Optional[RawRequestContent] = None,
    ):
        url = f"{self.base_url}{path}"
        opts: Dict[str, Any] = {}
        if all([data, content]):
            raise ValueError("Cannot provide both data and content")
        if data is not None:
            opts["content"] = data
        if content is not None:
            if isinstance(content, dict):
                content = orjson.dumps(content)
            opts["content"] = content
        if query_params is not None:
            opts["params"] = query_params
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


class NucliaDBAsync(_NucliaDBBase):
    """
    Example usage

    >>> from nucliadb_sdk import *
    >>> sdk = NucliaDBAsync(region=Region.EUROPE1, api_key="api-key")
    >>> await sdk.list_resources(kbid='my-kbid')
    """

    def __init__(
        self,
        *,
        region: Region = Region.EUROPE1,
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
        >>> sdk = NucliaDBAsync(api_key="api-key")

        If the Knowledge Box you are interacting with is public, you don't even need the api key

        >>> sdk = NucliaDBAsync()

        If you are connecting to a NucliaDB on-prem instance, you will need to specify the URL

        >>> sdk = NucliaDBAsync(api_key="api-key", region=Region.ON_PREM, url="https://mycompany.api.com/api/nucliadb")
        """  # noqa
        super().__init__(region=region, api_key=api_key, url=url, headers=headers)
        self.session = httpx.AsyncClient(headers=self.headers, base_url=self.base_url, timeout=timeout)

    async def _request(
        self,
        path,
        method: str,
        data: Optional[Union[str, bytes]] = None,
        query_params: Optional[Dict[str, str]] = None,
        content: Optional[RawRequestContent] = None,
    ):
        url = f"{self.base_url}{path}"
        opts: Dict[str, Any] = {}
        if all([data, content]):
            raise ValueError("Cannot provide both data and content")
        if data is not None:
            opts["content"] = data
        if content is not None:
            if isinstance(content, dict):
                content = orjson.dumps(content)
            opts["content"] = content
        if query_params is not None:
            opts["params"] = query_params
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
