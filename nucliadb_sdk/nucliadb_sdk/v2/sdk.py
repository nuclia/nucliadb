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
import io
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import httpx
import orjson
from pydantic import BaseModel

from nucliadb_models.conversation import InputMessage
from nucliadb_models.entities import (
    CreateEntitiesGroupPayload,
    EntitiesGroup,
    UpdateEntitiesGroupPayload,
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
    ChatRequest,
    FeedbackRequest,
    FindRequest,
    KnowledgeboxFindResults,
    KnowledgeboxSearchResults,
    Relations,
    SearchRequest,
)
from nucliadb_models.vectors import VectorSet, VectorSets
from nucliadb_models.writer import (
    CreateResourcePayload,
    ResourceCreated,
    ResourceFieldAdded,
    ResourceUpdated,
    UpdateResourcePayload,
)
from nucliadb_sdk.v2 import docstrings, exceptions


class Region(enum.Enum):
    EUROPE1 = "europe-1"
    ON_PREM = "on-prem"


class ChatResponse(BaseModel):
    result: KnowledgeboxFindResults
    answer: str
    relations: Optional[Relations]
    learning_id: Optional[str]


def chat_response_parser(response: httpx.Response) -> ChatResponse:
    raw = io.BytesIO(response.content)
    header = raw.read(4)
    payload_size = int.from_bytes(header, byteorder="big", signed=False)
    data = raw.read(payload_size)
    find_result = KnowledgeboxFindResults.parse_raw(base64.b64decode(data))
    data = raw.read()
    try:
        answer, relations_payload = data.split(b"_END_")
    except ValueError:
        answer = data
        relations_payload = b""
    learning_id = response.headers.get("NUCLIA-LEARNING-ID")
    relations_result = None
    if len(relations_payload) > 0:
        relations_result = Relations.parse_raw(base64.b64decode(relations_payload))

    return ChatResponse(
        result=find_result,
        answer=answer,
        relations=relations_result,
        learning_id=learning_id,
    )


def _parse_list_of_pydantic(
    data: List[Any],
) -> str:
    output = []
    for item in data:
        if isinstance(item, BaseModel):
            output.append(item.dict())
        else:
            output.append(item)
    return orjson.dumps(output).decode("utf-8")


def _parse_response(response_type, resp: httpx.Response) -> Any:
    if response_type is not None:
        if isinstance(response_type, type) and issubclass(response_type, BaseModel):
            return response_type.parse_raw(resp.content)  # type: ignore
        else:
            return response_type(resp)  # type: ignore
    else:
        return resp.content


def _request_builder(
    *,
    name: str,
    method: str,
    path_template: str,
    path_params: Tuple[str, ...],
    request_type: Optional[Union[Type[BaseModel], List[Any]]],
    response_type: Optional[
        Union[Type[BaseModel], Callable[[httpx.Response], BaseModel]]
    ],
    docstring: Optional[docstrings.Docstring] = None,
):
    def _func(self: "NucliaDB", content: Optional[Any] = None, **kwargs):
        path_data = {}
        for param in path_params:
            if param not in kwargs:
                raise TypeError(f"Missing required parameter {param}")
            path_data[param] = kwargs.pop(param)

        path = path_template.format(**path_data)
        data = None
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
                    if key in request_type.__fields__:  # type: ignore
                        content_data[key] = kwargs.pop(key)
                data = request_type.parse_obj(content_data).json(by_alias=True)  # type: ignore

        query_params = kwargs.pop("query_params", None)
        if len(kwargs) > 0:
            raise TypeError(f"Invalid arguments provided: {kwargs}")

        resp = self._request(path, method, data=data, query_params=query_params)

        if asyncio.iscoroutine(resp):

            async def _wrapped_resp():
                real_resp = await resp
                return _parse_response(response_type, real_resp)

            return _wrapped_resp()
        else:
            return _parse_response(response_type, resp)

    docstrings.inject_documentation(
        _func,
        name,
        method,
        path_template,
        path_params,
        request_type,
        response_type,
        docstring,
    )

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
        self.region = region
        self.api_key = api_key
        headers = headers or {}
        if region == Region.ON_PREM:
            if url is None:
                raise ValueError("url must be provided for on-prem")
            self.base_url = url.rstrip("/")
            # By default, on prem should utilize all headers available
            # For custom auth schemes, the user will need to provide custom
            # auth headers
            headers["X-NUCLIADB-ROLES"] = "MANAGER;WRITER;READER"
        else:
            if url is None:
                self.base_url = f"https://{region.value}.nuclia.cloud/api"
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
    ):
        raise NotImplementedError

    def _check_response(self, response: httpx.Response):
        if response.status_code < 300:
            return response
        elif response.status_code in (401, 403):
            raise exceptions.AuthError(
                f"Auth error {response.status_code}: {response.text}"
            )
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
            raise exceptions.NotFoundError(
                f"Resource not found at url {response.url}: {response.text}"
            )
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
        docstring=docstrings.CREATE_RESOURCE,
    )
    update_resource = _request_builder(
        name="update_resource",
        path_template="/v1/kb/{kbid}/resource/{rid}",
        method="PATCH",
        path_params=("kbid", "rid"),
        request_type=UpdateResourcePayload,
        response_type=ResourceUpdated,
        docstring=docstrings.UPDATE_RESOURCE,
    )
    delete_resource = _request_builder(
        name="delete_resource",
        path_template="/v1/kb/{kbid}/resource/{rid}",
        method="DELETE",
        path_params=("kbid", "rid"),
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
        docstring=docstrings.GET_RESOURCE_BY_SLUG,
    )
    get_resource_by_id = _request_builder(
        name="get_resource_by_id",
        path_template="/v1/kb/{kbid}/resource/{rid}",
        method="GET",
        path_params=("kbid", "rid"),
        request_type=None,
        response_type=Resource,
        docstring=docstrings.GET_RESOURCE_BY_ID,
    )
    list_resources = _request_builder(
        name="list_resources",
        path_template="/v1/kb/{kbid}/resources",
        method="GET",
        path_params=("kbid",),
        request_type=None,
        response_type=ResourceList,
        docstring=docstrings.LIST_RESOURCES,
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
        docstring=docstrings.DELETE_LABELSET,
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
    set_entitygroup_entities = _request_builder(
        name="set_entitygroup_entities",
        path_template="/v1/kb/{kbid}/entitiesgroup/{group}",
        method="POST",
        path_params=("kbid", "group"),
        request_type=EntitiesGroup,
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
        path_params=("kbid", "show_entities"),
        request_type=None,
        response_type=KnowledgeBoxLabels,
    )
    get_entitygroup = _request_builder(
        name="get_entitygroup",
        path_template="/v1/kb/{kbid}/entitiesgroup/{group}",
        method="GET",
        path_params=("kbid", "group"),
        request_type=None,
        response_type=LabelSet,
    )

    # Vectorsets
    create_vectorset = _request_builder(
        name="create_vectorset",
        path_template="/v1/kb/{kbid}/vectorset/{vectorset}",
        method="POST",
        path_params=("kbid", "vectorset"),
        request_type=VectorSet,
        response_type=None,
    )
    delete_vectorset = _request_builder(
        name="delete_vectorset",
        path_template="/v1/kb/{kbid}/vectorset/{vectorset}",
        method="POST",
        path_params=("kbid", "vectorset"),
        request_type=None,
        response_type=None,
    )
    list_vectorsets = _request_builder(
        name="list_vectorsets",
        path_template="/v1/kb/{kbid}/vectorsets",
        method="GET",
        path_params=("kbid",),
        request_type=None,
        response_type=VectorSets,
    )

    # Search / Find Endpoints
    find = _request_builder(
        name="find",
        path_template="/v1/kb/{kbid}/find",
        method="POST",
        path_params=("kbid",),
        request_type=FindRequest,
        response_type=KnowledgeboxFindResults,
        docstring=docstrings.FIND,
    )
    search = _request_builder(
        name="search",
        path_template="/v1/kb/{kbid}/search",
        method="POST",
        path_params=("kbid",),
        request_type=SearchRequest,
        response_type=KnowledgeboxSearchResults,
        docstring=docstrings.SEARCH,
    )
    chat = _request_builder(
        name="chat",
        path_template="/v1/kb/{kbid}/chat",
        method="POST",
        path_params=("kbid",),
        request_type=ChatRequest,
        response_type=chat_response_parser,
        docstring=docstrings.CHAT,
    )
    chat_on_resource = _request_builder(
        name="chat_on_resource",
        path_template="/v1/kb/{kbid}/resource/{rid}/chat",
        method="POST",
        path_params=("kbid", "rid"),
        request_type=ChatRequest,
        response_type=chat_response_parser,
        docstring=docstrings.RESOURCE_CHAT,
    )
    feedback = _request_builder(
        name="feedback",
        path_template="/v1/kb/{kbid}/feedback",
        method="POST",
        path_params=("kbid",),
        request_type=FeedbackRequest,
        response_type=None,
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
        self.session = httpx.Client(
            headers=self.headers, base_url=self.base_url, timeout=timeout
        )

    def _request(
        self,
        path,
        method: str,
        data: Optional[Union[str, bytes]] = None,
        query_params: Optional[Dict[str, str]] = None,
    ):
        url = f"{self.base_url}{path}"
        opts: Dict[str, Any] = {}
        if data is not None:
            opts["data"] = data
        if query_params is not None:
            opts["params"] = query_params
        response: httpx.Response = getattr(self.session, method.lower())(url, **opts)
        return self._check_response(response)


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
        self.session = httpx.AsyncClient(
            headers=self.headers, base_url=self.base_url, timeout=timeout
        )

    async def _request(
        self,
        path,
        method: str,
        data: Optional[Union[str, bytes]] = None,
        query_params: Optional[Dict[str, str]] = None,
    ):
        url = f"{self.base_url}{path}"
        opts: Dict[str, Any] = {}
        if data is not None:
            opts["data"] = data
        if query_params is not None:
            opts["params"] = query_params
        response: httpx.Response = await getattr(self.session, method.lower())(
            url, **opts
        )
        self._check_response(response)
        return response
