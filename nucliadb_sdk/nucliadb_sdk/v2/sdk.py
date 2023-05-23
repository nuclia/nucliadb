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
from typing import Any, Callable, Optional, Type, Union

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
from nucliadb_sdk.v2 import exceptions


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
    answer, relations_payload = data.split(b"_END_")

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
    data: list[Any],
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
    path_template: str,
    method: str,
    path_params: tuple[str, ...],
    request_type: Optional[Union[Type[BaseModel], list[Any]]],
    response_type: Optional[
        Union[Type[BaseModel], Callable[[httpx.Response], BaseModel]]
    ],
):
    def _func(self: "NucliaSDK", content: Optional[Any] = None, **kwargs):
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
                        data = content.json()
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
                data = request_type.parse_obj(content_data).json()  # type: ignore

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

    return _func


class _NucliaSDKBase:
    def __init__(
        self,
        *,
        region: Region = Region.EUROPE1,
        api_key: Optional[str] = None,
        url: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
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
            if api_key is None:
                raise ValueError("api_key must be provided for cloud sdk usage")
            if url is None:
                self.base_url = f"https://{region.value}.nuclia.cloud/api"
            else:
                self.base_url = url.rstrip("/")
            headers["X-STF-SERVICEACCOUNT"] = f"Bearer {api_key}"

        self.headers = headers

    def _request(
        self,
        path,
        method: str,
        data: Optional[Union[str, bytes]] = None,
        query_params: Optional[dict[str, str]] = None,
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
        elif response.status_code == 419:
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
        "/v1/kbs", "POST", (), KnowledgeBoxConfig, KnowledgeBoxObj
    )
    delete_knowledge_box = _request_builder(
        "/v1/kb/{kbid}", "DELETE", ("kbid",), None, KnowledgeBoxObj
    )
    get_knowledge_box = _request_builder(
        "/v1/kb/{kbid}", "GET", ("kbid",), None, KnowledgeBoxObj
    )
    get_knowledge_box_by_slug = _request_builder(
        "/v1/kb/s/{slug}", "GET", ("slug",), None, KnowledgeBoxObj
    )
    list_knowledge_boxes = _request_builder(
        "/v1/kbs", "GET", (), None, KnowledgeBoxList
    )

    # Resource Endpoints
    create_resource = _request_builder(
        "/v1/kb/{kbid}/resources",
        "POST",
        ("kbid",),
        CreateResourcePayload,
        ResourceCreated,
    )
    update_resource = _request_builder(
        "/v1/kb/{kbid}/resource/{rid}",
        "PATCH",
        ("kbid", "rid"),
        UpdateResourcePayload,
        ResourceUpdated,
    )
    delete_resource = _request_builder(
        "/v1/kb/{kbid}/resource/{rid}", "DELETE", ("kbid", "rid"), None, None
    )
    get_resource_by_slug = _request_builder(
        "/v1/kb/{kbid}/slug/{slug}", "GET", ("kbid", "slug"), None, Resource
    )
    get_resource_by_id = _request_builder(
        "/v1/kb/{kbid}/resource/{rid}", "GET", ("kbid", "rid"), None, Resource
    )
    list_resources = _request_builder(
        "/v1/kb/{kbid}/resources", "GET", ("kbid",), None, ResourceList
    )

    # Conversation endpoints
    add_conversation_message = _request_builder(
        "/v1/kb/{kbid}/resource/{rid}/conversation/{field_id}/messages",
        "PUT",
        ("kbid", "rid", "field_id"),
        list[InputMessage],  # type: ignore
        ResourceFieldAdded,
    )

    # Labels
    set_labelset = _request_builder(
        "/v1/kb/{kbid}/labelset/{labelset}",
        "POST",
        ("kbid", "labelset"),
        LabelSet,
        None,
    )
    delete_labelset = _request_builder(
        "/v1/kb/{kbid}/labelset/{labelset}", "DELETE", ("kbid", "labelset"), None, None
    )
    get_labelsets = _request_builder(
        "/v1/kb/{kbid}/labelset", "GET", ("kbid", "labelset"), None, KnowledgeBoxLabels
    )
    get_labelset = _request_builder(
        "/v1/kb/{kbid}/labelset/{labelset}", "GET", ("kbid", "labelset"), None, LabelSet
    )

    # Entity Groups
    create_entitygroup = _request_builder(
        "/v1/kb/{kbid}/entitiesgroups",
        "POST",
        ("kbid",),
        CreateEntitiesGroupPayload,
        None,
    )
    update_entitygroup = _request_builder(
        "/v1/kb/{kbid}/entitiesgroup/{group}",
        "PATCH",
        ("kbid", "group"),
        UpdateEntitiesGroupPayload,
        None,
    )
    set_entitygroup_entities = _request_builder(
        "/v1/kb/{kbid}/entitiesgroup/{group}",
        "POST",
        ("kbid", "group"),
        EntitiesGroup,
        None,
    )
    delete_entitygroup = _request_builder(
        "/v1/kb/{kbid}/labelset/{labelset}", "DELETE", ("kbid", "labelset"), None, None
    )
    get_entitygroups = _request_builder(
        "/v1/kb/{kbid}/labelset", "GET", ("kbid", "labelset"), None, KnowledgeBoxLabels
    )
    get_entitygroup = _request_builder(
        "/v1/kb/{kbid}/labelset/{labelset}", "GET", ("kbid", "labelset"), None, LabelSet
    )

    # Vectorsets
    create_vectorset = _request_builder(
        "/v1/kb/{kbid}/vectorset/{vectorset}",
        "POST",
        ("kbid", "vectorset"),
        VectorSet,
        None,
    )
    delete_vectorset = _request_builder(
        "/v1/kb/{kbid}/vectorset/{vectorset}", "POST", ("kbid", "vectorset"), None, None
    )
    list_vectorsets = _request_builder(
        "/v1/kb/{kbid}/vectorsets", "GET", ("kbid",), None, VectorSets
    )

    # Search / Find Endpoints
    find = _request_builder(
        "/v1/kb/{kbid}/find", "POST", ("kbid",), FindRequest, KnowledgeboxFindResults
    )
    search = _request_builder(
        "/v1/kb/{kbid}/search",
        "POST",
        ("kbid",),
        SearchRequest,
        KnowledgeboxSearchResults,
    )
    chat = _request_builder(
        "/v1/kb/{kbid}/chat", "POST", ("kbid",), ChatRequest, chat_response_parser
    )


class NucliaSDK(_NucliaSDKBase):
    """
    Example usage:

    from nucliadb_sdk.v2.sdk import *
    sdk = NucliaSDK(region=Region.EUROPE1, api_key="api-key")
    sdk.list_resources(kbid='70a2530a-5863-41ec-b42b-bfe795bef2eb')
    """

    def __init__(
        self,
        *,
        region: Region = Region.EUROPE1,
        api_key: Optional[str] = None,
        url: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
        timeout: Optional[float] = 60.0,
    ):
        super().__init__(region=region, api_key=api_key, url=url, headers=headers)
        self.session = httpx.Client(
            headers=self.headers, base_url=self.base_url, timeout=timeout
        )

    def _request(
        self,
        path,
        method: str,
        data: Optional[Union[str, bytes]] = None,
        query_params: Optional[dict[str, str]] = None,
    ):
        url = f"{self.base_url}{path}"
        opts: dict[str, Any] = {}
        if data is not None:
            opts["data"] = data
        if query_params is not None:
            opts["params"] = query_params
        response: httpx.Response = getattr(self.session, method.lower())(url, **opts)
        return self._check_response(response)


class NucliaSDKAsync(_NucliaSDKBase):
    """
    Example usage:

    from nucliadb_sdk.v2.sdk import *
    sdk = NucliaSDK(region=Region.EUROPE1, api_key="api-key")
    sdk.list_resources(kbid='70a2530a-5863-41ec-b42b-bfe795bef2eb')
    """

    def __init__(
        self,
        *,
        region: Region = Region.EUROPE1,
        api_key: Optional[str] = None,
        url: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
        timeout: Optional[float] = 60.0,
    ):
        super().__init__(region=region, api_key=api_key, url=url, headers=headers)
        self.session = httpx.AsyncClient(
            headers=self.headers, base_url=self.base_url, timeout=timeout
        )

    async def _request(
        self,
        path,
        method: str,
        data: Optional[Union[str, bytes]] = None,
        query_params: Optional[dict[str, str]] = None,
    ):
        url = f"{self.base_url}{path}"
        opts: dict[str, Any] = {}
        if data is not None:
            opts["data"] = data
        if query_params is not None:
            opts["params"] = query_params
        response: httpx.Response = await getattr(self.session, method.lower())(
            url, **opts
        )
        self._check_response(response)
        return response
