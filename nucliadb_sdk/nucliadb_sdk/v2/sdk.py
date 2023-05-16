import enum
from typing import Any, Optional, Type, Union

import httpx
from pydantic import BaseModel

from nucliadb_models.resource import Resource, ResourceList
from nucliadb_sdk.v2 import exceptions
from nucliadb_models.search import (
    FindRequest,
    KnowledgeboxFindResults,
    SearchRequest,
    KnowledgeboxSearchResults,
)

from nucliadb_models.resource import (
    KnowledgeBoxConfig,
    KnowledgeBoxObj,
    KnowledgeBoxList,
)
from nucliadb_models.writer import (
    CreateResourcePayload,
    ResourceCreated,
    ResourceUpdated,
    UpdateResourcePayload,
)


class Region(enum.Enum):
    EUROPE1 = "europe-1"
    ON_PREM = "on-prem"


def _request_builder(
    path_template: str,
    method: str,
    path_params: dict[str, str],
    request_type: Optional[Type[BaseModel]],
    response_type: Optional[Type[BaseModel]],
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
                if not isinstance(content, request_type):
                    raise TypeError(f"Expected {request_type}, got {type(content)}")
                else:
                    data = content.json()
            else:
                # pull properties out of kwargs now
                content_data = {}
                for key in list(kwargs.keys()):
                    if key in request_type.__fields__:
                        content_data[key] = kwargs.pop(key)
                data = request_type.parse_obj(content_data).json()

        query_params = kwargs.pop("query_params", None)
        if len(kwargs) > 0:
            raise TypeError(f"Invalid arguments provided: {kwargs}")

        resp_data = self._request(path, method, data=data, query_params=query_params)

        if response_type is not None:
            return response_type.parse_raw(resp_data)
        else:
            return resp_data

    return _func


class NucliaSDK:
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
            self.base_url = f"https://{region.value}.nuclia.cloud/api"
            headers["X-STF-SERVICEACCOUNT"] = f"Bearer {api_key}"

        self.session = httpx.Client(headers=headers, base_url=self.base_url)

    def _request(
        self,
        path,
        method: str,
        data: Optional[Union[str, bytes]] = None,
        query_params: Optional[dict[str, str]] = None,
    ):
        url = f"{self.base_url}{path}"
        opts = {}
        if data is not None:
            opts["data"] = data
        if query_params is not None:
            opts["params"] = query_params
        response: httpx.Response = getattr(self.session, method.lower())(url, **opts)

        if response.status_code < 300:
            return response.content
        elif response.status_code in (401, 403):
            raise exceptions.AuthError(
                f"Auth error {response.status_code}: {response.text}"
            )
        elif response.status_code == 429:
            raise exceptions.RateLimitError(response.text)
        elif response.status_code == 419:
            raise exceptions.ConflictError(response.text)
        elif response.status_code == 404:
            raise exceptions.NotFoundError(
                f"Resource not found at url {url}: {response.text}"
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
        "/v1/kb/{kbid}/slug/{rslug}", "GET", ("kbid", "slug"), None, Resource
    )

    get_resource_by_id = _request_builder(
        "/v1/kb/{kbid}/resource/{rid}", "GET", ("kbid", "rid"), None, Resource
    )

    list_resources = _request_builder(
        "/v1/kb/{kbid}/resources", "GET", ("kbid",), None, ResourceList
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
