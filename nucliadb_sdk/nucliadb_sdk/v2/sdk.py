import enum
from typing import Any, Optional, Type, Union

import httpx
from pydantic import BaseModel

from nucliadb_models.resource import Resource, ResourceList
from nucliadb_sdk.v2 import exceptions


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
        if content is None and request_type is not None:
            raise TypeError("Missing required request body")

        if request_type is not None:
            if not isinstance(content, request_type):
                raise TypeError(f"Expected {request_type}, got {type(content)}")
            kwargs["data"] = content.json()

        resp_data = self._request(path, method, **kwargs)

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

    def __init__(self, *, region: Region, api_key: str, url: Optional[str] = None):
        self.region = region
        self.api_key = api_key
        if region == Region.ON_PREM:
            if url is None:
                raise ValueError("url must be provided for on-prem")
            self.base_url = url
        else:
            self.base_url = f"https://{region.value}.nuclia.cloud/api"

        self.session = httpx.Client(
            headers={"X-STF-SERVICEACCOUNT": f"Bearer {api_key}"},
            base_url=self.base_url,
        )

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

        if response.status_code == 200:
            return response.content
        elif response.status_code == 404:
            raise exceptions.NotFoundError(response.text)
        else:
            raise exceptions.UnknownError(
                f"Status code {response.status_code}: {response.text}"
            )

    get_resource_by_slug = _request_builder(
        "/v1/kb/{kbid}/slug/{rslug}",
        "GET",
        ("kbid", "slug"),
        None,
        Resource,
    )

    get_resource_by_id = _request_builder(
        "/v1/kb/{kbid}/resource/{rid}",
        "GET",
        ("kbid", "rid"),
        None,
        Resource,
    )

    list_resources = _request_builder(
        "/v1/kb/{kbid}/resources",
        "GET",
        ("kbid",),
        None,
        ResourceList,
    )
