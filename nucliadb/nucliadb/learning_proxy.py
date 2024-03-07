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
#
import contextlib
import json
import logging
from collections.abc import AsyncIterator
from enum import Enum
from typing import Any, Optional, Union

import backoff
import httpx
from fastapi import Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from nucliadb_telemetry import errors
from nucliadb_utils.settings import is_onprem_nucliadb, nuclia_settings

SERVICE_NAME = "nucliadb.learning_proxy"
logger = logging.getLogger(SERVICE_NAME)

WHITELISTED_HEADERS = {
    "x-nucliadb-user",
    "x-nucliadb-roles",
    "x-stf-roles",
    "x-stf-user",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-forwarded-port",
}


class LearningService(str, Enum):
    CONFIG = "config"
    COLLECTOR = "collector-api"


class LearningConfiguration(BaseModel):
    semantic_model: str
    semantic_vector_similarity: str
    semantic_vector_size: Optional[int]
    semantic_threshold: Optional[float]


async def get_configuration(
    kbid: str,
) -> Optional[LearningConfiguration]:
    async with learning_config_client() as client:
        resp = await client.get(f"config/{kbid}")
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as err:
            if err.response.status_code == 404:
                return None
            raise
        return LearningConfiguration.parse_obj(resp.json())


async def set_configuration(
    kbid: str,
    config: dict[str, Any],
) -> LearningConfiguration:
    async with learning_config_client() as client:
        resp = await client.post(f"config/{kbid}", json=config)
        resp.raise_for_status()
        return LearningConfiguration.parse_obj(resp.json())


async def delete_configuration(
    kbid: str,
) -> None:
    async with learning_config_client() as client:
        resp = await client.delete(f"config/{kbid}")
        resp.raise_for_status()


async def learning_config_proxy(
    request: Request,
    method: str,
    url: str,
    extra_headers: Optional[dict[str, str]] = None,
) -> Union[Response, StreamingResponse]:
    return await proxy(
        service=LearningService.CONFIG,
        request=request,
        method=method,
        url=url,
        extra_headers=extra_headers,
    )


async def learning_collector_proxy(
    request: Request,
    method: str,
    url: str,
    extra_headers: Optional[dict[str, str]] = None,
) -> Union[Response, StreamingResponse]:
    return await proxy(
        service=LearningService.COLLECTOR,
        request=request,
        method=method,
        url=url,
        extra_headers=extra_headers,
    )


def is_white_listed_header(header: str) -> bool:
    return header.lower() in WHITELISTED_HEADERS


@backoff.on_exception(
    backoff.expo,
    (Exception,),  # retry all unhandled http client/server errors right now
    jitter=backoff.random_jitter,
    max_tries=3,
)
async def _retriable_proxied_request(
    *,
    client: httpx.AsyncClient,
    method: str,
    url: str,
    content: bytes,
    headers: dict[str, str],
    params: dict[str, Any],
) -> httpx.Response:
    return await client.request(
        method=method.upper(),
        url=url,
        params=params,
        content=content,
        headers=headers,
    )


async def proxy(
    service: LearningService,
    request: Request,
    method: str,
    url: str,
    extra_headers: Optional[dict[str, str]] = None,
) -> Union[Response, StreamingResponse]:
    """
    Proxy the request to a learning API.

    service: LearningService. The learning service to proxy the request to.
    request: Request. The incoming request.
    method: str. The HTTP method to use.
    url: str. The URL to proxy the request to.
    extra_headers: Optional[dict[str, str]]. Extra headers to include in the proxied request.

    Returns: Response. The response from the learning API. If the response is chunked, a StreamingResponse is returned.
    """

    proxied_headers = extra_headers or {}
    proxied_headers.update(
        {k.lower(): v for k, v in request.headers.items() if is_white_listed_header(k)}
    )

    async with service_client(
        base_url=get_base_url(service=service),
        headers=get_auth_headers(),
    ) as client:
        try:
            response = await _retriable_proxied_request(
                client=client,
                method=method.upper(),
                url=url,
                params=dict(request.query_params),
                content=await request.body(),
                headers=proxied_headers,
            )
        except Exception as exc:
            errors.capture_exception(exc)
            msg = f"Unexpected error while trying to proxy the request to the learning {service.value} API."
            logger.exception(msg, exc_info=True)
            return Response(
                content=msg.encode(),
                status_code=503,
                media_type="text/plain",
            )
        if response.headers.get("Transfer-Encoding") == "chunked":
            return StreamingResponse(
                content=response.aiter_bytes(),
                status_code=response.status_code,
                headers=response.headers,
                media_type=response.headers.get("Content-Type"),
            )
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=response.headers,
            media_type=response.headers.get("Content-Type"),
        )


def get_base_url(service: LearningService) -> str:
    if is_onprem_nucliadb():
        nuclia_public_url = nuclia_settings.nuclia_public_url.format(
            zone=nuclia_settings.nuclia_zone
        )
        return f"{nuclia_public_url}/api/v1"
    learning_svc_base_url = nuclia_settings.learning_internal_svc_base_url.format(
        service=service.value
    )
    return f"{learning_svc_base_url}/api/v1/internal"


def get_auth_headers() -> dict[str, str]:
    if is_onprem_nucliadb():
        # public api: auth is done via the 'x-nuclia-nuakey' header
        return {"X-NUCLIA-NUAKEY": f"Bearer {nuclia_settings.nuclia_service_account}"}
    else:
        # internal api: auth is proxied from the request coming to NucliaDB to the learning
        # apis via the 'x-nucliadb-user' and 'x-nucliadb-roles' headers that
        # idp injects on auth lookup.
        return {}


@contextlib.asynccontextmanager
async def service_client(
    base_url: str,
    headers: dict[str, str],
) -> AsyncIterator[httpx.AsyncClient]:
    """
    Context manager for the learning client. Makes sure the client is closed after use.
    For now, a new client session is created for each request. This is to avoid having to
    save a client session in the FastAPI app state.
    """
    if nuclia_settings.dummy_learning_services:
        # This is a workaround to be able to run integration tests that start nucliadb with docker.
        # The learning APIs are not available in the docker setup, so we use a dummy client.
        client = DummyClient(base_url=base_url, headers=headers)
        logger.warning(
            "Using dummy client. If you see this in production, something is wrong."
        )
    else:
        client = httpx.AsyncClient(base_url=base_url, headers=headers)  # type: ignore
    try:
        yield client
    finally:
        if client.is_closed is False:
            await client.aclose()


@contextlib.asynccontextmanager
async def learning_config_client() -> AsyncIterator[httpx.AsyncClient]:
    """
    Context manager for the learning config client.
    """
    async with service_client(
        base_url=get_base_url(LearningService.CONFIG), headers=get_auth_headers()
    ) as client:
        yield client


class DummyResponse(httpx.Response):
    def raise_for_status(self) -> httpx.Response:
        return self


class DummyClient(httpx.AsyncClient):
    def _response(self, content=None):
        if content is None:
            content = {"detail": "Dummy client is not supposed to be used"}
        return DummyResponse(
            status_code=200,
            headers={"content-type": "application/json"},
            content=json.dumps(content).encode(),
        )

    async def get(self, *args, **kwargs: Any):
        return self._handle_request("GET", *args, **kwargs)

    async def post(self, *args: Any, **kwargs: Any):
        return self._handle_request("POST", *args, **kwargs)

    async def patch(self, *args: Any, **kwargs: Any):
        return self._handle_request("PATCH", *args, **kwargs)

    async def delete(self, *args: Any, **kwargs: Any):
        return self._handle_request("DELETE", *args, **kwargs)

    def get_config(self, *args: Any, **kwargs: Any):
        lconfig = LearningConfiguration(
            semantic_model="multilingual",
            semantic_vector_similarity="cosine",
            semantic_vector_size=None,
            semantic_threshold=None,
        )
        return self._response(content=lconfig.dict())

    async def request(  # type: ignore
        self,
        method: str,
        url: str,
        params=None,
        content=None,
        headers=None,
    ) -> httpx.Response:
        return self._handle_request(
            method, url, params=params, content=content, headers=headers
        )

    def _handle_request(self, *args: Any, **kwargs: Any) -> httpx.Response:
        """
        Try to map HTTP Method + Path to methods of this class:
        e.g: GET /config/{kbid} -> get_config
        """
        http_method = args[0]
        http_url = args[1]
        method = f"{http_method.lower()}_{http_url.split('/')[0]}"
        if hasattr(self, method):
            return getattr(self, method)(*args, **kwargs)
        else:
            return self._response()
