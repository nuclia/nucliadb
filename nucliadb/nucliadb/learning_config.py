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
from typing import Any, Optional, Type, Union

import httpx
from fastapi import Request, Response
from fastapi.responses import StreamingResponse

from nucliadb_telemetry import errors
from nucliadb_utils.settings import nuclia_settings

SERVICE_NAME = "nucliadb.learning_config"
logger = logging.getLogger(SERVICE_NAME)


NUCLIA_ONPREM_AUTH_HEADER = "X-NUCLIA-NUAKEY"


async def set_configuration(
    kbid: str,
    config: dict[str, Any],
) -> None:
    async with learning_config_client() as client:
        resp = await client.post(f"config/{kbid}", json=config)
        resp.raise_for_status()


async def delete_configuration(
    kbid: str,
) -> None:
    async with learning_config_client() as client:
        resp = await client.delete(f"config/{kbid}")
        resp.raise_for_status()


async def proxy(
    request: Request, method: str, url: str, headers: Optional[dict[str, str]] = None
) -> Union[Response, StreamingResponse]:
    """
    Proxy the request to the learning config API.

    request: Request. The incoming request.
    method: str. The HTTP method to use.
    url: str. The URL to proxy the request to.

    Returns: Response. The response from the learning config API.
    If the response is chunked, a StreamingResponse is returned.
    """
    proxied_headers = headers or {}
    proxied_headers.update({k.lower(): v for k, v in request.headers.items()})
    proxied_headers.pop("host", None)

    async with learning_config_client() as client:
        try:
            response = await client.request(
                method=method.upper(),
                url=url,
                params=request.query_params,
                content=await request.body(),
                headers=proxied_headers,
            )
        except Exception as exc:
            errors.capture_exception(exc)
            return Response(
                content=b"Unexpected error while trying to proxy the request to the learning config API. Please try again later.",  # noqa
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


def is_onprem() -> bool:
    return nuclia_settings.nuclia_service_account is not None


def get_config_api_url() -> str:
    if is_onprem():
        nuclia_public_url = nuclia_settings.nuclia_public_url.format(
            zone=nuclia_settings.nuclia_zone
        )
        return f"{nuclia_public_url}/api/v1"
    else:
        return f"{nuclia_settings.nuclia_inner_learning_config_url}/api/v1/internal"


def get_config_auth_header() -> dict[str, str]:
    if is_onprem():
        # public api: auth is done via the 'x-nuclia-nuakey' header
        return {"X-NUCLIA-NUAKEY": f"Bearer {nuclia_settings.nuclia_service_account}"}
    else:
        # internal api: auth is proxied from the request to the learning
        # config api via the 'x-nucliadb-user' and 'x-nucliadb-roles' headers
        return {}


class DummyResponse(httpx.Response):
    def raise_for_status(self) -> httpx.Response:
        return self


class DummyClient(httpx.AsyncClient):
    def _response(self):
        content = {"detail": "Dummy client is not supposed to be used"}
        return DummyResponse(
            status_code=200,
            headers={"content-type": "application/json"},
            content=json.dumps(content).encode(),
        )

    async def get(self, *args: Any, **kwargs: Any):
        return self._response()

    async def post(self, *args: Any, **kwargs: Any):
        return self._response()

    async def delete(self, *args: Any, **kwargs: Any):
        return self._response()

    async def request(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> httpx.Response:
        return self._response()


@contextlib.asynccontextmanager
async def learning_config_client() -> AsyncIterator[httpx.AsyncClient]:
    """
    Context manager for the learning client. Makes sure the client is closed after use.
    For now, a new client session is created for each request. This is to avoid having to
    save a client session in the FastAPI app state.
    """
    client_class: Type[httpx.AsyncClient]
    if nuclia_settings.dummy_learning_config:
        # This is a workaround to be able to run integration tests that start nucliadb with docker.
        # The learning config API is not available in the docker setup, so we use a dummy client.
        client_class = DummyClient
        logger.warning(
            "Using dummy learning config client. If you see this in production, something is wrong."
        )
    else:
        client_class = httpx.AsyncClient

    client = client_class(
        base_url=get_config_api_url(),
        headers=get_config_auth_header(),
    )
    try:
        yield client
    finally:
        if client.is_closed is False:
            await client.aclose()
