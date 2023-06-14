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
import logging
from typing import Any, Optional

import aiohttp
import pydantic
from aiohttp.web import Response

from nucliadb_utils.settings import nuclia_settings

from . import exceptions

logger = logging.getLogger(__name__)


def get_processing_api_url() -> str:
    if nuclia_settings.nuclia_service_account:
        return (
            nuclia_settings.nuclia_public_url.format(zone=nuclia_settings.nuclia_zone)
            + "/api/v1/processing"
        )
    else:
        return nuclia_settings.nuclia_cluster_url + "/api/internal/processing"


def check_status(resp: aiohttp.ClientResponse, resp_text: str) -> None:
    if resp.status < 300:
        return
    elif resp.status == 402:
        raise exceptions.AccountLimitException(f"Account limits exceeded: {resp_text}")
    elif resp.status == 404:
        raise exceptions.NotFoundException(f"Resource not found: {resp_text}")
    elif resp.status in (401, 403):
        raise exceptions.AuthorizationException(
            f"Unauthorized to access: {resp.status}"
        )
    elif resp.status == 429:
        raise exceptions.RateLimitException("Rate limited")
    else:
        raise exceptions.ClientException(f"Unknown error: {resp.status} - {resp_text}")


class TelemetryHeadersMissing(Exception):
    pass


def check_proxy_telemetry_headers(resp: Response):
    if nuclia_settings.nuclia_service_account is not None:
        # do not care with on prem
        return
    try:
        expected = [
            "x-b3-traceid",
            "x-b3-spanid",
            "x-b3-sampled",
        ]
        missing = [header for header in expected if header not in resp.headers]
        if len(missing) > 0:
            raise TelemetryHeadersMissing(
                f"Missing headers {missing} in proxy response"
            )
    except TelemetryHeadersMissing:
        logger.warning("Some telemetry headers not found in proxy response")


class StatusResponse(pydantic.BaseModel):
    shared: dict[str, Any]
    account: Any


class PullResponse(pydantic.BaseModel):
    status: str
    payload: Optional[str] = None
    msgid: Optional[int] = None


class ProcessingHTTPClient:
    def __init__(self):
        self.session = aiohttp.ClientSession()
        self.base_url = get_processing_api_url()
        self.headers = {}
        if nuclia_settings.nuclia_service_account is not None:
            self.headers[
                "X-STF-NUAKEY"
            ] = f"Bearer {nuclia_settings.nuclia_service_account}"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        await self.session.close()

    async def status(self) -> StatusResponse:
        url = self.base_url + "/status"
        async with self.session.get(url, headers=self.headers) as resp:
            resp_text = await resp.text()
            check_status(resp, resp_text)
            return StatusResponse.parse_raw(resp_text)

    async def pull(self, partition: str) -> PullResponse:
        url = self.base_url + "/pull?partition=" + partition
        async with self.session.get(url, headers=self.headers) as resp:
            resp_text = await resp.text()
            check_proxy_telemetry_headers(resp)
            check_status(resp, resp_text)
            return PullResponse.parse_raw(resp_text)
