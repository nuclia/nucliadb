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
from datetime import datetime
from typing import Optional

import aiohttp
import jwt
import pydantic

from nucliadb_utils.helpers import MessageProgressUpdater
from nucliadb_utils.settings import nuclia_settings

from .utils import check_status

logger = logging.getLogger(__name__)


def get_nua_api_id() -> str:
    assert nuclia_settings.nuclia_service_account is not None
    claimset = jwt.decode(
        nuclia_settings.nuclia_service_account,
        options={"verify_signature": False},
    )
    return claimset.get("sub")


def get_processing_api_url() -> str:
    if nuclia_settings.nuclia_service_account:
        return (
            nuclia_settings.nuclia_public_url.format(zone=nuclia_settings.nuclia_zone)
            + "/api/v1/processing"
        )
    else:
        return nuclia_settings.nuclia_processing_cluster_url + "/api/v1/internal/processing"


def get_processing_api_v2_url() -> str:
    if nuclia_settings.nuclia_service_account:
        return (
            nuclia_settings.nuclia_public_url.format(zone=nuclia_settings.nuclia_zone)
            + "/api/v2/processing"
        )
    else:
        return nuclia_settings.nuclia_processing_cluster_url + "/api/v2/internal/processing"


class PullResponse(pydantic.BaseModel):
    status: str
    payload: Optional[str] = None
    payloads: list[bytes] = []
    msgid: Optional[str] = None
    cursor: Optional[int] = None


class PullPosition(pydantic.BaseModel):
    cursor: int


class RequestsResult(pydantic.BaseModel):
    processing_id: str = pydantic.Field(
        ...,
        title="Processing ID",
        description="Processing ID of the resource.",
    )
    resource_id: str = pydantic.Field(
        ...,
        title="Resource ID",
        description="Resource ID.",
    )
    kbid: str = pydantic.Field(..., title="KnowledgeBox ID")
    title: Optional[str] = pydantic.Field(
        None,
        title="Title",
        description="Title of the resource.",
    )
    labels: list[str] = pydantic.Field(
        [],
        title="Labels",
        description="Labels of the resource.",
    )
    completed: bool = pydantic.Field(
        ...,
        title="Completed",
        description="Whether the resource has been completed",
    )
    scheduled: bool = pydantic.Field(
        ...,
        title="Scheduled",
        description="Whether the resource has been scheduled",
    )
    timestamp: datetime = pydantic.Field(
        ...,
        title="Timestamp",
        description="Timestamp of when the resource was first scheduled.",
    )
    completed_at: Optional[datetime] = pydantic.Field(
        None,
        title="Completed At",
        description="Timestamp of when the resource was completed",
    )
    scheduled_at: Optional[datetime] = pydantic.Field(
        None,
        title="Scheduled At",
        description="Timestamp of when the resource was first scheduled.",
    )
    failed: bool = pydantic.Field(
        False,
        title="Failed",
        description="Whether the resource has failed to process",
    )
    retries: int = pydantic.Field(
        0,
        title="Retries",
        description="Number of retries for the resource.",
    )
    schedule_eta: float = pydantic.Field(
        0.0,
        title="Schedule ETA",
        description="Estimated time until the resource is scheduled.",
    )
    schedule_order: int = pydantic.Field(
        0,
        title="Schedule Order",
        description="Order of the resource in the schedule queue.",
    )


class RequestsResults(pydantic.BaseModel):
    results: list[RequestsResult] = pydantic.Field(
        [],
        title="Results",
        description="List of results.",
    )
    cursor: Optional[str] = pydantic.Field(
        None,
        title="Cursor",
        description="Cursor to use for the next page of results.",
    )


class StatsResponse(pydantic.BaseModel):
    incomplete: int
    scheduled: int


class PullRequestV2(pydantic.BaseModel):
    timeout: float = 5
    limit: int = 1
    ack: list[str] = []


class InProgressRequest(pydantic.BaseModel):
    ack: list[str] = []


class PulledMessage(pydantic.BaseModel):
    payload: bytes
    headers: dict[str, str]
    ack_token: str
    seq: int


class PullResponseV2(pydantic.BaseModel):
    messages: list[PulledMessage]
    ttl: float
    pending: int


class PullStatusResponse(pydantic.BaseModel):
    pending: int


JSON_HEADERS = {"Content-Type": "application/json"}


class ProcessingHTTPClient:
    def __init__(self) -> None:
        self.session = aiohttp.ClientSession()
        self.base_url = get_processing_api_url()
        self.base_url_v2 = get_processing_api_v2_url()
        self.headers: dict[str, str] = {}
        if nuclia_settings.nuclia_service_account is not None:
            self.headers["X-STF-NUAKEY"] = f"Bearer {nuclia_settings.nuclia_service_account}"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        await self.session.close()

    async def in_progress(self, ack_token: str):
        url = self.base_url_v2 + "/pull/in_progress"
        request = InProgressRequest(ack=[ack_token])
        async with self.session.post(
            url, headers=self.headers | JSON_HEADERS, data=request.model_dump_json()
        ) as resp:
            resp_text = await resp.text()
            check_status(resp, resp_text)

    async def pull_v2(
        self, ack_tokens: list[str], limit: int = 1, timeout: float = 5
    ) -> Optional[PullResponseV2]:
        url = self.base_url_v2 + "/pull"
        request = PullRequestV2(limit=limit, timeout=timeout, ack=ack_tokens)
        async with self.session.post(
            url, headers=self.headers | JSON_HEADERS, data=request.model_dump_json()
        ) as resp:
            resp_text = await resp.text()
            check_status(resp, resp_text)

            if resp.status == 204:
                return None
            else:
                return PullResponseV2.model_validate_json(resp_text)

    async def pull_status(self) -> PullStatusResponse:
        url = self.base_url_v2 + "/pull/status"
        async with self.session.get(url, headers=self.headers) as resp:
            resp_text = await resp.text()
            check_status(resp, resp_text)

            return PullStatusResponse.model_validate_json(resp_text)

    async def requests(
        self,
        cursor: Optional[str] = None,
        scheduled: Optional[bool] = None,
        kbid: Optional[str] = None,
        limit: int = 20,
    ) -> RequestsResults:
        url = self.base_url + "/requests"
        params: dict[str, str] = {"limit": str(limit)}
        if cursor is not None:
            params["cursor"] = cursor
        if scheduled is not None:
            params["scheduled"] = str(scheduled)
        if kbid is not None:
            params["kbid"] = kbid

        async with self.session.get(url, headers=self.headers, params=params) as resp:
            resp_text = await resp.text()
            check_status(resp, resp_text)
            return RequestsResults.model_validate_json(resp_text)

    async def stats(self, kbid: str, timeout: Optional[float] = 1.0) -> StatsResponse:
        url = self.base_url + "/stats"
        async with self.session.get(
            url,
            headers=self.headers,
            params={"kbid": kbid},
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as resp:
            resp_text = await resp.text()
            check_status(resp, resp_text)
            return StatsResponse.model_validate_json(resp_text)


class ProcessingPullMessageProgressUpdater(MessageProgressUpdater):
    """
    Context manager to send progress updates to NATS.

    This should allow lower ack_wait time settings without causing
    messages to be redelivered.
    """

    def __init__(self, client: ProcessingHTTPClient, msg: PulledMessage, timeout: float):
        async def update_msg() -> bool:
            await client.in_progress(msg.ack_token)
            return False

        super().__init__(str(msg.seq), update_msg, timeout)
