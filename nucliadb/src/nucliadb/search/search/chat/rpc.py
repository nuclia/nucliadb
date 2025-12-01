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
from contextlib import asynccontextmanager
from typing import AsyncIterator

from httpx import AsyncClient
from opentelemetry import trace
from opentelemetry.trace import INVALID_SPAN, format_trace_id

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.models.internal.retrieval import RetrievalRequest, RetrievalResponse
from nucliadb.search import API_PREFIX
from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb.search.search.metrics import Metrics
from nucliadb_models.augment import AugmentRequest, AugmentResponse
from nucliadb_models.search import FindRequest, KnowledgeboxFindResults, NucliaDBClientType
from nucliadb_telemetry.fastapi.tracing import NUCLIA_TRACE_ID_HEADER
from nucliadb_utils.settings import running_settings


# TODO: replace this for a sdk.find call when moving /ask to RAO
async def find(
    kbid: str,
    item: FindRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    # XXX: we are losing track of metrics ignoring this. Do we care?
    metrics: Metrics,
) -> tuple[KnowledgeboxFindResults, bool]:
    """RPC to /find endpoint making it look as an internal call."""

    async with get_client() as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{kbid}/find",
            headers={
                "x-ndb-client": x_ndb_client,
                "x-nucliadb-user": x_nucliadb_user,
                "x-forwarded-for": x_forwarded_for,
            },
            json=item.model_dump(),
        )
        if resp.status_code == 200:
            incomplete = False
        elif resp.status_code == 206:
            incomplete = True
        elif resp.status_code == 404:
            raise KnowledgeBoxNotFound()
        else:
            raise Exception(f"/find call failed: {resp.status_code} {resp.content.decode()}")

        find_results = KnowledgeboxFindResults.model_validate(resp.json())

    return find_results, incomplete


# TODO: replace this for a sdk.retrieve call when moving /ask to RAO
async def retrieve(
    kbid: str,
    item: RetrievalRequest,
    *,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
) -> RetrievalResponse:
    """RPC to /augment endpoint making it look as an internal call."""

    payload = item.model_dump()
    async with get_client() as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{kbid}/retrieve",
            headers={
                "x-ndb-client": x_ndb_client,
                "x-nucliadb-user": x_nucliadb_user,
                "x-forwarded-for": x_forwarded_for,
            },
            json=payload,
        )
        if resp.status_code != 200:
            raise Exception(f"/retrieve call failed: {resp.status_code} {resp.content.decode()}")

        retrieved = RetrievalResponse.model_validate(resp.json())

    return retrieved


# TODO: replace this for a sdk.augment call when moving /ask to RAO
async def augment(kbid: str, item: AugmentRequest) -> AugmentResponse:
    """RPC to /augment endpoint making it look as an internal call."""

    payload = item.model_dump()
    async with get_client() as client:
        resp = await client.post(f"/{KB_PREFIX}/{kbid}/augment", json=payload)
        if resp.status_code != 200:
            raise Exception(f"/augment call failed: {resp.status_code} {resp.content.decode()}")

        augmented = AugmentResponse.model_validate(resp.json())

    return augmented


@asynccontextmanager
async def get_client() -> AsyncIterator[AsyncClient]:
    async with AsyncClient(
        headers={"X-NUCLIADB-ROLES": "READER", **__get_tracing_headers()},
        base_url=f"http://{running_settings.serving_host}:{running_settings.serving_port}/{API_PREFIX}/v1",
        timeout=10.0,
    ) as client:
        yield client


def __get_tracing_headers() -> dict[str, str]:
    headers = {}

    span = trace.get_current_span()
    if span is INVALID_SPAN:
        return {}

    trace_id = format_trace_id(span.get_span_context().trace_id)
    headers = {
        NUCLIA_TRACE_ID_HEADER: trace_id,
    }
    return headers
