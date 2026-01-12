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
import base64
from contextlib import asynccontextmanager
from typing import AsyncIterator

from httpx import AsyncClient

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.common.ids import FieldId
from nucliadb.search import API_PREFIX
from nucliadb.search.api.v1.router import KB_PREFIX, RESOURCE_PREFIX
from nucliadb.search.search.metrics import Metrics
from nucliadb_models.augment import AugmentRequest, AugmentResponse
from nucliadb_models.labels import KnowledgeBoxLabels
from nucliadb_models.retrieval import RetrievalRequest, RetrievalResponse
from nucliadb_models.search import (
    FindRequest,
    Image,
    KnowledgeboxFindResults,
    NucliaDBClientType,
)
from nucliadb_utils.settings import running_settings


# TODO(decoupled-ask): replace this for a sdk.find call when moving /ask to RAO
async def find(
    kbid: str,
    item: FindRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    # REVIEW(decoupled-ask): once in an SDK metrics, we'll lose track of metrics
    metrics: Metrics,
) -> tuple[KnowledgeboxFindResults, bool]:
    """RPC to /find endpoint making it look as an internal call."""

    async with get_client("search") as client:
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


# TODO(decoupled-ask): replace this for a sdk.retrieve call when moving /ask to RAO
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
    async with get_client("search") as client:
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


# TODO(decoupled-ask): replace this for a sdk.augment call when moving /ask to RAO
async def augment(kbid: str, item: AugmentRequest) -> AugmentResponse:
    """RPC to /augment endpoint making it look as an internal call."""

    payload = item.model_dump()
    async with get_client("search") as client:
        resp = await client.post(f"/{KB_PREFIX}/{kbid}/augment", json=payload)
        if resp.status_code != 200:
            raise Exception(f"/augment call failed: {resp.status_code} {resp.content.decode()}")

        augmented = AugmentResponse.model_validate(resp.json())

    return augmented


# TODO(decoupled-ask): replace this for a sdk.labelsets call when moving /ask to RAO
async def labelsets(kbid: str) -> KnowledgeBoxLabels:
    async with get_client("reader") as client:
        resp = await client.get(f"/{KB_PREFIX}/{kbid}/labelsets")
        if resp.status_code != 200:
            raise Exception(f"/labelsets call failed: {resp.status_code} {resp.content.decode()}")

        labelsets = KnowledgeBoxLabels.model_validate(resp.json())

    return labelsets


# TODO(decoupled-ask): replace this for a sdk.download call when moving /ask to RAO
async def download_image(kbid: str, field_id: FieldId, path: str, *, mime_type: str) -> Image | None:
    async with get_client("reader") as client:
        async with client.stream(
            "GET",
            f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{field_id.rid}/{field_id.type_name.value}/{field_id.key}/download/extracted/{path}",
        ) as resp:
            data = await resp.aread()
            return Image(
                b64encoded=base64.b64encode(data).decode(),
                content_type=mime_type,
            )


@asynccontextmanager
async def get_client(_service: str) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(
        headers={"X-NUCLIADB-ROLES": "READER"},
        base_url=f"http://{running_settings.serving_host}:{running_settings.serving_port}/{API_PREFIX}/v1",
        timeout=10.0,
    ) as client:
        yield client
