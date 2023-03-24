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
import asyncio
import time

from httpx import AsyncClient
from nucliadb_protos.knowledgebox_pb2 import KnowledgeBoxID
from nucliadb_protos.writer_pb2 import GetEntitiesGroupRequest, GetEntitiesGroupResponse
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.writer.api.v1.router import KB_PREFIX
from nucliadb_models.entities import (
    CreateEntitiesGroupPayload,
    UpdateEntitiesGroupPayload,
)


async def create_entities_group(
    writer: AsyncClient, kbid: str, payload: CreateEntitiesGroupPayload
):
    resp = await writer.post(
        f"/{KB_PREFIX}/{kbid}/entitiesgroups",
        content=payload.json(),
    )
    return resp


async def update_entities_group(
    writer: AsyncClient,
    kbid: str,
    group: str,
    payload: UpdateEntitiesGroupPayload,
):
    resp = await writer.patch(
        f"/{KB_PREFIX}/{kbid}/entitiesgroup/{group}",
        content=payload.json(),
    )
    return resp


async def delete_entities_group(writer: AsyncClient, kbid: str, group: str):
    resp = await writer.delete(f"/{KB_PREFIX}/{kbid}/entitiesgroup/{group}")
    return resp


async def wait_until_entity(
    ingest: WriterStub, kbid: str, group: str, entity: str, timeout: float = 1.0
):
    start = time.time()
    found = False
    while not found:
        response: GetEntitiesGroupResponse = await ingest.GetEntitiesGroup(  # type: ignore
            GetEntitiesGroupRequest(kb=KnowledgeBoxID(uuid=kbid), group=group)
        )
        found = entity in response.group.entities
        assert (
            time.time() - start < timeout
        ), "Timeout while waiting for entity {group}/{entity}"

        if not found:
            await asyncio.sleep(0.1)
