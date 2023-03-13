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
import uuid
from unittest.mock import AsyncMock, Mock

import pytest
from nucliadb_protos.train_pb2 import GetEntitiesRequest
from nucliadb_protos.train_pb2_grpc import TrainStub
from nucliadb_protos.writer_pb2 import GetEntitiesResponse


@pytest.fixture(scope="function")
async def entities_manager_mock():
    from nucliadb.train import nodes

    original = nodes.EntitiesManager

    mock = Mock()
    nodes.EntitiesManager = Mock(return_value=mock)

    yield mock

    nodes.EntitiesManager = original


@pytest.mark.asyncio
async def test_get_entities(
    train_client: TrainStub,
    knowledgebox_ingest: str,
    entities_manager_mock: Mock,
) -> None:
    def get_entities_mock(response):
        response.groups["group1"].entities["entity1"].value = "PERSON"

    entities_manager_mock.get_entities = AsyncMock(side_effect=get_entities_mock)

    req = GetEntitiesRequest()
    req.kb.uuid = knowledgebox_ingest
    entities: GetEntitiesResponse = await train_client.GetEntities(req)  # type: ignore

    assert entities.groups["group1"].entities["entity1"].value == "PERSON"


@pytest.mark.asyncio
async def test_get_entities_kb_not_found(train_client: TrainStub) -> None:
    req = GetEntitiesRequest()
    req.kb.uuid = str(uuid.uuid4())
    entities: GetEntitiesResponse = await train_client.GetEntities(req)  # type: ignore
    assert entities.status == GetEntitiesResponse.Status.NOTFOUND


@pytest.mark.asyncio
async def test_get_entities_error(
    train_client: TrainStub, knowledgebox_ingest: str, entities_manager_mock
) -> None:
    entities_manager_mock.get_entities = AsyncMock(
        side_effect=Exception("Testing exception on ingest")
    )

    req = GetEntitiesRequest()
    req.kb.uuid = knowledgebox_ingest
    entities: GetEntitiesResponse = await train_client.GetEntities(req)  # type: ignore
    assert entities.status == GetEntitiesResponse.Status.ERROR
