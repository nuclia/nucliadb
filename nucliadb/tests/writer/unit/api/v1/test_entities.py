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
from httpx import AsyncClient
from pytest_mock import MockerFixture

from nucliadb_models.entities import (
    CreateEntitiesGroupPayload,
    Entity,
    UpdateEntitiesGroupPayload,
)
from nucliadb_protos.writer_pb2 import (
    NewEntitiesGroupResponse,
    OpStatusWriter,
    UpdateEntitiesGroupResponse,
)
from tests.utils.entities import (
    create_entities_group,
    delete_entities_group,
    update_entities_group,
)


class TestEntitiesApi:
    @pytest.fixture
    def knowledgebox(self) -> str:
        """Fake kbid, as we are mocking everything related."""
        return uuid.uuid4().hex

    @pytest.fixture(autouse=True)
    async def ingest_mock(self, mocker: MockerFixture):
        mock = AsyncMock()
        mocker.patch("nucliadb.writer.api.v1.services.get_ingest", new=Mock(return_value=mock))
        yield mock

    @pytest.fixture
    def animals_entities_group(self) -> CreateEntitiesGroupPayload:
        return CreateEntitiesGroupPayload(
            group="ANIMALS",
            entities={
                "cat": Entity(value="cat", merged=False, represents=["domestic-cat"]),
                "domestic-cat": Entity(value="domestic-cat", merged=True),
                "dog": Entity(value="dog"),
                "bird": Entity(value="bird"),
            },
            title="Animals",
            color="black",
        )

    @pytest.fixture
    def animals_update(self) -> UpdateEntitiesGroupPayload:
        return UpdateEntitiesGroupPayload(
            add={"seal": Entity(value="seal")},
            update={"dog": Entity(value="updated-dog")},
            delete=["domestic-cat"],
        )

    @pytest.mark.deploy_modes("component")
    async def test_create_entities_group(
        self,
        nucliadb_writer: AsyncClient,
        knowledgebox: str,
        ingest_mock: AsyncMock,
        animals_entities_group: CreateEntitiesGroupPayload,
    ):
        ingest_mock.NewEntitiesGroup.return_value = NewEntitiesGroupResponse(
            status=NewEntitiesGroupResponse.Status.OK
        )
        resp = await create_entities_group(nucliadb_writer, knowledgebox, animals_entities_group)
        assert resp.status_code == 200

    @pytest.mark.deploy_modes("component")
    async def test_create_entities_group_that_already_exists(
        self,
        nucliadb_writer: AsyncClient,
        knowledgebox: str,
        ingest_mock: AsyncMock,
        animals_entities_group: CreateEntitiesGroupPayload,
    ):
        ingest_mock.NewEntitiesGroup.return_value = NewEntitiesGroupResponse(
            status=NewEntitiesGroupResponse.Status.ALREADY_EXISTS
        )
        resp = await create_entities_group(nucliadb_writer, knowledgebox, animals_entities_group)
        assert resp.status_code == 409

    @pytest.mark.deploy_modes("component")
    async def test_create_entities_group_kb_not_found(
        self,
        nucliadb_writer: AsyncClient,
        knowledgebox: str,
        ingest_mock: AsyncMock,
        animals_entities_group: CreateEntitiesGroupPayload,
    ):
        ingest_mock.NewEntitiesGroup.return_value = NewEntitiesGroupResponse(
            status=NewEntitiesGroupResponse.Status.KB_NOT_FOUND
        )
        resp = await create_entities_group(nucliadb_writer, knowledgebox, animals_entities_group)
        assert resp.status_code == 404

    @pytest.mark.deploy_modes("component")
    async def test_create_entities_group_ingest_error(
        self,
        nucliadb_writer: AsyncClient,
        knowledgebox: str,
        ingest_mock: AsyncMock,
        animals_entities_group: CreateEntitiesGroupPayload,
    ):
        ingest_mock.NewEntitiesGroup.return_value = NewEntitiesGroupResponse(
            status=NewEntitiesGroupResponse.Status.ERROR
        )
        resp = await create_entities_group(nucliadb_writer, knowledgebox, animals_entities_group)
        assert resp.status_code == 500

    @pytest.mark.deploy_modes("component")
    async def test_update_entities_group(
        self,
        nucliadb_writer: AsyncClient,
        knowledgebox: str,
        ingest_mock: AsyncMock,
        animals_update: UpdateEntitiesGroupPayload,
    ):
        ingest_mock.UpdateEntitiesGroup.return_value = UpdateEntitiesGroupResponse(
            status=UpdateEntitiesGroupResponse.Status.OK
        )
        resp = await update_entities_group(nucliadb_writer, knowledgebox, "ANIMALS", animals_update)
        assert resp.status_code == 200

    @pytest.mark.deploy_modes("component")
    async def test_update_entities_group_kb_not_found(
        self,
        nucliadb_writer: AsyncClient,
        knowledgebox: str,
        ingest_mock: AsyncMock,
        animals_update: UpdateEntitiesGroupPayload,
    ):
        ingest_mock.UpdateEntitiesGroup.return_value = UpdateEntitiesGroupResponse(
            status=UpdateEntitiesGroupResponse.Status.KB_NOT_FOUND
        )
        resp = await update_entities_group(nucliadb_writer, knowledgebox, "ANIMALS", animals_update)
        assert resp.status_code == 404

    @pytest.mark.deploy_modes("component")
    async def test_update_entities_group_entities_group_not_found(
        self,
        nucliadb_writer: AsyncClient,
        knowledgebox: str,
        ingest_mock: AsyncMock,
        animals_update: UpdateEntitiesGroupPayload,
    ):
        ingest_mock.UpdateEntitiesGroup.return_value = UpdateEntitiesGroupResponse(
            status=UpdateEntitiesGroupResponse.Status.ENTITIES_GROUP_NOT_FOUND
        )
        resp = await update_entities_group(nucliadb_writer, knowledgebox, "ANIMALS", animals_update)
        assert resp.status_code == 404

    @pytest.mark.deploy_modes("component")
    async def test_update_entities_group_ingest_error(
        self,
        nucliadb_writer: AsyncClient,
        knowledgebox: str,
        ingest_mock: AsyncMock,
        animals_update: UpdateEntitiesGroupPayload,
    ):
        ingest_mock.UpdateEntitiesGroup.return_value = UpdateEntitiesGroupResponse(
            status=UpdateEntitiesGroupResponse.Status.ERROR
        )
        resp = await update_entities_group(nucliadb_writer, knowledgebox, "ANIMALS", animals_update)
        assert resp.status_code == 500

    @pytest.mark.deploy_modes("component")
    async def test_delete_entities_group(
        self,
        nucliadb_writer: AsyncClient,
        knowledgebox: str,
        ingest_mock: AsyncMock,
    ):
        ingest_mock.DelEntities.return_value = OpStatusWriter(status=OpStatusWriter.Status.OK)
        resp = await delete_entities_group(nucliadb_writer, knowledgebox, "ANIMALS")
        assert resp.status_code == 200

    @pytest.mark.deploy_modes("component")
    async def test_delete_entities_group_kb_not_found(
        self,
        nucliadb_writer: AsyncClient,
        knowledgebox: str,
        ingest_mock: AsyncMock,
    ):
        ingest_mock.DelEntities.return_value = OpStatusWriter(status=OpStatusWriter.Status.NOTFOUND)
        resp = await delete_entities_group(nucliadb_writer, knowledgebox, "ANIMALS")
        assert resp.status_code == 404

    @pytest.mark.deploy_modes("component")
    async def test_delete_entities_group_ingest_error(
        self,
        nucliadb_writer: AsyncClient,
        knowledgebox: str,
        ingest_mock: AsyncMock,
    ):
        ingest_mock.DelEntities.return_value = OpStatusWriter(status=OpStatusWriter.Status.ERROR)
        resp = await delete_entities_group(nucliadb_writer, knowledgebox, "ANIMALS")
        assert resp.status_code == 500
