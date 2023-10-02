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
import pytest

from nucliadb.writer.api.v1.router import KB_PREFIX, KBS_PREFIX
from nucliadb_models.configuration import KBConfiguration
from nucliadb_models.entities import CreateEntitiesGroupPayload, Entity
from nucliadb_models.labels import Label, LabelSet
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_protos import knowledgebox_pb2, writer_pb2
from nucliadb_utils.utilities import get_ingest


@pytest.mark.asyncio
async def test_service_lifecycle_entities(writer_api, entities_manager_mock):
    async with writer_api(roles=[NucliaDBRoles.MANAGER]) as client:
        resp = await client.post(
            f"/{KBS_PREFIX}",
            json={
                "slug": "kbid1",
                "title": "My Knowledge Box",
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["slug"] == "kbid1"
        kbid = data["uuid"]

    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        eg = CreateEntitiesGroupPayload(
            group="0",
            title="My group",
            color="#0000000",
            entities={
                "ent1": Entity(value="asd", merged=False),
                "ent2": Entity(value="asd", merged=False),
                "ent3": Entity(value="asd", merged=False),
            },
        )

        resp = await client.post(f"/{KB_PREFIX}/{kbid}/entitiesgroups", json=eg.dict())
        assert resp.status_code == 200

        ingest = get_ingest()
        result = await ingest.GetEntities(
            writer_pb2.GetEntitiesRequest(kb=knowledgebox_pb2.KnowledgeBoxID(uuid=kbid))
        )
        assert set(result.groups.keys()) == {"0"}
        assert result.groups["0"].title == eg.title
        assert result.groups["0"].color == eg.color
        assert set(result.groups["0"].entities.keys()) == {"ent1", "ent2", "ent3"}
        assert result.groups["0"].entities["ent1"].value == "asd"

        eg.group = "1"
        resp = await client.post(f"/{KB_PREFIX}/{kbid}/entitiesgroups", json=eg.dict())
        assert resp.status_code == 200
        result = await ingest.GetEntities(
            writer_pb2.GetEntitiesRequest(kb=knowledgebox_pb2.KnowledgeBoxID(uuid=kbid))
        )
        assert set(result.groups.keys()) == {"0", "1"}


@pytest.mark.asyncio
async def test_entities_custom_field_for_user_defined_groups(
    writer_api, entities_manager_mock
):
    """
    Test description:

    - Create an entity group and check that the default value for the `custom`
      field is True
    """
    async with writer_api(roles=[NucliaDBRoles.MANAGER]) as client:
        resp = await client.post(
            f"/{KBS_PREFIX}",
            json={
                "slug": "kbid1",
                "title": "My Knowledge Box",
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        kbid = data["uuid"]

    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        eg = CreateEntitiesGroupPayload(group="0")
        resp = await client.post(f"/{KB_PREFIX}/{kbid}/entitiesgroups", json=eg.dict())
        assert resp.status_code == 200

        ingest = get_ingest()
        result = await ingest.GetEntities(
            writer_pb2.GetEntitiesRequest(kb=knowledgebox_pb2.KnowledgeBoxID(uuid=kbid))
        )
        assert result.groups["0"].custom is True


@pytest.mark.asyncio
async def test_service_lifecycle_labels(writer_api):
    async with writer_api(roles=[NucliaDBRoles.MANAGER]) as client:
        resp = await client.post(
            f"/{KBS_PREFIX}",
            json={
                "slug": "kbid1",
                "title": "My Knowledge Box",
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["slug"] == "kbid1"
        kbid = data["uuid"]

    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        ls = LabelSet(
            title="My labelset", color="#0000000", multiple=False, kind=["RESOURCES"]
        )
        ls.labels.append(Label(title="asd"))
        ls.labels.append(Label(title="asd"))
        resp = await client.post(f"/{KB_PREFIX}/{kbid}/labelset/ls1", json=ls.dict())
        assert resp.status_code == 200
        resp = await client.post(f"/{KB_PREFIX}/{kbid}/labelset/ls2", json=ls.dict())
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_service_lifecycle_configuration(writer_api):
    async with writer_api(roles=[NucliaDBRoles.MANAGER]) as client:
        resp = await client.post(
            f"/{KBS_PREFIX}",
            json={
                "slug": "kbid1",
                "title": "My Knowledge Box",
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["slug"] == "kbid1"
        kbid = data["uuid"]

    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        conf = KBConfiguration(semantic_model="test")
        resp = await client.post(f"/{KB_PREFIX}/{kbid}/configuration", json=conf.dict())
        assert resp.status_code == 200
