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

from nucliadb.models.entities import EntitiesGroup, Entity
from nucliadb.models.labels import Label, LabelSet
from nucliadb.models.resource import NucliaDBRoles
from nucliadb.writer.api.v1.router import KB_PREFIX, KBS_PREFIX
from nucliadb_protos import knowledgebox_pb2, writer_pb2
from nucliadb_utils.utilities import get_ingest


@pytest.mark.asyncio
async def test_service_lifecycle_entities(writer_api):
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
        eg = EntitiesGroup()
        eg.title = "My group"
        eg.color = "#0000000"
        eg.entities["ent1"] = Entity(value="asd", merged=False)
        eg.entities["ent2"] = Entity(value="asd", merged=False)
        eg.entities["ent3"] = Entity(value="asd", merged=False)

        resp = await client.post(f"/{KB_PREFIX}/{kbid}/entitiesgroup/0", json=eg.dict())
        assert resp.status_code == 200

        ingest = get_ingest()
        result = await ingest.GetEntities(
            writer_pb2.GetEntitiesRequest(kb=knowledgebox_pb2.KnowledgeBoxID(uuid=kbid))
        )
        assert set(result.groups.keys()) == {"0"}
        assert result.groups["0"].title == eg.title
        assert result.groups["0"].color == eg.color
        assert set(result.groups["0"].entities.keys()) == {"ent3", "ent1", "ent2"}
        assert result.groups["0"].entities["ent1"].value == "asd"

        resp = await client.post(f"/{KB_PREFIX}/{kbid}/entitiesgroup/1", json=eg.dict())
        assert resp.status_code == 200
        result = await ingest.GetEntities(
            writer_pb2.GetEntitiesRequest(kb=knowledgebox_pb2.KnowledgeBoxID(uuid=kbid))
        )
        assert set(result.groups.keys()) == {"0", "1"}


@pytest.mark.asyncio
async def test_entities_custom_field(writer_api):
    """
    Test description:

    - Create an entity group and check that the default value for the `custom`
      field is False

    - Create another entity group and set `custom` to True. Check that on a
      subsequent get, the value is correct.
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
        eg = EntitiesGroup()
        custom_eg = EntitiesGroup(custom=True)

        resp = await client.post(f"/{KB_PREFIX}/{kbid}/entitiesgroup/0", json=eg.dict())
        assert resp.status_code == 200

        resp = await client.post(
            f"/{KB_PREFIX}/{kbid}/entitiesgroup/1", json=custom_eg.dict()
        )
        assert resp.status_code == 200

        ingest = get_ingest()
        result = await ingest.GetEntities(
            writer_pb2.GetEntitiesRequest(kb=knowledgebox_pb2.KnowledgeBoxID(uuid=kbid))
        )
        assert result.groups["0"].custom is False
        assert result.groups["1"].custom is True


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
