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
from httpx import AsyncClient

from nucliadb.writer.api.v1.router import KB_PREFIX
from nucliadb_models.entities import CreateEntitiesGroupPayload, Entity
from nucliadb_models.labels import Label, LabelSet, LabelSetKind
from nucliadb_protos import knowledgebox_pb2, writer_pb2
from nucliadb_utils.utilities import get_ingest


@pytest.mark.deploy_modes("component")
async def test_service_lifecycle_entities(
    nucliadb_writer: AsyncClient, knowledgebox: str, entities_manager_mock
):
    kbid = knowledgebox

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

    resp = await nucliadb_writer.post(f"/{KB_PREFIX}/{kbid}/entitiesgroups", json=eg.model_dump())
    assert resp.status_code == 200

    ingest = get_ingest()
    result = await ingest.GetEntities(  # type: ignore
        writer_pb2.GetEntitiesRequest(kb=knowledgebox_pb2.KnowledgeBoxID(uuid=kbid))
    )
    assert set(result.groups.keys()) == {"0"}
    assert result.groups["0"].title == eg.title
    assert result.groups["0"].color == eg.color
    assert set(result.groups["0"].entities.keys()) == {"ent1", "ent2", "ent3"}
    assert result.groups["0"].entities["ent1"].value == "asd"

    eg.group = "1"
    resp = await nucliadb_writer.post(f"/{KB_PREFIX}/{kbid}/entitiesgroups", json=eg.model_dump())
    assert resp.status_code == 200
    result = await ingest.GetEntities(  # type: ignore
        writer_pb2.GetEntitiesRequest(kb=knowledgebox_pb2.KnowledgeBoxID(uuid=kbid))
    )
    assert set(result.groups.keys()) == {"0", "1"}


@pytest.mark.deploy_modes("component")
async def test_entities_custom_field_for_user_defined_groups(
    nucliadb_writer: AsyncClient, knowledgebox: str, entities_manager_mock
):
    """
    Test description:

    - Create an entity group and check that the default value for the `custom`
      field is True
    """
    kbid = knowledgebox

    eg = CreateEntitiesGroupPayload(group="0")
    resp = await nucliadb_writer.post(f"/{KB_PREFIX}/{kbid}/entitiesgroups", json=eg.model_dump())
    assert resp.status_code == 200

    ingest = get_ingest()
    result = await ingest.GetEntities(  # type: ignore
        writer_pb2.GetEntitiesRequest(kb=knowledgebox_pb2.KnowledgeBoxID(uuid=kbid))
    )
    assert result.groups["0"].custom is True


@pytest.mark.deploy_modes("component")
async def test_service_lifecycle_labels(nucliadb_writer: AsyncClient, knowledgebox: str):
    kbid = knowledgebox

    ls = LabelSet(title="My labelset", color="#0000000", multiple=False, kind=[LabelSetKind.RESOURCES])
    ls.labels.append(Label(title="asd"))
    ls.labels.append(Label(title="fgh"))

    resp = await nucliadb_writer.post(f"/{KB_PREFIX}/{kbid}/labelset/ls1", json=ls.model_dump())
    assert resp.status_code == 200

    resp = await nucliadb_writer.post(f"/{KB_PREFIX}/{kbid}/labelset/ls2", json=ls.model_dump())
    assert resp.status_code == 422

    ls.title = "My labelset 2"
    resp = await nucliadb_writer.post(f"/{KB_PREFIX}/{kbid}/labelset/ls2", json=ls.model_dump())
    assert resp.status_code == 200
