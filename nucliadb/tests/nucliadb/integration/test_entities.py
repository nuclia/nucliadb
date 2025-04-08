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

"""
There are three sources of entities:
- resource processing
- resource annotations
- entities API

Entities coming from resources are immutable in the index.

We'll test how all this combinations interact to provide a custom entities view
for users.

"""

import pytest
from httpx import AsyncClient

from nucliadb_models.entities import (
    CreateEntitiesGroupPayload,
    Entity,
    UpdateEntitiesGroupPayload,
)
from nucliadb_protos.resources_pb2 import (
    Relation,
    RelationNode,
    Relations,
)
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import broker_resource, inject_message
from tests.utils.dirty_index import wait_for_sync
from tests.utils.entities import (
    create_entities_group,
    delete_entities_group,
    update_entities_group,
    wait_until_entity,
)


@pytest.fixture
async def text_field(
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox
    field_id = "text-field"

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "slug": "entities-test-resource",
            "title": "E2E entities test resource",
            "texts": {
                field_id: {
                    "body": "A dog is an animal and a bird is another one",
                    "format": "PLAIN",
                }
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    yield (kbid, rid, field_id)

    resp = await nucliadb_writer.delete(f"/kb/{kbid}/resource/{rid}")
    assert resp.status_code == 204


@pytest.fixture
async def processing_entities(nucliadb_ingest_grpc: WriterStub, standalone_knowledgebox: str):
    entities = {
        "cat": {"value": "cat"},
        "dolphin": {"value": "dolphin"},
    }
    bm = broker_resource(
        standalone_knowledgebox, slug="automatic-entities", source=BrokerMessage.MessageSource.WRITER
    )
    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()

    relations = []

    bm.source = BrokerMessage.MessageSource.PROCESSOR
    for entity in entities.values():
        node = RelationNode(
            value=entity["value"],
            ntype=RelationNode.NodeType.ENTITY,
            subtype="ANIMALS",
        )
        relations.append(
            Relation(
                relation=Relation.RelationType.ENTITY,
                source=node,
                to=node,
                relation_label="itself",
            )
        )

    bm.field_metadata[0].metadata.metadata.relations.append(Relations(relations=relations))
    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()


@pytest.fixture
async def user_entities(nucliadb_writer: AsyncClient, standalone_knowledgebox: str):
    await wait_for_sync()
    payload = CreateEntitiesGroupPayload(
        group="ANIMALS",
        entities={
            "cat": Entity(value="cat", merged=False, represents=["domestic-cat"]),
            "domestic-cat": Entity(value="domestic cat", merged=True),
            "dog": Entity(value="dog"),
        },
        title="Animals",
        color="black",
    )
    resp = await create_entities_group(nucliadb_writer, standalone_knowledgebox, payload)
    assert resp.status_code == 200


@pytest.fixture
async def entities(
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
    user_entities,
    processing_entities,
):
    """Single fixture to get entities injected in different ways."""
    # Ensure entities are properly stored/indexed
    await wait_until_entity(nucliadb_ingest_grpc, standalone_knowledgebox, "ANIMALS", "cat")
    await wait_until_entity(nucliadb_ingest_grpc, standalone_knowledgebox, "ANIMALS", "dog")
    await wait_until_entity(nucliadb_ingest_grpc, standalone_knowledgebox, "ANIMALS", "domestic-cat")
    await wait_until_entity(nucliadb_ingest_grpc, standalone_knowledgebox, "ANIMALS", "dolphin")


@pytest.mark.deploy_modes("standalone")
async def test_get_entities_groups(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    entities,
):
    kbid = standalone_knowledgebox

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroup/ANIMALS")
    assert resp.status_code == 200
    body = resp.json()

    assert body["entities"].keys() == {
        "cat",
        "dog",
        "dolphin",
        "domestic-cat",
    }
    assert body["entities"]["cat"]["merged"] is False
    assert body["entities"]["cat"]["represents"] == ["domestic-cat"]
    assert body["entities"]["domestic-cat"]["merged"] is True
    assert body["entities"]["domestic-cat"]["represents"] == []

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroup/I-DO-NOT-EXIST")
    assert resp.status_code == 404
    body = resp.json()
    assert body["detail"] == "Entities group 'I-DO-NOT-EXIST' does not exist"


@pytest.mark.deploy_modes("standalone")
async def test_list_entities_groups(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    entities,
):
    kbid = standalone_knowledgebox

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroups?show_entities=false")
    assert resp.status_code == 200
    body = resp.json()

    assert body["groups"].keys() == {"ANIMALS"}
    assert "entities" in body["groups"]["ANIMALS"]
    assert len(body["groups"]["ANIMALS"]["entities"]) == 0


@pytest.mark.deploy_modes("standalone")
async def test_create_entities_group_twice(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    entities,
):
    kbid = standalone_knowledgebox

    payload = CreateEntitiesGroupPayload(
        group="ANIMALS",
        entities={"lion": Entity(value="lion"), "quokka": Entity(value="quokka")},
        title="Animals",
        color="orange",
    )
    resp = await create_entities_group(nucliadb_writer, kbid, payload)
    assert resp.status_code == 409


@pytest.mark.deploy_modes("standalone")
async def test_update_entities_group(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    entities,
):
    kbid = standalone_knowledgebox

    update = UpdateEntitiesGroupPayload(
        add={"seal": Entity(value="seal")},
        update={"dog": Entity(value="updated-dog")},
        delete=["domestic-cat", "dolphin"],
    )
    resp = await update_entities_group(nucliadb_writer, kbid, "ANIMALS", update)
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroup/ANIMALS")
    assert resp.status_code == 200
    body = resp.json()

    assert body["entities"].keys() == {
        "cat",
        "dog",
        "seal",
    }
    assert body["entities"]["dog"]["value"] == "updated-dog"


@pytest.mark.deploy_modes("standalone")
async def test_update_indexed_entities_group(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    processing_entities,
):
    kbid = standalone_knowledgebox

    update = UpdateEntitiesGroupPayload(
        add={"seal": Entity(value="seal")},
        update={"dolphin": Entity(value="updated-dolphin")},
        delete=["cat"],
    )
    resp = await update_entities_group(nucliadb_writer, kbid, "ANIMALS", update)
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroup/ANIMALS")
    assert resp.status_code == 200
    body = resp.json()

    assert body["entities"].keys() == {
        "dolphin",
        "seal",
    }
    assert body["entities"]["dolphin"]["value"] == "updated-dolphin"


@pytest.mark.deploy_modes("standalone")
async def test_update_entities_group_metadata(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    entities,
):
    kbid = standalone_knowledgebox

    update = UpdateEntitiesGroupPayload(
        title="Updated Animals",
        color="red",
    )
    resp = await update_entities_group(nucliadb_writer, kbid, "ANIMALS", update)
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroup/ANIMALS")
    assert resp.status_code == 200
    body = resp.json()

    assert body["title"] == "Updated Animals"
    assert body["color"] == "red"


@pytest.mark.deploy_modes("standalone")
async def test_delete_entities_group(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    entities,
):
    kbid = standalone_knowledgebox

    resp = await delete_entities_group(nucliadb_writer, kbid, "ANIMALS")
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroup/ANIMALS")
    assert resp.status_code == 404


@pytest.mark.deploy_modes("standalone")
async def test_delete_and_recreate_entities_group(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    user_entities,
):
    kbid = standalone_knowledgebox

    resp = await delete_entities_group(nucliadb_writer, kbid, "ANIMALS")
    assert resp.status_code == 200

    payload = CreateEntitiesGroupPayload(
        group="ANIMALS",
        entities={"gecko": Entity(value="gecko")},
        title="Animals",
        color="white",
    )
    resp = await create_entities_group(nucliadb_writer, standalone_knowledgebox, payload)
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroup/ANIMALS")
    assert resp.status_code == 200
    body = resp.json()
    assert body["entities"].keys() == {"gecko"}
    assert body["color"] == "white"


@pytest.mark.deploy_modes("standalone")
async def test_entities_indexing(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    entities,
    predict_mock,
):
    kbid = standalone_knowledgebox

    async def suggested_entities(query: str) -> list[str]:
        resp = await nucliadb_reader.get(
            f"/kb/{kbid}/suggest",
            params={
                "query": query,
                "features": ["entities"],
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        entities = set((e["value"] for e in body["entities"]["entities"]))
        return list(entities)

    entities = await suggested_entities("do")
    # Only the processing entities are indexed
    assert "dog" not in entities
    assert "dolphin" in entities
    entities = await suggested_entities("ca")
    assert "cat" in entities
    assert "domestic-cat" not in entities
