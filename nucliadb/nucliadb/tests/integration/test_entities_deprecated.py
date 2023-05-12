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
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest
from httpx import AsyncClient
from nucliadb_protos.resources_pb2 import Relation, RelationNode
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.tests.utils import broker_resource, inject_message

pytestmark = pytest.mark.asyncio


TEST_API_ENTITIES_GROUPS = {
    "ANIMALS": {
        "title": "Animals",
        "entities": {
            "cat": {"value": "cat", "merged": False, "represents": ["domestic-cat"]},
            "domestic-cat": {"value": "domestic cat", "merged": True, "represents": []},
            "dog": {"value": "dog", "merged": False, "represents": []},
            "bird": {"value": "bird", "merged": False, "represents": []},
        },
        "color": "black",
        "custom": True,
    },
    "DESSERTS": {
        "title": "Desserts",
        "entities": {
            "cookie": {"value": "cookie", "represents": ["biscuit"]},
            "biscuit": {"value": "biscuit", "merged": True},
            "brownie": {"value": "brownie"},
            "souffle": {"value": "souffle"},
        },
        "color": "pink",
    },
}

TEST_BM_ENTITIES_GROUPS = {
    "ANIMALS": {
        "entities": {
            "cat": {"value": "cat"},
            "dolphin": {"value": "dolphin"},
        }
    },
    "SUPERPOWERS": {
        "entities": {
            "fly": {"value": "fly"},
            "invisibility": {"value": "invisibility"},
            "telepathy": {"value": "telepathy"},
        }
    },
}


async def create_entities_group_by_api(
    kbid: str, group: str, nucliadb_writer: AsyncClient
):
    entities_group = TEST_API_ENTITIES_GROUPS[group]
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/entitiesgroup/{group}",
        headers={"X-Synchronous": "true"},
        json=entities_group,
    )
    assert resp.status_code == 200
    return entities_group


async def create_entities_group_by_bm(kbid: str, group: str, nucliadb_grpc: WriterStub):
    entities_group = TEST_BM_ENTITIES_GROUPS[group]["entities"]
    bm = broker_resource(
        kbid,
        slug=f"resource-with-{group}-entities",
        title=f"Resource with {group} entities",
    )

    relations = []
    for entity in entities_group.values():
        node = RelationNode(
            value=entity["value"],
            ntype=RelationNode.NodeType.ENTITY,
            subtype=group,
        )
        relations.append(
            Relation(
                relation=Relation.RelationType.ENTITY,
                source=node,
                to=node,
                relation_label="itself",
            )
        )
    bm.relations.extend(relations)

    await inject_message(nucliadb_grpc, bm)
    return entities_group


@pytest.fixture(scope="function")
async def kb_with_entities(
    nucliadb_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    await create_entities_group_by_api(knowledgebox, "ANIMALS", nucliadb_writer)
    await create_entities_group_by_api(knowledgebox, "DESSERTS", nucliadb_writer)
    await create_entities_group_by_bm(knowledgebox, "ANIMALS", nucliadb_grpc)
    await create_entities_group_by_bm(knowledgebox, "SUPERPOWERS", nucliadb_grpc)

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        headers={"X-SYNCHRONOUS": "true"},
        json={
            "title": "Wait all is processed",
            "slug": "wait-all-is-processed",
            "summary": "Dummy resource with x-synchronous enabled to wait BM indexing",
        },
    )
    assert resp.status_code == 201

    bm_indexed = False
    iterations = 0
    while not bm_indexed:
        resp = await nucliadb_reader.get(
            f"/kb/{knowledgebox}/suggest",
            params={
                "query": "invisibility",
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        bm_indexed = len(body["entities"]["entities"]) > 0
        # small sleep to give time for indexing
        await asyncio.sleep(0.1)
        iterations += 1

        assert (
            iterations < 10
        ), "Entity indexing lasted too much, might be an error on the test"

    yield knowledgebox


async def test_crud_entities_group_via_api(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
    predict_mock,
):
    kbid = knowledgebox

    entities_group = await create_entities_group_by_api(
        kbid, "ANIMALS", nucliadb_writer
    )
    entities_group = deepcopy(entities_group)

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/entitiesgroup/ANIMALS",
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body == entities_group

    entities_group["entities"]["house-cat"] = {
        "value": "house cat",
        "merged": True,
        "represents": [],
    }
    entities_group["entities"]["cat"]["represents"].append("house-cat")
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/entitiesgroup/ANIMALS", json=entities_group
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/entitiesgroup/ANIMALS",
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body == entities_group

    resp = await nucliadb_writer.delete(
        f"/kb/{kbid}/entitiesgroup/ANIMALS",
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/entitiesgroup/ANIMALS",
    )
    assert resp.status_code == 404


async def test_entities_store_and_indexing(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    kb_with_entities,
    predict_mock,
):
    """Store and index some entities via different sources and validate correct
    storage and indexing.

    """
    kbid = kb_with_entities

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroups?show_entities=true")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body["groups"].keys()) == {"ANIMALS", "DESSERTS", "SUPERPOWERS"}

    predict_mock.detect_entities = AsyncMock(
        return_value=[
            RelationNode(
                value="cat",
                ntype=RelationNode.NodeType.ENTITY,
                subtype="ANIMALS",
            ),
            RelationNode(
                value="cookie",
                ntype=RelationNode.NodeType.ENTITY,
                subtype="DESSERTS",
            ),
            RelationNode(
                value="invisibility",
                ntype=RelationNode.NodeType.ENTITY,
                subtype="SUPERPOWERS",
            ),
        ]
    )

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/search",
        params={
            "query": "does your cat prefer eat cookies or have invisibility?",
            "features": "relations",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    entities = {
        related_entity["entity"]
        for entity in body["relations"]["entities"]
        for related_entity in body["relations"]["entities"][entity]["related_to"]
    }
    # "cookie" is not found because don't have relation with anything and
    # currently the index is not indexing them. Relations by BM have a fake
    # relations with themselves for sake of testing.
    assert entities == {"cat", "invisibility"}


async def test_get_entities(
    nucliadb_reader: AsyncClient,
    kb_with_entities,
):
    """Validate retrieval of entities coming from different sources."""
    kbid = kb_with_entities

    expected = {
        "ANIMALS": {"cat", "domestic-cat", "dog", "bird", "dolphin"},
        "DESSERTS": {"cookie", "biscuit", "brownie", "souffle"},
        "SUPERPOWERS": {"fly", "invisibility", "telepathy"},
    }

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroups?show_entities=true")
    assert resp.status_code == 200
    entitiesgroups = resp.json()
    assert len(entitiesgroups["groups"]) == len(expected)
    for group in entitiesgroups["groups"]:
        eg = entitiesgroups["groups"][group]
        assert set(eg["entities"].keys()) == expected[group]

    for group in expected:
        resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroup/{group}")
        assert resp.status_code == 200
        body = resp.json()
        entities = {entity for entity in body["entities"]}
        assert entities == expected[group]


async def test_modify_entities_group(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    kb_with_entities,
):
    """Modify ANIMALS entities group which is set by API and BM to test setting
    entities by API "overwrites" whatever is stored

    """
    kbid = kb_with_entities

    animals = {
        "title": "Animals",
        "entities": {
            "bear": {"value": "bear", "represents": [], "merged": False},
            "hamster": {"value": "hamster", "represents": [], "merged": False},
            "seal": {"value": "seal", "represents": [], "merged": False},
        },
        "color": "pink",
        "custom": False,
    }

    resp = await nucliadb_writer.post(f"/kb/{kbid}/entitiesgroup/ANIMALS", json=animals)
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroup/ANIMALS")
    assert resp.status_code == 200
    body = resp.json()
    assert body == animals


async def test_delete_non_existent_entities_group(
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    kbid = knowledgebox

    resp = await nucliadb_writer.delete(f"/kb/{kbid}/entitiesgroup/nonexistent")
    assert resp.status_code == 200


async def test_delete_entities_groups(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    kb_with_entities,
):
    """Validate deletion of entities groups coming from different sources."""
    kbid = kb_with_entities

    entitiesgroups = [
        "DESSERTS",  # added with API
        "SUPERPOWERS",  # added with broker message
        "ANIMALS",  # added with API and broker message
    ]

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroups?show_entities=true")
    assert resp.status_code == 200
    body = resp.json()
    assert set(entitiesgroups) == set(body["groups"])

    for entitiesgroup in entitiesgroups:
        resp = await nucliadb_writer.delete(f"/kb/{kbid}/entitiesgroup/{entitiesgroup}")
        assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroups")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["groups"]) == 0

    # create and delete again an entities group
    await create_entities_group_by_api(kbid, "ANIMALS", nucliadb_writer)
    resp = await nucliadb_writer.delete(f"/kb/{kbid}/entitiesgroup/{entitiesgroup}")
    assert resp.status_code == 200


async def test_delete_entities_from_entities_group(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    kb_with_entities,
    predict_mock,
):
    """Deletion of entities of an entities group doesn't remove them from the index,
    but provides a custom view of entities in a KB.

    """
    kbid = kb_with_entities

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroup/ANIMALS")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body["entities"].keys()) == {
        "cat",
        "domestic-cat",
        "dog",
        "bird",
        "dolphin",
    }

    # Delete 'cat', an entity that is both stored and indexed
    entities = body
    entities["entities"].pop("cat")

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/entitiesgroup/ANIMALS", json=entities
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroup/ANIMALS")
    assert resp.status_code == 200
    body = resp.json()
    assert "cat" not in body["entities"]

    # but in fact, it's still stored
    predict_mock.detect_entities = AsyncMock(
        return_value=[
            RelationNode(
                value="cat",
                ntype=RelationNode.NodeType.ENTITY,
                subtype="ANIMALS",
            ),
        ]
    )
    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/search",
        params={"query": "do you like cats?", "features": "relations"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "cat" in body["relations"]["entities"]
    assert len(body["relations"]["entities"]["cat"]["related_to"]) > 0
