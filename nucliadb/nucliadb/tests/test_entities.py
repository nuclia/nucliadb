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
from unittest.mock import AsyncMock, Mock

import pytest
from httpx import AsyncClient
from nucliadb_protos.resources_pb2 import Relation, RelationNode
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.tests.utils import broker_resource, inject_message
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility

TEST_API_ENTITIES_GROUPS = {
    "ANIMALS": {
        "title": "Animals",
        "entities": {
            "cat": {"value": "cat", "represents": ["domestic-cat"]},
            "domestic-cat": {"value": "domestic cat", "merged": True},
            "dog": {"value": "dog"},
            "bird": {"value": "bird"},
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
def predict_mock() -> Mock:  # type: ignore
    predict = get_utility(Utility.PREDICT)
    mock = Mock()
    set_utility(Utility.PREDICT, mock)

    yield mock

    if predict is None:
        clean_utility(Utility.PREDICT)
    else:
        set_utility(Utility.PREDICT, predict)


@pytest.mark.asyncio
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

    yield knowledgebox


@pytest.mark.asyncio
async def test_entities_store_and_indexing(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    kb_with_entities,
    predict_mock,
):
    kbid = kb_with_entities

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroups")
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
    # "cookie" is not found because don't have relation with anything. Relations
    # by BM have a fake relations with themselves for sake of testing
    assert entities == {"cat", "invisibility"}


@pytest.mark.asyncio
async def test_get_entities(
    nucliadb_reader: AsyncClient,
    kb_with_entities,
):
    kbid = kb_with_entities

    expected = {
        "ANIMALS": {"cat", "domestic-cat", "dog", "bird", "dolphin"},
        "DESSERTS": {"cookie", "biscuit", "brownie", "souffle"},
        "SUPERPOWERS": {"fly", "invisibility", "telepathy"},
    }

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroups")
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


@pytest.mark.asyncio
async def test_set_entities(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    kb_with_entities,
):
    """We'll modify ANIMALS entities group which is set by API and BM to test
    setting entities by API "overwrites" whatever is stored

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
        "custom": True,
    }

    resp = await nucliadb_writer.post(f"/kb/{kbid}/entitiesgroup/ANIMALS", json=animals)
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroup/ANIMALS")
    assert resp.status_code == 200
    body = resp.json()
    assert body == animals


@pytest.mark.asyncio
async def test_delete_entitiesgroups(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    kb_with_entities,
):
    kbid = kb_with_entities

    resp = await nucliadb_writer.delete(f"/kb/{kbid}/entitiesgroup/nonexistent")
    assert resp.status_code == 200

    entitiesgroups = [
        "DESSERTS",  # added with API
        "SUPERPOWERS",  # added with broker message
        "ANIMALS",  # added with API and broker message
    ]

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroups")
    body = resp.json()
    assert set(entitiesgroups) == set(body["groups"])

    for entitiesgroup in entitiesgroups:
        resp = await nucliadb_writer.delete(f"/kb/{kbid}/entitiesgroup/{entitiesgroup}")
        assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroups")
    body = resp.json()
    assert len(body["groups"]) == 0


@pytest.mark.asyncio
async def test_delete_entities(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    kb_with_entities,
    predict_mock,
):
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


@pytest.mark.skip(reason="KB reindex not implemented directly")
@pytest.mark.asyncio
async def test_reindex_entities(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    kb_with_entities,
    predict_mock,
):
    kbid = kb_with_entities

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroups")
    assert resp.status_code == 200
    body = resp.json()
    assert "ANIMALS" in body["groups"]
    assert "DESSERTS" in body["groups"]
    assert "SUPERPOWERS" in body["groups"]

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resources")
    assert resp.status_code == 200
    body = resp.json()

    for resource in body["resources"]:
        rid = resource["id"]
        resp = await nucliadb_writer.post(f"/kb/{kbid}/resource/reindex")
        assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroups")
    assert resp.status_code == 200
    body = resp.json()
    assert "ANIMALS" in body["groups"]
    assert "DESSERTS" in body["groups"]
    assert "SUPERPOWERS" in body["groups"]


@pytest.mark.asyncio
async def test_entity_migration(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
):
    """Test new change on state doesn't affect old data"""
    # TODO
