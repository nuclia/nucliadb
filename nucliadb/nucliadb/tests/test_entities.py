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
from unittest.mock import AsyncMock, Mock

import pytest
from httpx import AsyncClient
from nucliadb_protos.resources_pb2 import Relation, RelationNode
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.tests.utils import broker_resource, inject_message
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility


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


async def set_test_entities_by_api(kbid: str, nucliadb_writer: AsyncClient):
    animals = {
        "title": "Animals",
        "entities": {
            "cat": {"value": "cat", "represents": ["domestic-cat", "house-cat"]},
            "domestic-cat": {"value": "domestic cat", "merged": True},
            "house-cat": {"value": "house cat", "merged": True},
            "dog": {"value": "dog"},
            "bird": {"value": "bird"},
        },
        "color": "black",
    }

    desserts = {
        "title": "Desserts",
        "entities": {
            "cookie": {"value": "cookie", "represents": ["biscuit"]},
            "biscuit": {"value": "biscuit", "merged": True},
            "brownie": {"value": "brownie"},
            "souffle": {"value": "souffle"},
        },
        "color": "pink",
    }

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/entitiesgroup/ANIMALS",
        headers={"X-Synchronous": "true"},
        json=animals,
    )
    assert resp.status_code == 200

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/entitiesgroup/DESSERTS",
        headers={"X-Synchronous": "true"},
        json=desserts,
    )
    assert resp.status_code == 200


async def set_test_entities_by_broker_message(kbid: str, nucliadb_grpc: WriterStub):
    bm = broker_resource(
        kbid, slug="resource-with-entities", title="Resource with entities"
    )
    nodes = {
        "fly": RelationNode(
            value="fly", ntype=RelationNode.NodeType.ENTITY, subtype="SUPERPOWERS"
        ),
        "invisibility": RelationNode(
            value="invisibility",
            ntype=RelationNode.NodeType.ENTITY,
            subtype="SUPERPOWERS",
        ),
        "telepathy": RelationNode(
            value="telepathy", ntype=RelationNode.NodeType.ENTITY, subtype="SUPERPOWERS"
        ),
    }
    bm.relations.extend(
        [
            Relation(
                relation=Relation.RelationType.ENTITY,
                source=nodes["fly"],
                to=nodes["invisibility"],
                relation_label="",
            ),
            Relation(
                relation=Relation.RelationType.ENTITY,
                source=nodes["invisibility"],
                to=nodes["telepathy"],
                relation_label="",
            ),
            Relation(
                relation=Relation.RelationType.ENTITY,
                source=nodes["telepathy"],
                to=nodes["fly"],
                relation_label="",
            ),
        ]
    )
    await inject_message(nucliadb_grpc, bm)


@pytest.mark.asyncio
@pytest.fixture(scope="function")
async def kb_with_entities(
    nucliadb_grpc: WriterStub,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    await set_test_entities_by_api(knowledgebox, nucliadb_writer)
    await set_test_entities_by_broker_message(knowledgebox, nucliadb_grpc)

    yield knowledgebox


@pytest.mark.asyncio
async def test_set_entities_indexes_entities(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
    predict_mock,
):
    kbid = knowledgebox

    await set_test_entities_by_api(kbid, nucliadb_writer)

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
        ]
    )

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/search",
        params={"query": "does your cat like cookies?", "features": "relations"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert {entity for entity in body["relations"]["entities"]} == {"cat", "cookie"}
    assert {
        relation["entity"]
        for relation in body["relations"]["entities"]["cat"]["related_to"]
    } == {"domestic cat", "house cat"}
    assert {
        relation["entity"]
        for relation in body["relations"]["entities"]["cookie"]["related_to"]
    } == {"biscuit"}


@pytest.mark.asyncio
async def test_broker_message_entities_are_indexed(
    nucliadb_reader: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
    predict_mock,
):
    kbid = knowledgebox

    await set_test_entities_by_broker_message(kbid, nucliadb_grpc)

    predict_mock.detect_entities = AsyncMock(
        return_value=[
            RelationNode(
                value="fly",
                ntype=RelationNode.NodeType.ENTITY,
                subtype="SUPERPOWERS",
            ),
        ]
    )

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/search",
        params={"query": "would you like to fly?", "features": "relations"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert {entity for entity in body["relations"]["entities"]} == {"fly"}
    assert {
        relation["entity"]
        for relation in body["relations"]["entities"]["fly"]["related_to"]
    } == {"invisibility", "telepathy"}


@pytest.mark.asyncio
async def test_get_entities_through_api(
    nucliadb_reader: AsyncClient,
    kb_with_entities,
    predict_mock,
):
    kbid = kb_with_entities

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/entitiesgroup/ANIMALS"
    )
    assert resp.status_code == 200
    body = resp.json()

    entities = {entity for entity in body["entities"]}
    assert entities == {"cat", "domestic-cat", "house-cat", "dog", "bird"}

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/entitiesgroup/SUPERPOWERS"
    )
    assert resp.status_code == 200
    body = resp.json()

    entities = {entity for entity in body["entities"]}
    assert entities == {"fly", "invisibility", "telepathy"}
