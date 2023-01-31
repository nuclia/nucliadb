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
from nucliadb_protos.resources_pb2 import RelationNode
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility


@pytest.fixture(scope="function")
async def predict_mock() -> Mock:
    predict = get_utility(Utility.PREDICT)
    mock = Mock()
    set_utility(Utility.PREDICT, mock)

    yield mock

    if predict is None:
        clean_utility(Utility.PREDICT)
    else:
        set_utility(Utility.PREDICT, predict)


@pytest.fixture(scope="function")
async def simple_entities_kb(
    nucliadb_manager: AsyncClient,
    nucliadb_writer: AsyncClient,
):
    resp = await nucliadb_manager.post("/kbs", json={"slug": "entities-simple"})
    assert resp.status_code == 201
    kbid = resp.json().get("uuid")

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

    yield kbid

    resp = await nucliadb_manager.delete(f"/kb/{kbid}")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_set_entities_indexes_entities(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    simple_entities_kb,
    predict_mock,
):
    kbid = simple_entities_kb

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
