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
import logging

import pytest
from grpc import aio
from httpx import AsyncClient

from nucliadb.standalone.settings import Settings
from nucliadb_protos.train_pb2_grpc import TrainStub
from nucliadb_protos.utils_pb2 import Relation, RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import inject_message
from tests.utils.dirty_index import wait_for_sync

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
async def knowledgebox(nucliadb_writer_manager: AsyncClient):
    resp = await nucliadb_writer_manager.post("/kbs", json={"slug": "knowledgebox"})
    assert resp.status_code == 201
    uuid = resp.json().get("uuid")

    yield uuid

    resp = await nucliadb_writer_manager.delete(f"/kb/{uuid}")
    assert resp.status_code == 200


@pytest.fixture(scope="function")
async def nucliadb_train(nucliadb: Settings):
    stub = TrainStub(aio.insecure_channel(f"localhost:{nucliadb.train_grpc_port}"))
    return stub


@pytest.fixture(scope="function")
async def knowledge_graph(nucliadb_writer: AsyncClient, nucliadb_ingest_grpc: WriterStub, knowledgebox):
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "title": "Knowledge graph",
            "slug": "knowledgegraph",
            "summary": "Test knowledge graph",
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    nodes = {
        "Animal": RelationNode(value="Animal", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Batman": RelationNode(value="Batman", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Becquer": RelationNode(value="Becquer", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Cat": RelationNode(value="Cat", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Catwoman": RelationNode(value="Catwoman", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Eric": RelationNode(value="Eric", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Fly": RelationNode(value="Fly", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Gravity": RelationNode(value="Gravity", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Joan Antoni": RelationNode(value="Joan Antoni", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Joker": RelationNode(value="Joker", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Newton": RelationNode(value="Newton", ntype=RelationNode.NodeType.ENTITY, subtype="science"),
        "Isaac Newsome": RelationNode(
            value="Isaac Newsome", ntype=RelationNode.NodeType.ENTITY, subtype="science"
        ),
        "Physics": RelationNode(value="Physics", ntype=RelationNode.NodeType.ENTITY, subtype="science"),
        "Poetry": RelationNode(value="Poetry", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Swallow": RelationNode(value="Swallow", ntype=RelationNode.NodeType.ENTITY, subtype=""),
    }

    edges = [
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Batman"],
            to=nodes["Catwoman"],
            relation_label="love",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Batman"],
            to=nodes["Joker"],
            relation_label="fight",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Joker"],
            to=nodes["Physics"],
            relation_label="enjoy",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Catwoman"],
            to=nodes["Cat"],
            relation_label="imitate",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Cat"],
            to=nodes["Animal"],
            relation_label="species",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Newton"],
            to=nodes["Physics"],
            relation_label="study",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Newton"],
            to=nodes["Gravity"],
            relation_label="formulate",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Isaac Newsome"],
            to=nodes["Physics"],
            relation_label="study",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Isaac Newsome"],
            to=nodes["Gravity"],
            relation_label="formulate",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Eric"],
            to=nodes["Cat"],
            relation_label="like",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Eric"],
            to=nodes["Joan Antoni"],
            relation_label="collaborate",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Joan Antoni"],
            to=nodes["Eric"],
            relation_label="collaborate",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Joan Antoni"],
            to=nodes["Becquer"],
            relation_label="read",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Becquer"],
            to=nodes["Poetry"],
            relation_label="write",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Becquer"],
            to=nodes["Poetry"],
            relation_label="like",
        ),
        Relation(
            relation=Relation.RelationType.ABOUT,
            source=nodes["Poetry"],
            to=nodes["Swallow"],
            relation_label="about",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Swallow"],
            to=nodes["Animal"],
            relation_label="species",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Swallow"],
            to=nodes["Fly"],
            relation_label="can",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Fly"],
            to=nodes["Gravity"],
            relation_label="defy",
        ),
    ]

    bm = BrokerMessage()
    bm.uuid = rid
    bm.kbid = knowledgebox
    bm.relations.extend(edges)
    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/entitiesgroups",
        json={
            "title": "scientist",
            "color": "",
            "entities": {
                "Isaac": {"value": "Isaac"},
                "Isaac Newton": {"value": "Isaac Newton", "represents": ["Newton"]},
                "Isaac Newsome": {"value": "Isaac Newsome"},
            },
            "custom": True,
            "group": "scientist",
        },
    )
    assert resp.status_code == 200, resp.content
    resp = await nucliadb_writer.patch(
        f"/kb/{knowledgebox}/entitiesgroup/scientist",
        json={"add": {}, "update": {}, "delete": ["Isaac Newsome"]},
    )
    assert resp.status_code == 200, resp.content
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/entitiesgroups",
        json={
            "title": "poet",
            "color": "",
            "entities": {
                "Becquer": {
                    "value": "Becquer",
                    "represents": ["Gustavo Adolfo Bécquer"],
                },
                "Gustavo Adolfo Bécquer": {"value": "Gustavo Adolfo Bécquer"},
            },
            "custom": True,
            "group": "poet",
        },
    )
    assert resp.status_code == 200, resp.content

    return (nodes, edges)


@pytest.fixture(scope="function")
async def txn(maindb_driver):
    async with maindb_driver.transaction() as txn:
        yield txn
        await txn.abort()
