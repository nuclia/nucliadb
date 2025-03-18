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
import logging
import time
import uuid
from typing import AsyncIterator

import pytest
from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.processor import Processor
from nucliadb.ingest.orm.resource import Resource
from nucliadb.tests.vectors import V1
from nucliadb.writer.api.v1.router import KB_PREFIX, KBS_PREFIX
from nucliadb_models.metadata import RelationEntity, RelationNodeType, RelationType
from nucliadb_protos import utils_pb2 as upb
from nucliadb_protos.knowledgebox_pb2 import SemanticModelMetadata
from nucliadb_protos.utils_pb2 import Relation, RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_utils.storages.storage import Storage
from tests.utils import inject_message
from tests.utils.broker_messages import BrokerMessageBuilder
from tests.utils.dirty_index import wait_for_sync

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
async def knowledgebox(
    storage: Storage,
    maindb_driver: Driver,
    shard_manager: KBShardManager,
):
    kbid = KnowledgeBox.new_unique_kbid()
    kbslug = "slug-" + str(uuid.uuid4())
    model = SemanticModelMetadata(
        similarity_function=upb.VectorSimilarity.COSINE, vector_dimension=len(V1)
    )
    await KnowledgeBox.create(
        maindb_driver, kbid=kbid, slug=kbslug, semantic_models={"my-semantic-model": model}
    )

    yield kbid

    await KnowledgeBox.delete(maindb_driver, kbid)
    # await KnowledgeBox.purge(maindb_driver, kbid)


@pytest.fixture(scope="function")
async def standalone_knowledgebox(nucliadb_writer_manager: AsyncClient):
    resp = await nucliadb_writer_manager.post("/kbs", json={"slug": "knowledgebox"})
    assert resp.status_code == 201
    uuid = resp.json().get("uuid")

    yield uuid

    resp = await nucliadb_writer_manager.delete(f"/kb/{uuid}")
    assert resp.status_code == 200


# FIXME: this is a weird situation, we can use a hosted-like nucliadb while this
# creates a KB as it was onprem. The end result should not change much but still, is
# something we may want to fix
@pytest.fixture(scope="function")
async def knowledgebox_by_api(nucliadb_writer_manager: AsyncClient):
    kbslug = "slug-" + str(uuid.uuid4())
    resp = await nucliadb_writer_manager.post(
        f"/{KBS_PREFIX}",
        json={
            "slug": kbslug,
            "title": "My Test Knowledge Box",
        },
    )
    assert resp.status_code == 201
    kbid = resp.json().get("uuid")
    assert kbid is not None

    yield kbid

    resp = await nucliadb_writer_manager.delete(
        f"/{KB_PREFIX}/{kbid}",
    )
    assert resp.status_code == 200


# Used by: nucliadb standalone tests
@pytest.fixture(scope="function")
async def knowledgebox_one(knowledgebox_by_api: str):
    yield knowledgebox_by_api


# Used by: nucliadb writer tests
@pytest.fixture(scope="function")
async def knowledgebox_writer(knowledgebox_by_api: str):
    yield knowledgebox_by_api


@pytest.fixture(scope="function")
async def full_resource(
    storage: Storage,
    maindb_driver: Driver,
    knowledgebox: str,
) -> AsyncIterator[Resource]:
    """Create a resource in `knowledgebox` that has every possible bit of information.

    DISCLAIMER: this comes from a really old fixture, so probably it does
    **not** contains every possible bit of information.

    """
    from tests.ingest.fixtures import create_resource

    resource = await create_resource(
        storage=storage,
        driver=maindb_driver,
        knowledgebox_ingest=knowledgebox,
    )
    yield resource
    resource.clean()


# Used by: nucliadb writer tests
@pytest.fixture(scope="function")
async def resource(nucliadb_writer: AsyncClient, knowledgebox_writer: str):
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox_writer}/resources",
        json={
            "slug": "resource1",
            "title": "Resource 1",
        },
    )
    assert resp.status_code == 201
    uuid = resp.json()["uuid"]

    return uuid


@pytest.fixture(scope="function")
async def simple_resources(
    maindb_driver: Driver, processor: Processor, knowledgebox: str
) -> AsyncIterator[tuple[str, list[str]]]:
    """Create a set of resources with basic information on `knowledgebox`."""
    total = 10
    resource_ids = []

    for i in range(1, total + 1):
        slug = f"simple-resource-{i}"
        bmb = BrokerMessageBuilder(kbid=knowledgebox, slug=slug)
        bmb.with_title(f"My simple resource {i}")
        bmb.with_summary(f"Summary of my simple resource {i}")
        bm = bmb.build()
        await processor.process(message=bm, seqid=i)
        resource_ids.append(bm.uuid)

    # Give processed data some time to be processed
    timeout = 5
    start = time.time()
    created_count = 0
    while created_count < total or (time.time() - start) < timeout:
        created_count = len(
            [rid async for rid in datamanagers.resources.iterate_resource_ids(kbid=knowledgebox)]
        )
        await asyncio.sleep(0.1)

    yield knowledgebox, resource_ids


# Only supported for standalone (as it depends on standalone_knowledgebox fixture)
@pytest.fixture(scope="function")
async def knowledge_graph(
    nucliadb_writer: AsyncClient, nucliadb_ingest_grpc: WriterStub, standalone_knowledgebox: str
):
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
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
    bm.kbid = standalone_knowledgebox
    bm.user_relations.relations.extend(edges)
    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()

    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/entitiesgroups",
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
        f"/kb/{standalone_knowledgebox}/entitiesgroup/scientist",
        json={"add": {}, "update": {}, "delete": ["Isaac Newsome"]},
    )
    assert resp.status_code == 200, resp.content
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/entitiesgroups",
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

    return (nodes, edges, rid)


@pytest.fixture(scope="function")
async def kb_with_entity_graph(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    entity_graph: tuple[dict[str, RelationEntity], dict[str, RelationType], list[tuple[str, str, str]]],
) -> AsyncIterator[str]:
    kbid = standalone_knowledgebox
    entities, relations, paths = entity_graph

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "usermetadata": {
                "relations": [
                    {
                        "relation": relations[relation].value,
                        "label": relation,
                        "from": entities[source].model_dump(),
                        "to": entities[target].model_dump(),
                    }
                    for source, relation, target in paths
                ],
            },
        },
    )
    assert resp.status_code == 201

    yield kbid


@pytest.fixture(scope="function")
def entity_graph() -> tuple[
    dict[str, RelationEntity], dict[str, RelationType], list[tuple[str, str, str]]
]:
    entities = {
        "Anastasia": RelationEntity(value="Anastasia", type=RelationNodeType.ENTITY, group="PERSON"),
        "Anna": RelationEntity(value="Anna", type=RelationNodeType.ENTITY, group="PERSON"),
        "Apollo": RelationEntity(value="Apollo", type=RelationNodeType.ENTITY, group="PROJECT"),
        "Cat": RelationEntity(value="Cat", type=RelationNodeType.ENTITY, group="ANIMAL"),
        "Climbing": RelationEntity(value="Climbing", type=RelationNodeType.ENTITY, group="ACTIVITY"),
        "Computer science": RelationEntity(
            value="Computer science", type=RelationNodeType.ENTITY, group="STUDY_FIELD"
        ),
        "Dimitri": RelationEntity(value="Dimitri", type=RelationNodeType.ENTITY, group="PERSON"),
        "Erin": RelationEntity(value="Erin", type=RelationNodeType.ENTITY, group="PERSON"),
        "Jerry": RelationEntity(value="Jerry", type=RelationNodeType.ENTITY, group="ANIMAL"),
        "Margaret": RelationEntity(value="Margaret", type=RelationNodeType.ENTITY, group="PERSON"),
        "Mr. P": RelationEntity(value="Mr. P", type=RelationNodeType.ENTITY, group="AGENT"),
        "Mouse": RelationEntity(value="Mouse", type=RelationNodeType.ENTITY, group="ANIMAL"),
        "New York": RelationEntity(value="New York", type=RelationNodeType.ENTITY, group="PLACE"),
        "Olympic athlete": RelationEntity(
            value="Olympic athlete", type=RelationNodeType.ENTITY, group="SPORT"
        ),
        "Peter": RelationEntity(value="Peter", type=RelationNodeType.ENTITY, group="PERSON"),
        "Rocket": RelationEntity(value="Rocket", type=RelationNodeType.ENTITY, group="VEHICLE"),
        "Tom": RelationEntity(value="Tom", type=RelationNodeType.ENTITY, group="ANIMAL"),
        "UK": RelationEntity(value="UK", type=RelationNodeType.ENTITY, group="PLACE"),
    }
    relations = {
        "ALIAS": RelationType.SYNONYM,
        "BORN_IN": RelationType.ENTITY,
        "CHASE": RelationType.ENTITY,
        "DEVELOPED": RelationType.ENTITY,
        "FOLLOW": RelationType.ENTITY,
        "IS": RelationType.ENTITY,
        "IS_FRIEND": RelationType.ENTITY,
        "LIVE_IN": RelationType.ENTITY,
        "LOVE": RelationType.ENTITY,
        "WORK_IN": RelationType.ENTITY,
    }
    graph = [
        ("Anastasia", "IS_FRIEND", "Anna"),
        ("Anna", "FOLLOW", "Erin"),
        ("Anna", "LIVE_IN", "New York"),
        ("Anna", "LOVE", "Cat"),
        ("Anna", "WORK_IN", "New York"),
        ("Apollo", "IS", "Rocket"),
        ("Dimitri", "LOVE", "Anastasia"),
        ("Erin", "BORN_IN", "UK"),
        ("Erin", "IS", "Olympic athlete"),
        ("Erin", "LOVE", "Climbing"),
        ("Jerry", "IS", "Mouse"),
        ("Margaret", "DEVELOPED", "Apollo"),
        ("Margaret", "WORK_IN", "Computer science"),
        ("Mr. P", "ALIAS", "Peter"),
        ("Peter", "LIVE_IN", "New York"),
        ("Tom", "CHASE", "Jerry"),
        ("Tom", "IS", "Cat"),
    ]

    return (entities, relations, graph)
