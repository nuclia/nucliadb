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
from typing import AsyncIterator

import pytest
from httpx import AsyncClient

from nucliadb_models.metadata import RelationEntity, RelationNodeType, RelationType
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_protos.utils_pb2 import Relation, RelationMetadata, RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage, FieldComputedMetadataWrapper
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import inject_message
from tests.utils.dirty_index import wait_for_sync


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
            metadata=RelationMetadata(
                paragraph_id="myresource/0/myresource/100-200",
                source_start=1,
                source_end=10,
                to_start=11,
                to_end=20,
                data_augmentation_task_id="mytask",
            ),
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


@pytest.fixture(scope="function")
async def graph_resource(nucliadb_writer: AsyncClient, nucliadb_ingest_grpc, standalone_knowledgebox):
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": "Knowledge graph",
            "slug": "knowledgegraph",
            "summary": "Test knowledge graph",
            "texts": {
                "inception1": {"body": "Christopher Nolan directed Inception. Very interesting movie."},
                "inception2": {"body": "Leonardo DiCaprio starred in Inception."},
                "inception3": {"body": "Joseph Gordon-Levitt starred in Inception."},
                "leo": {"body": "Leonardo DiCaprio is a great actor. DiCaprio started acting in 1989."},
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    nodes = {
        "nolan": RelationNode(
            value="Christopher Nolan", ntype=RelationNode.NodeType.ENTITY, subtype="DIRECTOR"
        ),
        "inception": RelationNode(
            value="Inception", ntype=RelationNode.NodeType.ENTITY, subtype="MOVIE"
        ),
        "leo": RelationNode(
            value="Leonardo DiCaprio", ntype=RelationNode.NodeType.ENTITY, subtype="ACTOR"
        ),
        "dicaprio": RelationNode(value="DiCaprio", ntype=RelationNode.NodeType.ENTITY, subtype="ACTOR"),
        "levitt": RelationNode(
            value="Joseph Gordon-Levitt", ntype=RelationNode.NodeType.ENTITY, subtype="ACTOR"
        ),
    }

    edges = [
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["nolan"],
            to=nodes["inception"],
            relation_label="directed",
            metadata=RelationMetadata(
                # Set this field id as int enum value since this is how legacy relations reported paragraph_id
                paragraph_id=rid + "/4/inception1/0-37",
                data_augmentation_task_id="my_graph_task_id",
            ),
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["leo"],
            to=nodes["inception"],
            relation_label="starred",
            metadata=RelationMetadata(
                paragraph_id=rid + "/t/inception2/0-39",
                data_augmentation_task_id="my_graph_task_id",
            ),
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["levitt"],
            to=nodes["inception"],
            relation_label="starred",
            metadata=RelationMetadata(
                paragraph_id=rid + "/t/inception3/0-42",
            ),
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["leo"],
            to=nodes["dicaprio"],
            relation_label="analogy",
            metadata=RelationMetadata(
                paragraph_id=rid + "/t/leo/0-70",
                data_augmentation_task_id="my_graph_task_id",
            ),
        ),
    ]

    # Add relations to the resource as processor does
    for n in nodes.values():
        edges.append(
            Relation(
                relation=Relation.RelationType.ENTITY,
                source=RelationNode(value=rid, ntype=RelationNode.NodeType.RESOURCE),
                to=n,
            )
        )

    bm = BrokerMessage()
    bm.uuid = rid
    bm.kbid = standalone_knowledgebox
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    fcmw = FieldComputedMetadataWrapper()
    fcmw.field.field_type = FieldType.TEXT
    fcmw.field.field = "inception1"
    fcmw.metadata.metadata.relations.add(relations=edges)
    bm.field_metadata.append(fcmw)
    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()
    yield rid
