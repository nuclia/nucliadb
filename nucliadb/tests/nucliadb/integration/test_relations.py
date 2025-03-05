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

from nucliadb_models.metadata import RelationEntity, RelationNodeType
from nucliadb_models.search import GraphPath, GraphSearchResponse
from nucliadb_protos.resources_pb2 import (
    FieldComputedMetadataWrapper,
    FieldType,
    Relations,
)
from nucliadb_protos.utils_pb2 import Relation, RelationMetadata, RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import inject_message


@pytest.fixture
async def resource_with_bm_relations(
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "slug": "myresource",
            "texts": {"text1": {"body": "Mickey loves Minnie"}},
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    bm = await create_broker_message_with_relations()
    bm.kbid = standalone_knowledgebox
    bm.uuid = rid

    await inject_message(nucliadb_ingest_grpc, bm)

    yield rid, "text1"


@pytest.mark.deploy_modes("standalone")
async def test_api_aliases(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    resource_with_bm_relations: tuple[str, str],
):
    rid, field_id = resource_with_bm_relations

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}",
        params=dict(
            show=["relations", "extracted"],
            extracted=["metadata"],
        ),
    )
    assert resp.status_code == 200
    body = resp.json()
    extracted_metadata = body["data"]["texts"]["text1"]["extracted"]["metadata"]
    assert len(extracted_metadata["metadata"]["relations"]) == 1
    assert "from" in extracted_metadata["metadata"]["relations"][0]
    assert "from_" not in extracted_metadata["metadata"]["relations"][0]

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}/text/{field_id}",
        params=dict(
            show=["extracted"],
            extracted=["metadata"],
        ),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["extracted"]["metadata"]["metadata"]["relations"]) == 1
    assert "from" in body["extracted"]["metadata"]["metadata"]["relations"][0]
    assert "from_" not in body["extracted"]["metadata"]["metadata"]["relations"][0]


@pytest.mark.deploy_modes("standalone")
async def test_broker_message_relations(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    resource_with_bm_relations: tuple[str, str],
):
    """
    Test description:
    - Create a resource to assign some relations to it.
    - Using processing API, send a BrokerMessage with some relations
      for the resource
    - Validate the relations have been saved and are searchable
    """
    rid, field_id = resource_with_bm_relations

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}",
        params=dict(
            show=["relations", "extracted"],
            extracted=["metadata"],
        ),
    )
    assert resp.status_code == 200
    body = resp.json()
    # Resource level relations
    assert len(body["relations"]) == 3

    # Field level relations
    extracted_metadata = body["data"]["texts"]["text1"]["extracted"]["metadata"]
    assert len(extracted_metadata["metadata"]["relations"]) == 1
    relation = extracted_metadata["metadata"]["relations"][0]
    assert relation["label"] == "love"
    assert relation["metadata"] == dict(
        paragraph_id="foo", source_start=1, source_end=2, to_start=10, to_end=11
    )

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}/text/{field_id}",
        params=dict(
            show=["extracted"],
            extracted=["metadata"],
        ),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["extracted"]["metadata"]["metadata"]["relations"]) == 1


@pytest.mark.deploy_modes("standalone")
async def test_extracted_relations(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    """
    Test description:

    Create a resource with fields from which relations can be
    extracted and test it.
    """
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": "My resource",
            "slug": "myresource",
            "summary": "Some summary",
            "origin": {
                "collaborators": ["Anne", "John"],
            },
            "usermetadata": {
                "classifications": [
                    {"labelset": "labelset-1", "label": "label-1"},
                    {"labelset": "labelset-2", "label": "label-2"},
                ],
                "relations": [
                    {
                        "relation": "CHILD",
                        "to": {"type": "resource", "value": "document-uuid"},
                    },
                    {"relation": "ABOUT", "to": {"type": "label", "value": "label"}},
                    {
                        "relation": "ENTITY",
                        "to": {
                            "type": "entity",
                            "value": "entity-1",
                            "group": "entity-type-1",
                        },
                    },
                    {
                        "relation": "COLAB",
                        "to": {
                            "type": "user",
                            "value": "user",
                        },
                    },
                    {
                        "relation": "OTHER",
                        "to": {
                            "type": "label",
                            "value": "other",
                        },
                    },
                ],
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}?show=basic&show=relations"
    )
    assert resp.status_code == 200
    assert len(resp.json()["usermetadata"]["relations"]) == 5


async def create_broker_message_with_relations():
    e0 = RelationNode(value="E0", ntype=RelationNode.NodeType.ENTITY, subtype="")
    e1 = RelationNode(value="E1", ntype=RelationNode.NodeType.ENTITY, subtype="Official")
    e2 = RelationNode(value="E2", ntype=RelationNode.NodeType.ENTITY, subtype="Propaganda")
    r0 = Relation(relation=Relation.RelationType.CHILD, source=e0, to=e1, relation_label="R0")
    r1 = Relation(relation=Relation.RelationType.CHILD, source=e1, to=e2, relation_label="R1")
    r2 = Relation(relation=Relation.RelationType.CHILD, source=e2, to=e0, relation_label="R2")
    mickey = RelationNode(value="Mickey", ntype=RelationNode.NodeType.ENTITY, subtype="")
    minnie = RelationNode(value="Minnie", ntype=RelationNode.NodeType.ENTITY, subtype="Official")
    love_relation = Relation(
        relation=Relation.RelationType.CHILD,
        source=mickey,
        to=minnie,
        relation_label="love",
        metadata=RelationMetadata(
            paragraph_id="foo",
            source_start=1,
            source_end=2,
            to_start=10,
            to_end=11,
        ),
    )
    bm = BrokerMessage()

    # Add relations at the resource level
    bm.relations.extend([r0, r1, r2])

    # Add relations at the field level
    fcmw = FieldComputedMetadataWrapper()
    fcmw.field.field_type = FieldType.TEXT
    fcmw.field.field = "text1"
    relations = Relations()
    relations.relations.extend([love_relation])
    fcmw.metadata.metadata.relations.append(relations)
    bm.field_metadata.append(fcmw)

    return bm


@pytest.fixture(scope="function")
def knowledge_graph() -> tuple[dict[str, RelationEntity], list[tuple[str, str, str]]]:
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
        ("Peter", "LIVE_IN", "New York"),
        ("Tom", "CHASE", "Jerry"),
        ("Tom", "IS", "Cat"),
    ]

    return (entities, graph)


@pytest.mark.deploy_modes("standalone")
async def test_user_defined_knowledge_graph(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    knowledge_graph: tuple[dict[str, RelationEntity], list[tuple[str, str, str]]],
):
    kbid = standalone_knowledgebox
    entities, paths = knowledge_graph

    graph = paths[:3]
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Knowledge graph",
            "slug": "knowledge-graph",
            "summary": "User defined knowledge graph",
            "usermetadata": {
                "relations": [
                    {
                        "relation": "ENTITY",
                        "label": relation,
                        "from": entities[source].model_dump(),
                        "to": entities[target].model_dump(),
                    }
                    for source, relation, target in graph
                ],
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid}?show=basic")
    assert resp.status_code == 200
    user_graph = resp.json()["usermetadata"]["relations"]
    assert len(user_graph) == len(graph)

    # Update graph

    graph = paths
    resp = await nucliadb_writer.patch(
        f"/kb/{kbid}/resource/{rid}",
        json={
            "usermetadata": {
                "relations": [
                    {
                        "relation": "ENTITY",
                        "label": relation,
                        "from": entities[source].model_dump(),
                        "to": entities[target].model_dump(),
                    }
                    for source, relation, target in graph
                ],
            },
        },
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid}?show=basic")
    assert resp.status_code == 200
    user_graph = resp.json()["usermetadata"]["relations"]
    assert len(user_graph) == len(graph)

    # Search graph

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "query_entities": [
                {
                    "name": entities["Anna"].value,
                    "type": entities["Anna"].type.value,
                    "subtype": entities["Anna"].group,
                }
            ],
            "features": ["relations"],
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    retrieved_graph = set()
    for source, subgraph in body["relations"]["entities"].items():
        for target in subgraph["related_to"]:
            if target["direction"] == "in":
                path = (source, "-", target["relation_label"], "->", target["entity"])
            else:
                path = (source, "<-", target["relation_label"], "-", target["entity"])

            assert path not in retrieved_graph, "We don't expect duplicated paths"
            retrieved_graph.add(path)

    for relation in retrieved_graph:
        print(relation)

    assert len(retrieved_graph) == 5


@pytest.mark.deploy_modes("standalone")
async def test_graph_search(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    knowledge_graph: tuple[dict[str, RelationEntity], list[tuple[str, str, str]]],
):
    kbid = standalone_knowledgebox
    entities, paths = knowledge_graph

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "usermetadata": {
                "relations": [
                    {
                        "relation": "ENTITY",
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

    def simple_paths(paths: list[GraphPath]) -> list[tuple[str, str, str]]:
        simple_paths = []
        for path in paths:
            # response should never return empty nodes/relations
            assert path.source is not None
            assert path.source.value is not None
            assert path.relation is not None
            assert path.relation.label is not None
            assert path.destination is not None
            assert path.destination.value is not None
            simple_paths.append((path.source.value, path.relation.label, path.destination.value))
        return simple_paths

    ## Directed paths

    # (:Erin)-[]->(:UK)
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "source": {
                    "value": "Erin",
                    "group": "PERSON",
                },
                "destination": {
                    "value": "UK",
                    "group": "PLACE",
                },
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 1
    assert ("Erin", "BORN_IN", "UK") in paths

    # (:PERSON)-[]->(:PLACE)
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "source": {
                    "group": "PERSON",
                },
                "destination": {
                    "group": "PLACE",
                },
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 4
    assert ("Anna", "LIVE_IN", "New York") in paths
    assert ("Anna", "WORK_IN", "New York") in paths
    assert ("Erin", "BORN_IN", "UK") in paths
    assert ("Peter", "LIVE_IN", "New York") in paths

    # (:PERSON)-[LIVE_IN]->(:PLACE)
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "source": {
                    "group": "PERSON",
                },
                "relation": {
                    "label": "LIVE_IN",
                },
                "destination": {
                    "group": "PLACE",
                },
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 2
    assert ("Anna", "LIVE_IN", "New York") in paths
    assert ("Peter", "LIVE_IN", "New York") in paths

    ## Undirected paths
    # (:Anna)-[:IS_FRIEND]-()
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "source": {
                    "value": "Anna",
                },
                "relation": {
                    "label": "IS_FRIEND",
                },
                "undirected": True,
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 1
    assert ("Anastasia", "IS_FRIEND", "Anna") in paths
