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

from nucliadb_models.graph import responses as graph_responses
from nucliadb_models.graph.responses import GraphSearchResponse
from nucliadb_models.metadata import RelationType
from nucliadb_protos.resources_pb2 import FieldComputedMetadataWrapper, FieldType, Relations
from nucliadb_protos.utils_pb2 import Relation, RelationMetadata, RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import inject_message


@pytest.mark.deploy_modes("standalone")
async def test_graph_search__node_queries(
    nucliadb_reader: AsyncClient,
    kb_with_entity_graph: str,
):
    kbid = kb_with_entity_graph

    # ()-[]->()
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
                "source": {},
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 17

    # (:PERSON)-[]->()
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "source_node",
                "group": "PERSON",
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 12

    # (:PERSON)-[]->()
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
                "source": {
                    "group": "PERSON",
                },
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 12

    # (:Anna)-[]->()
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
                "source": {
                    "value": "Anna",
                    "group": "PERSON",
                },
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 4
    assert ("Anna", "FOLLOW", "Erin") in paths
    assert ("Anna", "LIVE_IN", "New York") in paths
    assert ("Anna", "LOVE", "Cat") in paths
    assert ("Anna", "WORK_IN", "New York") in paths

    # ()-[]->(:Anna)
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
                "destination": {
                    "value": "Anna",
                    "group": "PERSON",
                },
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 1
    assert ("Anastasia", "IS_FRIEND", "Anna") in paths

    # (:Anna)
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
                "source": {
                    "value": "Anna",
                    "group": "PERSON",
                },
                "undirected": True,
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 5
    assert ("Anna", "FOLLOW", "Erin") in paths
    assert ("Anna", "LIVE_IN", "New York") in paths
    assert ("Anna", "LOVE", "Cat") in paths
    assert ("Anna", "WORK_IN", "New York") in paths
    assert ("Anastasia", "IS_FRIEND", "Anna") in paths


@pytest.mark.deploy_modes("standalone")
async def test_graph_search__fuzzy_node_queries(
    nucliadb_reader: AsyncClient,
    kb_with_entity_graph: str,
):
    kbid = kb_with_entity_graph

    # (:~Anastas)
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
                "source": {
                    "value": "Anastas",
                    "match": "fuzzy",
                },
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 1
    assert ("Anastasia", "IS_FRIEND", "Anna") in paths

    # (:~AnXstXsiX)
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
                "source": {
                    "value": "AnXstXsiX",
                    "match": "fuzzy",
                },
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 0

    # (:~Ansatasia)
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
                "source": {
                    "value": "Ansatasia",
                    "match": "fuzzy",
                },
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 1
    assert ("Anastasia", "IS_FRIEND", "Anna") in paths


@pytest.mark.deploy_modes("standalone")
async def test_graph_search__relation_queries(
    nucliadb_reader: AsyncClient,
    kb_with_entity_graph: str,
):
    kbid = kb_with_entity_graph

    # ()-[LIVE_IN]->()
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "relation",
                "label": "LIVE_IN",
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 2
    assert ("Anna", "LIVE_IN", "New York") in paths
    assert ("Peter", "LIVE_IN", "New York") in paths

    # ()-[LIVE_IN]->()
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
                "relation": {
                    "label": "LIVE_IN",
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

    # ()-[LIVE_IN]->()
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
                "relation": {
                    "type": RelationType.SYNONYM.value,
                },
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 1
    assert ("Mr. P", "ALIAS", "Peter") in paths

    # ()-[: LIVE_IN | BORN_IN]->()
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "or": [
                    {
                        "prop": "path",
                        "relation": {
                            "label": "LIVE_IN",
                        },
                    },
                    {
                        "prop": "path",
                        "relation": {
                            "label": "BORN_IN",
                        },
                    },
                ]
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 3
    assert ("Anna", "LIVE_IN", "New York") in paths
    assert ("Erin", "BORN_IN", "UK") in paths
    assert ("Peter", "LIVE_IN", "New York") in paths

    # ()-[:!LIVE_IN]->()
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "not": {
                    "prop": "path",
                    "relation": {
                        "label": "LIVE_IN",
                    },
                },
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 15


@pytest.mark.deploy_modes("standalone")
async def test_graph_search__directed_path_queries(
    nucliadb_reader: AsyncClient,
    kb_with_entity_graph: str,
):
    kbid = kb_with_entity_graph

    # (:Erin)-[]->(:UK)
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
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
                "prop": "path",
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
                "prop": "path",
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

    # (:!Anna)-[:LIVE_IN|LOVE]->()
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "and": [
                    {
                        "not": {
                            "prop": "path",
                            "source": {
                                "value": "Anna",
                            },
                        }
                    },
                    {
                        "or": [
                            {
                                "prop": "path",
                                "relation": {
                                    "label": "LIVE_IN",
                                },
                            },
                            {
                                "prop": "path",
                                "relation": {
                                    "label": "LOVE",
                                },
                            },
                        ]
                    },
                ]
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 3
    assert ("Erin", "LOVE", "Climbing") in paths
    assert ("Dimitri", "LOVE", "Anastasia") in paths
    assert ("Peter", "LIVE_IN", "New York") in paths


@pytest.mark.deploy_modes("standalone")
async def test_graph_search__undirected_path_queries(
    nucliadb_reader: AsyncClient,
    kb_with_entity_graph: str,
):
    kbid = kb_with_entity_graph

    # (:Anna)-[:IS_FRIEND]-()
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
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


@pytest.mark.deploy_modes("standalone")
async def test_graph_search__security(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    kb_with_entity_graph: str,
):
    kbid = kb_with_entity_graph

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resources",
    )
    assert resp.status_code == 200
    rid = resp.json()["resources"][0]["id"]

    resp = await nucliadb_writer.patch(
        f"/kb/{kbid}/resource/{rid}",
        json={
            "security": {
                "access_groups": ["secret"],
            },
        },
    )
    assert resp.status_code == 200, resp.text

    # Without groups, returns it
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
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

    # With the proper group, returns it
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
                "source": {
                    "value": "Anna",
                },
                "relation": {
                    "label": "IS_FRIEND",
                },
                "undirected": True,
            },
            "top_k": 100,
            "security": {"groups": ["secret"]},
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 1

    # With other groups, returns nothing
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
                "source": {
                    "value": "Anna",
                },
                "relation": {
                    "label": "IS_FRIEND",
                },
                "undirected": True,
            },
            "top_k": 100,
            "security": {"groups": ["fake"]},
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 0


@pytest.mark.deploy_modes("standalone")
async def test_graph_search__hidden(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_writer_manager: AsyncClient,
    kb_with_entity_graph: str,
):
    kbid = kb_with_entity_graph

    resp = await nucliadb_writer_manager.patch(
        f"/kb/{kb_with_entity_graph}", json={"hidden_resources_enabled": True}
    )

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resources",
    )
    assert resp.status_code == 200
    rid = resp.json()["resources"][0]["id"]

    resp = await nucliadb_writer.patch(
        f"/kb/{kbid}/resource/{rid}",
        json={
            "hidden": True,
        },
    )
    assert resp.status_code == 200, resp.text

    # By default, does not return hidden
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
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
    assert len(paths) == 0

    # It returns them with show_hidden
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
                "source": {
                    "value": "Anna",
                },
                "relation": {
                    "label": "IS_FRIEND",
                },
                "undirected": True,
            },
            "top_k": 100,
            "show_hidden": True,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 1


@pytest.mark.deploy_modes("standalone")
async def test_graph_search__filtering(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    kb_with_entity_graph: str,
):
    kbid = kb_with_entity_graph

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resources",
    )
    assert resp.status_code == 200
    rid = resp.json()["resources"][0]["id"]

    # Filter to include the result
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
                "source": {
                    "value": "Anna",
                },
                "relation": {
                    "label": "IS_FRIEND",
                },
                "undirected": True,
            },
            "top_k": 100,
            "filter_expression": {"field": {"prop": "resource", "id": rid}},
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 1

    # Filter to exclude the result
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
                "source": {
                    "value": "Anna",
                },
                "relation": {
                    "label": "IS_FRIEND",
                },
                "undirected": True,
            },
            "top_k": 100,
            "filter_expression": {"field": {"not": {"prop": "resource", "id": rid}}},
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 0


@pytest.mark.deploy_modes("standalone")
async def test_graph_search_facets(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox

    # Create a resource with entities from different origins (user, processor, da)
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
                        "label": "SAME",
                        "from": {"type": "entity", "group": "PERSON", "value": "Ursula User"},
                        "to": {"type": "entity", "group": "PERSON", "value": "Úrsula Usuario"},
                    }
                ],
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    rel = Relation(
        relation=Relation.RelationType.ENTITY,
        source=RelationNode(
            ntype=RelationNode.NodeType.ENTITY, subtype="PERSON", value="Peter Processor"
        ),
        to=RelationNode(ntype=RelationNode.NodeType.ENTITY, subtype="PERSON", value="Pedro Procesador"),
        relation_label="SAME",
        metadata=RelationMetadata(
            paragraph_id="foo",
            source_start=1,
            source_end=2,
            to_start=10,
            to_end=11,
        ),
    )

    # Broker message from processor
    bm = BrokerMessage()
    bm.kbid = kbid
    bm.uuid = rid
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    fcmw = FieldComputedMetadataWrapper()
    fcmw.field.field_type = FieldType.TEXT
    fcmw.field.field = "text"
    relations = Relations()
    relations.relations.append(rel)
    fcmw.metadata.metadata.relations.append(relations)
    bm.field_metadata.append(fcmw)

    await inject_message(nucliadb_ingest_grpc, bm)

    # Broker message from DA
    rel.metadata.data_augmentation_task_id = "mytask"
    rel.source.value = "Alfred Agent"
    rel.to.value = "Alfredo Agente"

    bm = BrokerMessage()
    bm.kbid = kbid
    bm.uuid = rid
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    fcmw = FieldComputedMetadataWrapper()
    fcmw.field.field_type = FieldType.TEXT
    fcmw.field.field = "text2"
    relations = Relations()
    relations.relations.append(rel)
    fcmw.metadata.metadata.relations.append(relations)
    bm.field_metadata.append(fcmw)

    await inject_message(nucliadb_ingest_grpc, bm)

    # Everything
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "relation",
                "label": "SAME",
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 3

    # User
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "and": [
                    {
                        "prop": "relation",
                        "label": "SAME",
                    },
                    {"prop": "generated", "by": "user"},
                ]
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 1
    assert paths == [("Ursula User", "SAME", "Úrsula Usuario")]

    # Processor
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "and": [
                    {
                        "prop": "relation",
                        "label": "SAME",
                    },
                    {"prop": "generated", "by": "processor"},
                ]
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 1
    assert paths == [("Peter Processor", "SAME", "Pedro Procesador")]

    # DA
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "and": [
                    {
                        "prop": "relation",
                        "label": "SAME",
                    },
                    {"prop": "generated", "by": "data-augmentation"},
                ]
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 1
    assert paths == [("Alfred Agent", "SAME", "Alfredo Agente")]

    # DA task
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "and": [
                    {
                        "prop": "relation",
                        "label": "SAME",
                    },
                    {"prop": "generated", "by": "data-augmentation", "da_task": "mytask"},
                ]
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 1
    assert paths == [("Alfred Agent", "SAME", "Alfredo Agente")]

    # fake DA task
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "and": [
                    {
                        "prop": "relation",
                        "label": "SAME",
                    },
                    {"prop": "generated", "by": "data-augmentation", "da_task": "faketask"},
                ]
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    paths = simple_paths(GraphSearchResponse.model_validate(resp.json()).paths)
    assert len(paths) == 0


def simple_paths(paths: list[graph_responses.GraphPath]) -> list[tuple[str, str, str]]:
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
