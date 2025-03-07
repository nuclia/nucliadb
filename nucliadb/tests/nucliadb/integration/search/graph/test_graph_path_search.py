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
    assert len(paths) == 16

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

    # (:~AnXstXsia)
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph",
        json={
            "query": {
                "prop": "path",
                "source": {
                    "value": "AnXstXsia",
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
    assert len(paths) == 14


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
