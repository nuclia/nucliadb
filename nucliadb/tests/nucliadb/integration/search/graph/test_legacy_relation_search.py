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
from unittest.mock import AsyncMock, Mock, patch

import pytest
from httpx import AsyncClient, Response

from nucliadb.search.search.query_parser.models import (
    NoopReranker,
    Query,
    ReciprocalRankFusion,
    RelationQuery,
    UnitRetrieval,
)
from nucliadb.search.search.query_parser.parsers.unit_retrieval import convert_retrieval_to_proto
from nucliadb_protos import utils_pb2


@pytest.mark.deploy_modes("standalone")
async def test_legacy_relation_search(
    nucliadb_reader: AsyncClient,
    kb_with_entity_graph: str,
):
    """Hacky test in which ... TODO"""
    kbid = kb_with_entity_graph

    query = RelationQuery(
        entry_points=[
            utils_pb2.RelationNode(value="Margaret", ntype=utils_pb2.RelationNode.NodeType.ENTITY)
        ],
        deleted_entity_groups=[],
        deleted_entities={},
    )
    resp = await entities_subgraph_search(nucliadb_reader, kbid, query)
    assert resp.status_code == 200
    body = resp.json()
    retrieved_graph = parse_graph_simple(body)
    assert retrieved_graph == {
        ("Margaret", "<-", "DEVELOPED", "-", "Apollo"),
        ("Margaret", "<-", "WORK_IN", "-", "Computer science"),
    }

    query = RelationQuery(
        entry_points=[
            utils_pb2.RelationNode(value="Margaret", ntype=utils_pb2.RelationNode.NodeType.ENTITY)
        ],
        deleted_entity_groups=["PROJECT"],
        deleted_entities={},
    )
    resp = await entities_subgraph_search(nucliadb_reader, kbid, query)
    assert resp.status_code == 200
    body = resp.json()
    retrieved_graph = parse_graph_simple(body)
    assert retrieved_graph == {
        ("Margaret", "<-", "WORK_IN", "-", "Computer science"),
    }

    query = RelationQuery(
        entry_points=[
            utils_pb2.RelationNode(value="Margaret", ntype=utils_pb2.RelationNode.NodeType.ENTITY)
        ],
        deleted_entity_groups=[],
        deleted_entities={"": ["Apollo"]},
    )
    resp = await entities_subgraph_search(nucliadb_reader, kbid, query)
    assert resp.status_code == 200
    body = resp.json()
    retrieved_graph = parse_graph_simple(body)
    assert retrieved_graph == {
        ("Margaret", "<-", "WORK_IN", "-", "Computer science"),
    }

    query = RelationQuery(
        entry_points=[
            utils_pb2.RelationNode(value="Margaret", ntype=utils_pb2.RelationNode.NodeType.ENTITY)
        ],
        deleted_entity_groups=[],
        deleted_entities={"PROJECT": ["Apollo"]},
    )
    resp = await entities_subgraph_search(nucliadb_reader, kbid, query)
    assert resp.status_code == 200
    body = resp.json()
    retrieved_graph = parse_graph_simple(body)
    assert retrieved_graph == {
        ("Margaret", "<-", "WORK_IN", "-", "Computer science"),
    }


async def entities_subgraph_search(
    nucliadb_reader: AsyncClient, kbid: str, query: RelationQuery
) -> Response:
    parsed = Mock()
    parsed.retrieval = UnitRetrieval(
        query=Query(relation=query),
        top_k=20,
        rank_fusion=ReciprocalRankFusion(window=20),  # unused
        reranker=NoopReranker(),  # unused
    )

    converted = convert_retrieval_to_proto(parsed)
    with (
        patch("nucliadb.search.search.find.parse_find", new=AsyncMock(return_value=parsed)),
        patch(
            "nucliadb.search.search.find.convert_retrieval_to_proto",
            new=AsyncMock(return_value=converted),
        ),
    ):
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/find",
            json={
                "features": ["relations"],
            },
        )
        return resp


def parse_graph_simple(body: dict):
    retrieved_graph = set()
    for source, subgraph in body["relations"]["entities"].items():
        for target in subgraph["related_to"]:
            if target["direction"] == "in":
                path = (source, "-", target["relation_label"], "->", target["entity"])
            else:
                path = (source, "<-", target["relation_label"], "-", target["entity"])

            assert path not in retrieved_graph, "We don't expect duplicated paths"
            retrieved_graph.add(path)
    return retrieved_graph
