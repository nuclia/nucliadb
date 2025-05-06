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
from nidx_protos.nodereader_pb2 import SearchRequest
from pytest_mock import MockerFixture

from nucliadb.search.search.plan import query


@pytest.mark.deploy_modes("standalone")
async def test_find_graph_request(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    mocker: MockerFixture,
):
    """Validate how /find prepares a graph search"""
    kbid = standalone_knowledgebox
    spy = mocker.spy(query, "node_query")

    # graph_query but missing features=graph
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "graph_query": {
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
    assert resp.status_code == 412
    assert spy.call_count == 0

    # features=graph but missing graph_query
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "features": ["graph"],
            "top_k": 100,
        },
    )
    assert resp.status_code == 412
    assert spy.call_count == 0

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "graph_query": {
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
            "features": ["graph"],
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    assert spy.call_count == 1
    pb_query = spy.call_args[0][2]
    assert isinstance(pb_query, SearchRequest)
    assert pb_query.graph_search.query.path.path.source.value == "Erin"
    assert pb_query.graph_search.query.path.path.source.node_subtype == "PERSON"
    assert pb_query.graph_search.query.path.path.destination.value == "UK"
    assert pb_query.graph_search.query.path.path.destination.node_subtype == "PLACE"


@pytest.mark.deploy_modes("standalone")
async def test_find_graph_feature(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    graph_resource: str,
    mocker: MockerFixture,
):
    """Validates graph feature for /find endpoint. We can query the graph with a
    custom query and paths with paragraph_id as metadata will be found and
    returned as text block matches.

    """

    kbid = standalone_knowledgebox
    spy = mocker.spy(query, "merge_shard_responses")

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "query": "Searching in my knowledge graph",
            "graph_query": {
                "prop": "path",
                "source": {
                    "value": "DiCaprio",
                    "match": "exact",
                },
                "undirected": True,
            },
            "features": ["keyword", "graph"],
            "top_k": 100,
            "reranker": "noop",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1
    assert graph_resource in body["resources"]
    assert len(body["resources"][graph_resource]["fields"]) == 3
    # graph search
    assert "/t/leo" in body["resources"][graph_resource]["fields"]
    assert (
        f"{graph_resource}/t/leo/0-70"
        in body["resources"][graph_resource]["fields"]["/t/leo"]["paragraphs"]
    )
    assert (
        body["resources"][graph_resource]["fields"]["/t/leo"]["paragraphs"][
            f"{graph_resource}/t/leo/0-70"
        ]["score_type"]
        == "RELATION_RELEVANCE"
    )
    # keyword search
    assert "/a/title" in body["resources"][graph_resource]["fields"]
    assert (
        body["resources"][graph_resource]["fields"]["/a/title"]["paragraphs"][
            f"{graph_resource}/a/title/0-15"
        ]["score_type"]
        == "BM25"
    )
    assert "/a/summary" in body["resources"][graph_resource]["fields"]
    assert (
        body["resources"][graph_resource]["fields"]["/a/summary"]["paragraphs"][
            f"{graph_resource}/a/summary/0-20"
        ]["score_type"]
        == "BM25"
    )

    assert spy.call_count == 1
    pb_responses = spy.call_args.args[0]
    assert len(pb_responses) == 1, "there's only 1 shard in this KB"
    pb_response = pb_responses[0]
    assert pb_response.HasField("graph")
    graph_response = pb_response.graph
    graph_response.nodes[graph_response.graph[0].source].value == "Leonardo DiCaprio"
    graph_response.relations[graph_response.graph[0].relation].label == "analogy"
    graph_response.nodes[graph_response.graph[0].destination].value == "DiCaprio"
