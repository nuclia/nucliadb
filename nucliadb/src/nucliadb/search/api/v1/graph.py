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
from fastapi import Header, Request, Response
from fastapi_versioning import version

from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search.query_parser.models import GraphRetrieval
from nucliadb.search.search.query_parser.parsers import parse_graph_search
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    GraphNode,
    GraphPath,
    GraphRelation,
    GraphSearchRequest,
    GraphSearchResponse,
    NucliaDBClientType,
)
from nucliadb_protos import nodereader_pb2
from nucliadb_utils.authentication import requires


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/graph",
    status_code=200,
    summary="Search Knowledge Box graph",
    description="Search on the Knowledge Box graph and retrieve triplets of vertex-edge-vertex",
    response_model=GraphSearchResponse,
    response_model_exclude_unset=True,
    tags=["Search"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def graph_search_knowledgebox(
    request: Request,
    response: Response,
    kbid: str,
    item: GraphSearchRequest,
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> GraphSearchResponse:
    return await _graph_search(
        kbid,
        item,
        x_ndb_client=x_ndb_client,
        x_nucliadb_user=x_nucliadb_user,
        x_forwarded_for=x_forwarded_for,
    )


async def _graph_search(
    kbid: str,
    item: GraphSearchRequest,
    *,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
) -> GraphSearchResponse:
    parsed = await parse_graph_search(kbid, item)
    pb_query = graph_retrieval_to_pb(parsed)

    results, _, _ = await node_query(kbid, Method.GRAPH, pb_query)

    response = build_graph_response(results)
    return response


def graph_retrieval_to_pb(payload: GraphRetrieval) -> nodereader_pb2.GraphSearchRequest:
    pb = nodereader_pb2.GraphSearchRequest()

    # path query
    pb.query.path.source.value = payload.query.source.value
    pb.query.path.relation.value = payload.query.relation.value
    pb.query.path.destination.value = payload.query.destination.value

    pb.top_k = payload.top_k
    print(f"Graph request: {pb}")
    return pb


def build_graph_response(results: list[nodereader_pb2.GraphSearchResponse]) -> GraphSearchResponse:
    print(f"Graph results: {results}")
    response = GraphSearchResponse(
        paths=[],
    )

    for shard_results in results:
        for pb_path in shard_results.graph:
            source = shard_results.nodes[pb_path.source]
            relation = shard_results.relations[pb_path.relation]
            destination = shard_results.nodes[pb_path.destination]

            path = GraphPath(
                source=GraphNode(
                    value=source.value,
                ),
                relation=GraphRelation(
                    value=relation.label,
                ),
                destination=GraphNode(
                    value=destination.value,
                ),
            )
            response.paths.append(path)

    return response
