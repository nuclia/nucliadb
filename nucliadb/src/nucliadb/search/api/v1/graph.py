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

from nucliadb.common.models_utils.from_proto import RelationNodeTypePbMap
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search.query_parser.parsers import (
    parse_graph_node_search,
    parse_graph_relation_search,
    parse_graph_search,
)
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    GraphNode,
    GraphNodesSearchRequest,
    GraphNodesSearchResponse,
    GraphPath,
    GraphRelation,
    GraphRelationsSearchRequest,
    GraphRelationsSearchResponse,
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
    response_model_exclude_unset=True,
    include_in_schema=False,
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
    pb_query = await parse_graph_search(item)

    results, _, _ = await node_query(kbid, Method.GRAPH, pb_query)

    return build_graph_response(results)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/graph/nodes",
    status_code=200,
    summary="Search Knowledge Box graph nodes",
    description="Search on the Knowledge Box graph and retrieve nodes (vertices)",
    response_model_exclude_unset=True,
    include_in_schema=False,
    tags=["Search"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def graph_nodes_search_knowledgebox(
    request: Request,
    response: Response,
    kbid: str,
    item: GraphNodesSearchRequest,
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> GraphNodesSearchResponse:
    pb_query = await parse_graph_node_search(item)

    results, _, _ = await node_query(kbid, Method.GRAPH, pb_query)

    return build_graph_nodes_response(results)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/graph/relations",
    status_code=200,
    summary="Search Knowledge Box graph relations",
    description="Search on the Knowledge Box graph and retrieve relations (edges)",
    response_model_exclude_unset=True,
    include_in_schema=False,
    tags=["Search"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def graph_relations_search_knowledgebox(
    request: Request,
    response: Response,
    kbid: str,
    item: GraphRelationsSearchRequest,
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> GraphRelationsSearchResponse:
    pb_query = await parse_graph_relation_search(item)

    results, _, _ = await node_query(kbid, Method.GRAPH, pb_query)

    return build_graph_relations_response(results)


def build_graph_response(results: list[nodereader_pb2.GraphSearchResponse]) -> GraphSearchResponse:
    paths = []
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
                    label=relation.label,
                ),
                destination=GraphNode(
                    value=destination.value,
                ),
            )
            paths.append(path)

    response = GraphSearchResponse(paths=paths)
    return response


def build_graph_nodes_response(
    results: list[nodereader_pb2.GraphSearchResponse],
) -> GraphNodesSearchResponse:
    nodes = []
    for shard_results in results:
        for node in shard_results.nodes:
            nodes.append(
                GraphNode(
                    value=node.value,
                    type=RelationNodeTypePbMap[node.ntype],
                    group=node.subtype,
                )
            )
    response = GraphNodesSearchResponse(nodes=nodes)
    return response


def build_graph_relations_response(
    results: list[nodereader_pb2.GraphSearchResponse],
) -> GraphRelationsSearchResponse:
    relations = []
    for shard_results in results:
        for relation in shard_results.relations:
            relations.append(
                GraphRelation(
                    label=relation.label,
                )
            )
    response = GraphRelationsSearchResponse(relations=relations)
    return response
