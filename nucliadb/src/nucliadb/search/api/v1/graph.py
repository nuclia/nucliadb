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
from nucliadb.search.search.graph_merge import (
    build_graph_nodes_response,
    build_graph_relations_response,
    build_graph_response,
)
from nucliadb.search.search.query_parser.parsers import (
    parse_graph_node_search,
    parse_graph_relation_search,
    parse_graph_search,
)
from nucliadb_models.graph.requests import (
    GraphNodesSearchRequest,
    GraphRelationsSearchRequest,
    GraphSearchRequest,
)
from nucliadb_models.graph.responses import (
    GraphNodesSearchResponse,
    GraphRelationsSearchResponse,
    GraphSearchResponse,
)
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    NucliaDBClientType,
)
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
    pb_query = await parse_graph_search(kbid, item)

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
    pb_query = await parse_graph_node_search(kbid, item)

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
    pb_query = await parse_graph_relation_search(kbid, item)

    results, _, _ = await node_query(kbid, Method.GRAPH, pb_query)

    return build_graph_relations_response(results)
