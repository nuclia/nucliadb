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
from nucliadb.search.api.v1.utils import get_injected_security_groups
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
from nucliadb_models.security import RequestSecurity
from nucliadb_utils.authentication import requires


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/graph",
    status_code=200,
    summary="Search Knowledge Box graph",
    description="Search on the Knowledge Box graph and retrieve triplets of vertex-edge-vertex",
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
    # TODO: audit this request!
    # Backend-injected security groups (e.g. from a service account) always
    # take priority over any groups the client supplied.
    injected_groups = get_injected_security_groups(request)
    if injected_groups is not None:
        item.security = RequestSecurity(groups=injected_groups)
    return await graph_path_search(kbid, item)


async def graph_path_search(kbid: str, item: GraphSearchRequest) -> GraphSearchResponse:
    # TODO(Marklogic): implement graph
    return GraphSearchResponse(paths=[])


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/graph/nodes",
    status_code=200,
    summary="Search Knowledge Box graph nodes",
    description="Search on the Knowledge Box graph and retrieve nodes (vertices)",
    response_model_exclude_unset=True,
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
    # TODO: audit this request!
    # Backend-injected security groups (e.g. from a service account) always
    # take priority over any groups the client supplied.
    injected_groups = get_injected_security_groups(request)
    if injected_groups is not None:
        item.security = RequestSecurity(groups=injected_groups)
    return await graph_nodes_search(kbid, item)


async def graph_nodes_search(kbid: str, item: GraphNodesSearchRequest) -> GraphNodesSearchResponse:
    # TODO(Marklogic): implement graph
    return GraphNodesSearchResponse(nodes=[])


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/graph/relations",
    status_code=200,
    summary="Search Knowledge Box graph relations",
    description="Search on the Knowledge Box graph and retrieve relations (edges)",
    response_model_exclude_unset=True,
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
    # TODO: audit this request!
    # Backend-injected security groups (e.g. from a service account) always
    # take priority over any groups the client supplied.
    injected_groups = get_injected_security_groups(request)
    if injected_groups is not None:
        item.security = RequestSecurity(groups=injected_groups)
    return await graph_relations_search(kbid, item)


async def graph_relations_search(
    kbid: str, item: GraphRelationsSearchRequest
) -> GraphRelationsSearchResponse:
    # TODO(Marklogic): implement graph
    return GraphRelationsSearchResponse(relations=[])
