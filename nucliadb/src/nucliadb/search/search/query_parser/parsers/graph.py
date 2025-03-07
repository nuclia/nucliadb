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


from nucliadb.common.models_utils.from_proto import RelationNodeTypeMap
from nucliadb.search.search.query_parser.models import GraphRetrieval
from nucliadb_models.graph import requests as graph_requests
from nucliadb_protos import nodereader_pb2


def parse_graph_search(item: graph_requests.GraphSearchRequest) -> GraphRetrieval:
    pb = nodereader_pb2.GraphSearchRequest()
    pb.query.path.CopyFrom(_parse_path_query(item.query))
    pb.top_k = item.top_k
    return pb


def parse_graph_node_search(item: graph_requests.GraphNodesSearchRequest) -> GraphRetrieval:
    pb = nodereader_pb2.GraphSearchRequest()

    query = item.query

    if query.position == graph_requests.GraphNodePosition.ANY:
        _set_node_to_pb(query, pb.query.path.path.source)
        pb.query.path.path.undirected = True

    elif query.position == graph_requests.GraphNodePosition.SOURCE:
        _set_node_to_pb(query, pb.query.path.path.source)

    elif query.position == graph_requests.GraphNodePosition.DESTINATION:
        _set_node_to_pb(query, pb.query.path.path.destination)

    else:  # pragma: nocover
        # This is a trick so mypy generates an error if this branch can be reached,
        # that is, if we are missing some ifs
        _a: int = "a"

    pb.top_k = item.top_k
    return pb


def parse_graph_relation_search(
    item: graph_requests.GraphRelationsSearchRequest,
) -> GraphRetrieval:
    pb = nodereader_pb2.GraphSearchRequest()

    query = item.query

    relation = query
    if relation.label is not None:
        pb.query.path.path.relation.value = relation.label

    pb.top_k = item.top_k
    return pb


def _parse_path_query(expr: graph_requests.GraphPathQuery) -> nodereader_pb2.GraphQuery.PathQuery:
    pb = nodereader_pb2.GraphQuery.PathQuery()

    if isinstance(expr, graph_requests.And):
        for op in expr.operands:
            pb.bool_and.operands.append(_parse_path_query(op))

    elif isinstance(expr, graph_requests.Or):
        for op in expr.operands:
            pb.bool_or.operands.append(_parse_path_query(op))

    elif isinstance(expr, graph_requests.Not):
        pb.bool_not.CopyFrom(_parse_path_query(expr.operand))

    elif isinstance(expr, graph_requests.GraphPath):
        if expr.source is not None:
            _set_node_to_pb(expr.source, pb.path.source)

        if expr.destination is not None:
            _set_node_to_pb(expr.destination, pb.path.destination)

        if expr.relation is not None:
            relation = expr.relation
            if relation.label is not None:
                pb.path.relation.value = relation.label

        pb.path.undirected = expr.undirected

    else:  # pragma: nocover
        # This is a trick so mypy generates an error if this branch can be reached,
        # that is, if we are missing some ifs
        _a: int = "a"

    return pb


def _set_node_to_pb(node: graph_requests.GraphNode, pb: nodereader_pb2.GraphQuery.Node):
    if node.value is not None:
        pb.value = node.value
        if node.match == graph_requests.GraphNodeMatchKind.EXACT:
            pb.match_kind = nodereader_pb2.GraphQuery.Node.MatchKind.EXACT

        elif node.match == graph_requests.GraphNodeMatchKind.FUZZY:
            pb.match_kind = nodereader_pb2.GraphQuery.Node.MatchKind.FUZZY

        else:  # pragma: nocover
            # This is a trick so mypy generates an error if this branch can be reached,
            # that is, if we are missing some ifs
            _a: int = "a"

    if node.type is not None:
        pb.node_type = RelationNodeTypeMap[node.type]

    if node.group is not None:
        pb.node_subtype = node.group
