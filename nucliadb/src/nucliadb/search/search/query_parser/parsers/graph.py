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

from typing import Optional, Union

from nidx_protos import nodereader_pb2

from nucliadb.common.filter_expression import add_and_expression, parse_expression
from nucliadb.common.models_utils.from_proto import RelationNodeTypeMap, RelationTypeMap
from nucliadb.search.search.query_parser.models import GraphRetrieval
from nucliadb.search.search.utils import filter_hidden_resources
from nucliadb_models.graph import requests as graph_requests
from nucliadb_models.labels import LABEL_HIDDEN
from nucliadb_protos import utils_pb2


async def parse_graph_search(kbid: str, item: graph_requests.GraphSearchRequest) -> GraphRetrieval:
    pb = await _parse_common(kbid, item)
    pb.query.path.CopyFrom(parse_path_query(item.query))
    pb.kind = nodereader_pb2.GraphSearchRequest.QueryKind.PATH
    return pb


async def parse_graph_node_search(
    kbid: str, item: graph_requests.GraphNodesSearchRequest
) -> GraphRetrieval:
    pb = await _parse_common(kbid, item)
    pb.query.path.CopyFrom(_parse_node_query(item.query))
    pb.kind = nodereader_pb2.GraphSearchRequest.QueryKind.NODES
    return pb


async def parse_graph_relation_search(
    kbid: str, item: graph_requests.GraphRelationsSearchRequest
) -> GraphRetrieval:
    pb = await _parse_common(kbid, item)
    pb.query.path.CopyFrom(_parse_relation_query(item.query))
    pb.kind = nodereader_pb2.GraphSearchRequest.QueryKind.RELATIONS
    return pb


AnyGraphRequest = Union[
    graph_requests.GraphSearchRequest,
    graph_requests.GraphNodesSearchRequest,
    graph_requests.GraphRelationsSearchRequest,
]


async def _parse_common(kbid: str, item: AnyGraphRequest) -> nodereader_pb2.GraphSearchRequest:
    pb = nodereader_pb2.GraphSearchRequest()
    pb.top_k = item.top_k

    filter_expr = await _parse_filters(kbid, item)
    if filter_expr is not None:
        pb.field_filter.CopyFrom(filter_expr)

    security = _parse_security(kbid, item)
    if security is not None:
        pb.security.CopyFrom(security)

    return pb


async def _parse_filters(kbid: str, item: AnyGraphRequest) -> Optional[nodereader_pb2.FilterExpression]:
    filter_expr = nodereader_pb2.FilterExpression()
    if item.filter_expression:
        if item.filter_expression.field:
            filter_expr = await parse_expression(item.filter_expression.field, kbid)

    hidden = await filter_hidden_resources(kbid, item.show_hidden)
    if hidden is not None:
        expr = nodereader_pb2.FilterExpression()
        if hidden:
            expr.facet.facet = LABEL_HIDDEN
        else:
            expr.bool_not.facet.facet = LABEL_HIDDEN

        add_and_expression(filter_expr, expr)

    if filter_expr.HasField("expr"):
        return filter_expr
    else:
        return None


def _parse_security(kbid: str, item: AnyGraphRequest) -> Optional[utils_pb2.Security]:
    if item.security is not None and len(item.security.groups) > 0:
        security_pb = utils_pb2.Security()
        for group_id in item.security.groups:
            if group_id not in security_pb.access_groups:
                security_pb.access_groups.append(group_id)
        return security_pb
    else:
        return None


def parse_path_query(expr: graph_requests.GraphPathQuery) -> nodereader_pb2.GraphQuery.PathQuery:
    pb = nodereader_pb2.GraphQuery.PathQuery()

    if isinstance(expr, graph_requests.And):
        for op in expr.operands:
            pb.bool_and.operands.append(parse_path_query(op))

    elif isinstance(expr, graph_requests.Or):
        for op in expr.operands:
            pb.bool_or.operands.append(parse_path_query(op))

    elif isinstance(expr, graph_requests.Not):
        pb.bool_not.CopyFrom(parse_path_query(expr.operand))

    elif isinstance(expr, graph_requests.GraphPath):
        if expr.source is not None:
            _set_node_to_pb(expr.source, pb.path.source)

        if expr.destination is not None:
            _set_node_to_pb(expr.destination, pb.path.destination)

        if expr.relation is not None:
            _set_relation_to_pb(expr.relation, pb.path.relation)

        pb.path.undirected = expr.undirected

    elif isinstance(expr, graph_requests.SourceNode):
        _set_node_to_pb(expr, pb.path.source)

    elif isinstance(expr, graph_requests.DestinationNode):
        _set_node_to_pb(expr, pb.path.destination)

    elif isinstance(expr, graph_requests.AnyNode):
        _set_node_to_pb(expr, pb.path.source)
        pb.path.undirected = True

    elif isinstance(expr, graph_requests.Relation):
        _set_relation_to_pb(expr, pb.path.relation)

    elif isinstance(expr, graph_requests.Generated):
        _set_generated_to_pb(expr, pb)

    else:  # pragma: nocover
        # This is a trick so mypy generates an error if this branch can be reached,
        # that is, if we are missing some ifs
        _a: int = "a"

    return pb


def _parse_node_query(expr: graph_requests.GraphNodesQuery) -> nodereader_pb2.GraphQuery.PathQuery:
    pb = nodereader_pb2.GraphQuery.PathQuery()

    if isinstance(expr, graph_requests.And):
        for op in expr.operands:
            pb.bool_and.operands.append(_parse_node_query(op))

    elif isinstance(expr, graph_requests.Or):
        for op in expr.operands:
            pb.bool_or.operands.append(_parse_node_query(op))

    elif isinstance(expr, graph_requests.Not):
        pb.bool_not.CopyFrom(_parse_node_query(expr.operand))

    elif isinstance(expr, graph_requests.AnyNode):
        _set_node_to_pb(expr, pb.path.source)
        pb.path.undirected = True

    elif isinstance(expr, graph_requests.Generated):
        _set_generated_to_pb(expr, pb)

    else:  # pragma: nocover
        # This is a trick so mypy generates an error if this branch can be reached,
        # that is, if we are missing some ifs
        _a: int = "a"

    return pb


def _parse_relation_query(
    expr: graph_requests.GraphRelationsQuery,
) -> nodereader_pb2.GraphQuery.PathQuery:
    pb = nodereader_pb2.GraphQuery.PathQuery()

    if isinstance(expr, graph_requests.And):
        for op in expr.operands:
            pb.bool_and.operands.append(_parse_relation_query(op))

    elif isinstance(expr, graph_requests.Or):
        for op in expr.operands:
            pb.bool_or.operands.append(_parse_relation_query(op))

    elif isinstance(expr, graph_requests.Not):
        pb.bool_not.CopyFrom(_parse_relation_query(expr.operand))

    elif isinstance(expr, graph_requests.Relation):
        _set_relation_to_pb(expr, pb.path.relation)

    elif isinstance(expr, graph_requests.Generated):
        _set_generated_to_pb(expr, pb)

    else:  # pragma: nocover
        # This is a trick so mypy generates an error if this branch can be reached,
        # that is, if we are missing some ifs
        _a: int = "a"

    return pb


def _set_node_to_pb(node: graph_requests.GraphNode, pb: nodereader_pb2.GraphQuery.Node):
    if node.value is not None:
        pb.value = node.value
        if node.match == graph_requests.NodeMatchKindName.EXACT:
            pb.exact.kind = nodereader_pb2.GraphQuery.Node.MatchLocation.FULL

        elif node.match == graph_requests.NodeMatchKindName.FUZZY:
            pb.fuzzy.kind = nodereader_pb2.GraphQuery.Node.MatchLocation.PREFIX
            pb.fuzzy.distance = 1

        else:  # pragma: nocover
            # This is a trick so mypy generates an error if this branch can be reached,
            # that is, if we are missing some ifs
            _a: int = "a"

    if node.type is not None:
        pb.node_type = RelationNodeTypeMap[node.type]

    if node.group is not None:
        pb.node_subtype = node.group


def _set_relation_to_pb(relation: graph_requests.GraphRelation, pb: nodereader_pb2.GraphQuery.Relation):
    if relation.label is not None:
        pb.value = relation.label
    if relation.type is not None:
        pb.relation_type = RelationTypeMap[relation.type]


def _set_generated_to_pb(generated: graph_requests.Generated, pb: nodereader_pb2.GraphQuery.PathQuery):
    if generated.by == graph_requests.Generator.USER:
        pb.facet.facet = "/g/u"

    elif generated.by == graph_requests.Generator.PROCESSOR:
        pb.bool_not.facet.facet = "/g"

    elif generated.by == graph_requests.Generator.DATA_AUGMENTATION:
        facet = "/g/da"
        if generated.da_task is not None:
            facet += f"/{generated.da_task}"

        pb.facet.facet = facet

    else:  # pragma: nocover
        # This is a trick so mypy generates an error if this branch can be reached,
        # that is, if we are missing some ifs
        _a: int = "a"
