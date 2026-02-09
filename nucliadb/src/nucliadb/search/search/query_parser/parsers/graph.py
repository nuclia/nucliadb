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


from dataclasses import dataclass

from nidx_protos import nodereader_pb2
from typing_extensions import assert_never

from nucliadb.common import datamanagers
from nucliadb.common.filter_expression import add_and_expression, parse_expression
from nucliadb.common.models_utils.from_proto import RelationNodeTypeMap, RelationTypeMap
from nucliadb.search.predict_models import QueryModel
from nucliadb.search.search.query_parser.models import GraphRetrieval
from nucliadb.search.search.utils import filter_hidden_resources
from nucliadb.search.utilities import get_predict
from nucliadb_models import filters
from nucliadb_models.graph import requests as graph_requests
from nucliadb_models.labels import LABEL_HIDDEN
from nucliadb_protos import utils_pb2


async def parse_graph_search(kbid: str, item: graph_requests.GraphSearchRequest) -> GraphRetrieval:
    vectors = await _calculate_graph_vectors(kbid, item)

    pb = await _parse_common(kbid, item)
    pb.query.path.CopyFrom(parse_path_query(item.query, vectors.node_vectors, vectors.relation_vectors))
    pb.kind = nodereader_pb2.GraphSearchRequest.QueryKind.PATH
    if vectors.node_vectorset:
        pb.graph_node_vectorset = vectors.node_vectorset
    if vectors.relation_vectorset:
        pb.graph_edge_vectorset = vectors.relation_vectorset
    return pb


async def parse_graph_node_search(
    kbid: str, item: graph_requests.GraphNodesSearchRequest
) -> GraphRetrieval:
    vectors = await _calculate_graph_vectors(kbid, item)

    pb = await _parse_common(kbid, item)
    pb.query.path.CopyFrom(_parse_node_query(item.query, vectors.node_vectors))
    pb.kind = nodereader_pb2.GraphSearchRequest.QueryKind.NODES
    if vectors.node_vectorset:
        pb.graph_node_vectorset = vectors.node_vectorset
    return pb


async def parse_graph_relation_search(
    kbid: str, item: graph_requests.GraphRelationsSearchRequest
) -> GraphRetrieval:
    vectors = await _calculate_graph_vectors(kbid, item)

    pb = await _parse_common(kbid, item)
    pb.query.path.CopyFrom(_parse_relation_query(item.query, vectors.relation_vectors))
    pb.kind = nodereader_pb2.GraphSearchRequest.QueryKind.RELATIONS
    if vectors.relation_vectorset:
        pb.graph_edge_vectorset = vectors.relation_vectorset
    return pb


AnyGraphRequest = (
    graph_requests.GraphSearchRequest
    | graph_requests.GraphNodesSearchRequest
    | graph_requests.GraphRelationsSearchRequest
)


async def _parse_common(kbid: str, item: AnyGraphRequest) -> nodereader_pb2.GraphSearchRequest:
    pb = nodereader_pb2.GraphSearchRequest()
    pb.top_k = item.top_k

    filter_expr = await _parse_filters(kbid, item)
    if filter_expr is not None:
        pb.field_filter.CopyFrom(filter_expr)

    security = _parse_security(kbid, item)
    if security is not None:
        pb.security.CopyFrom(security)

    # TODO: Use proper vectorsets
    # Maybe this could come from the call to predict API?
    vss = []
    async with datamanagers.with_ro_transaction() as txn:
        async for vs_id, vs_config in datamanagers.vectorsets.iter(txn, kbid=kbid):
            vss.append(vs_id)

    return pb


async def _parse_filters(kbid: str, item: AnyGraphRequest) -> nodereader_pb2.FilterExpression | None:
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


def _parse_security(kbid: str, item: AnyGraphRequest) -> utils_pb2.Security | None:
    if item.security is not None and len(item.security.groups) > 0:
        security_pb = utils_pb2.Security()
        for group_id in item.security.groups:
            if group_id not in security_pb.access_groups:
                security_pb.access_groups.append(group_id)
        return security_pb
    else:
        return None


def parse_path_query(
    expr: graph_requests.GraphPathQuery,
    node_vectors: dict[str, list[float]],
    relation_vectors: dict[str, list[float]],
) -> nodereader_pb2.GraphQuery.PathQuery:
    pb = nodereader_pb2.GraphQuery.PathQuery()

    if isinstance(expr, graph_requests.And):
        for op in expr.operands:
            pb.bool_and.operands.append(parse_path_query(op, node_vectors, relation_vectors))

    elif isinstance(expr, graph_requests.Or):
        for op in expr.operands:
            pb.bool_or.operands.append(parse_path_query(op, node_vectors, relation_vectors))

    elif isinstance(expr, graph_requests.Not):
        pb.bool_not.CopyFrom(parse_path_query(expr.operand, node_vectors, relation_vectors))

    elif isinstance(expr, graph_requests.GraphPath):
        if expr.source is not None:
            _set_node_to_pb(expr.source, pb.path.source, node_vectors)

        if expr.destination is not None:
            _set_node_to_pb(expr.destination, pb.path.destination, node_vectors)

        if expr.relation is not None:
            _set_relation_to_pb(expr.relation, pb.path.relation, relation_vectors)

        pb.path.undirected = expr.undirected

    elif isinstance(expr, graph_requests.SourceNode):
        _set_node_to_pb(expr, pb.path.source, node_vectors)

    elif isinstance(expr, graph_requests.DestinationNode):
        _set_node_to_pb(expr, pb.path.destination, node_vectors)

    elif isinstance(expr, graph_requests.AnyNode):
        _set_node_to_pb(expr, pb.path.source, node_vectors)
        pb.path.undirected = True

    elif isinstance(expr, graph_requests.Relation):
        _set_relation_to_pb(expr, pb.path.relation, relation_vectors)

    elif isinstance(expr, graph_requests.Generated):
        _set_generated_to_pb(expr, pb)

    else:  # pragma: no cover
        assert_never(expr)

    return pb


def _parse_node_query(
    expr: graph_requests.GraphNodesQuery, node_vectors: dict[str, list[float]]
) -> nodereader_pb2.GraphQuery.PathQuery:
    pb = nodereader_pb2.GraphQuery.PathQuery()

    if isinstance(expr, graph_requests.And):
        for op in expr.operands:
            pb.bool_and.operands.append(_parse_node_query(op, node_vectors))

    elif isinstance(expr, graph_requests.Or):
        for op in expr.operands:
            pb.bool_or.operands.append(_parse_node_query(op, node_vectors))

    elif isinstance(expr, graph_requests.Not):
        pb.bool_not.CopyFrom(_parse_node_query(expr.operand, node_vectors))

    elif isinstance(expr, graph_requests.AnyNode):
        _set_node_to_pb(expr, pb.path.source, node_vectors)
        pb.path.undirected = True

    elif isinstance(expr, graph_requests.Generated):
        _set_generated_to_pb(expr, pb)

    else:  # pragma: no cover
        assert_never(expr)

    return pb


def _parse_relation_query(
    expr: graph_requests.GraphRelationsQuery,
    relation_vectors: dict[str, list[float]],
) -> nodereader_pb2.GraphQuery.PathQuery:
    pb = nodereader_pb2.GraphQuery.PathQuery()

    if isinstance(expr, graph_requests.And):
        for op in expr.operands:
            pb.bool_and.operands.append(_parse_relation_query(op, relation_vectors))

    elif isinstance(expr, graph_requests.Or):
        for op in expr.operands:
            pb.bool_or.operands.append(_parse_relation_query(op, relation_vectors))

    elif isinstance(expr, graph_requests.Not):
        pb.bool_not.CopyFrom(_parse_relation_query(expr.operand, relation_vectors))

    elif isinstance(expr, graph_requests.Relation):
        _set_relation_to_pb(expr, pb.path.relation, relation_vectors)

    elif isinstance(expr, graph_requests.Generated):
        _set_generated_to_pb(expr, pb)

    else:  # pragma: no cover
        assert_never(expr)

    return pb


def _set_node_to_pb(
    node: graph_requests.GraphNode,
    pb: nodereader_pb2.GraphQuery.Node,
    node_vectors: dict[str, list[float]],
):
    if node.value is not None:
        pb.value = node.value
        if node.match == graph_requests.NodeMatchKindName.EXACT:
            pb.exact.kind = nodereader_pb2.GraphQuery.Node.MatchLocation.FULL

        elif node.match == graph_requests.NodeMatchKindName.FUZZY:
            pb.fuzzy.kind = nodereader_pb2.GraphQuery.Node.MatchLocation.PREFIX
            pb.fuzzy.distance = 1

        elif node.match == graph_requests.NodeMatchKindName.FUZZY_WORDS:
            pb.fuzzy.kind = nodereader_pb2.GraphQuery.Node.MatchLocation.WORDS
            pb.fuzzy.distance = 1

        elif node.match == graph_requests.NodeMatchKindName.SEMANTIC:
            pb.vector.vector.extend(node_vectors[node.value])

        else:  # pragma: no cover
            assert_never(node.match)

    if node.type is not None:
        pb.node_type = RelationNodeTypeMap[node.type]

    if node.group is not None:
        pb.node_subtype = node.group


def _set_relation_to_pb(
    relation: graph_requests.GraphRelation,
    pb: nodereader_pb2.GraphQuery.Relation,
    relation_vectors: dict[str, list[float]],
):
    if relation.label is not None:
        pb.value = relation.label
    if relation.type is not None:
        pb.relation_type = RelationTypeMap[relation.type]

    if relation.label is not None:
        pb.exact.SetInParent()
        # TODO(semantic-graph): Disabled for now, not supported by NUA/processor yet
        # match relation.match:
        #     case graph_requests.RelationMatchKindName.EXACT:
        #         pb.exact.SetInParent()
        #     case graph_requests.RelationMatchKindName.SEMANTIC:
        #         pb.vector.vector.extend(relation_vectors[relation.label])
        #     case _:  # pragma: no cover
        #         assert_never(relation.match)


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

    else:  # pragma: no cover
        assert_never(generated.by)


@dataclass
class GraphVectors:
    node_vectors: dict[str, list[float]]
    relation_vectors: dict[str, list[float]]
    node_vectorset: str | None
    relation_vectorset: str | None


async def _calculate_graph_vectors(
    kbid: str,
    request: AnyGraphRequest,
) -> GraphVectors:
    nodes: set[str] = set()
    relations: set[str] = set()
    _extract_semantic_terms(request.query, nodes, relations)

    node_vectors = {}
    relation_vectors: dict[str, list[float]] = {}
    node_vectorset: str = ""
    relation_vectoset: str = ""

    # TODO(semantic-graph): Disabled relations for now, not supported by NUA/processor yet
    # TODO(semantic-graph): Assumming a single vectorset for now
    predict = get_predict()
    result = await predict.query(kbid, QueryModel(graph_nodes=list(nodes)))
    if result.graph_nodes:
        node_vectorset = next(iter(result.graph_nodes.vectors.keys()))
        for node in nodes:
            node_vectors[node] = result.graph_nodes.vectors[node_vectorset][node]

    return GraphVectors(
        node_vectors=node_vectors,
        relation_vectors=relation_vectors,
        node_vectorset=node_vectorset,
        relation_vectorset=relation_vectoset,
    )


def _extract_semantic_terms(
    query: graph_requests.GraphPathQuery
    | graph_requests.GraphNodesQuery
    | graph_requests.GraphRelationsQuery
    | graph_requests.GraphNode
    | graph_requests.GraphRelation
    | None,
    nodes: set[str],
    relations: set[str],
):
    match query:
        case filters.And() | filters.Or():
            for op in query.operands:
                _extract_semantic_terms(op, nodes, relations)
        case filters.Not():
            _extract_semantic_terms(query.operand, nodes, relations)
        case graph_requests.GraphPath():
            _extract_semantic_terms(query.source, nodes, relations)
            _extract_semantic_terms(query.relation, nodes, relations)
            _extract_semantic_terms(query.destination, nodes, relations)
        case graph_requests.GraphNode():
            if query.match == graph_requests.NodeMatchKindName.SEMANTIC and query.value:
                nodes.add(query.value)
        case graph_requests.GraphRelation():
            pass
            # TODO(semantic-graph): Disabled for now, not supported by NUA/processor yet
            # if query.match == graph_requests.RelationMatchKindName.SEMANTIC and query.label:
            #     relations.add(query.label)
        case graph_requests.Generated():
            pass
        case None:
            pass
        case _:
            assert_never(query)
