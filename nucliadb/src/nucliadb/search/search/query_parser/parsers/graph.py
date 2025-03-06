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

# TODO: remove repetition by adding some functions to convert query
# nodes/relations to pb nodes/relations


def parse_graph_search(item: graph_requests.GraphSearchRequest) -> GraphRetrieval:
    pb = nodereader_pb2.GraphSearchRequest()

    query = item.query
    if query.source is not None:
        source = query.source
        if source.value is not None:
            pb.query.path.source.value = source.value
        if source.type is not None:
            pb.query.path.source.node_type = RelationNodeTypeMap[source.type]
        if source.group is not None:
            pb.query.path.source.node_subtype = source.group

    if query.destination is not None:
        destination = query.destination
        if destination.value is not None:
            pb.query.path.destination.value = destination.value
        if destination.type is not None:
            pb.query.path.destination.node_type = RelationNodeTypeMap[destination.type]
        if destination.group is not None:
            pb.query.path.destination.node_subtype = destination.group

    if query.relation is not None:
        relation = query.relation
        if relation.label is not None:
            pb.query.path.relation.value = relation.label

    pb.query.path.undirected = query.undirected

    pb.top_k = item.top_k
    return pb


def parse_graph_node_search(item: graph_requests.GraphNodesSearchRequest) -> GraphRetrieval:
    pb = nodereader_pb2.GraphSearchRequest()

    query = item.query

    if query.position == graph_requests.GraphNodePosition.ANY:
        if query.value is not None:
            pb.query.node.value = query.value
        if query.type is not None:
            pb.query.node.node_type = RelationNodeTypeMap[query.type]
        if query.group is not None:
            pb.query.node.node_subtype = query.group

    elif query.position == graph_requests.GraphNodePosition.SOURCE:
        source = query
        if source.value is not None:
            pb.query.path.source.value = source.value
        if source.type is not None:
            pb.query.path.source.node_type = RelationNodeTypeMap[source.type]
        if source.group is not None:
            pb.query.path.source.node_subtype = source.group

    elif query.position == graph_requests.GraphNodePosition.DESTINATION:
        destination = query
        if destination.value is not None:
            pb.query.path.destination.value = destination.value
        if destination.type is not None:
            pb.query.path.destination.node_type = RelationNodeTypeMap[destination.type]
        if destination.group is not None:
            pb.query.path.destination.node_subtype = destination.group

    else:  # pragma: nocover
        raise ValueError(f"Unknown graph node position: {query.position}")

    pb.top_k = item.top_k
    return pb


def parse_graph_relation_search(
    item: graph_requests.GraphRelationsSearchRequest,
) -> GraphRetrieval:
    pb = nodereader_pb2.GraphSearchRequest()

    query = item.query

    relation = query
    if relation.label is not None:
        pb.query.path.relation.value = relation.label

    pb.top_k = item.top_k
    return pb
