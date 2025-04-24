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


from nidx_protos import nodereader_pb2

from nucliadb.common.models_utils.from_proto import RelationNodeTypePbMap, RelationTypePbMap
from nucliadb_models.graph import responses as graph_responses
from nucliadb_models.graph.responses import (
    GraphNodesSearchResponse,
    GraphRelationsSearchResponse,
    GraphSearchResponse,
)


def build_graph_response(results: list[nodereader_pb2.GraphSearchResponse]) -> GraphSearchResponse:
    paths = []
    for shard_results in results:
        for pb_path in shard_results.graph:
            source = shard_results.nodes[pb_path.source]
            relation = shard_results.relations[pb_path.relation]
            destination = shard_results.nodes[pb_path.destination]

            path = graph_responses.GraphPath(
                source=graph_responses.GraphNode(
                    value=source.value,
                    type=RelationNodeTypePbMap[source.ntype],
                    group=source.subtype,
                ),
                relation=graph_responses.GraphRelation(
                    label=relation.label,
                    type=RelationTypePbMap[relation.relation_type],
                ),
                destination=graph_responses.GraphNode(
                    value=destination.value,
                    type=RelationNodeTypePbMap[destination.ntype],
                    group=destination.subtype,
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
                graph_responses.GraphNode(
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
                graph_responses.GraphRelation(
                    label=relation.label,
                    type=RelationTypePbMap[relation.relation_type],
                )
            )
    response = GraphRelationsSearchResponse(relations=relations)
    return response
