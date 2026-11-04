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
"""Single-hop queries over the relation-occurrence TDE view."""

import json
from collections.abc import Callable
from typing import Any

from google.protobuf.json_format import ParseDict
from nidx_protos import nodereader_pb2

from nucliadb.common.marklogic import graph
from nucliadb.search.search.query_templates import load_template, render_template
from nucliadb_protos import utils_pb2


def fuzzy_patterns(value: str, prefix: bool) -> list[str]:
    characters = ["\\" + character if character in "*?\\" else character for character in value]
    patterns = {"".join(characters)}
    for index in range(len(characters)):
        patterns.add("".join([*characters[:index], "?", *characters[index + 1 :]]))
        patterns.add("".join(characters[:index] + characters[index + 1 :]))
        patterns.add("".join([*characters[:index], "?", *characters[index:]]))
        if index + 1 < len(characters):
            swapped = characters.copy()
            swapped[index], swapped[index + 1] = swapped[index + 1], swapped[index]
            patterns.add("".join(swapped))
    patterns.add("".join(characters) + "?")
    return sorted(pattern + ("*" if prefix else "") for pattern in patterns)


def _node(node) -> dict:
    result = {}
    if node.HasField("node_type"):
        result["type"] = node.node_type
    if node.HasField("node_subtype") and node.node_subtype:
        result["group"] = node.node_subtype
    if node.HasField("value") and node.value:
        result["value"] = graph.normalize(node.value)
        kind = node.WhichOneof("match_kind") or "exact"
        result["match"] = kind
        if kind == "vector":
            result["vector"] = list(node.vector.vector)
        else:
            match = getattr(node, kind)
            result["location"] = match.kind
            if kind == "fuzzy":
                result["distance"] = match.distance
                result["patterns"] = fuzzy_patterns(result["value"], match.kind == 1)
    return result


def _query(query) -> dict:
    kind = query.WhichOneof("query")
    if kind in ("bool_and", "bool_or"):
        return {kind: [_query(operand) for operand in getattr(query, kind).operands]}
    if kind == "bool_not":
        return {kind: _query(query.bool_not)}
    if kind == "facet":
        return {kind: query.facet.facet}
    if kind != "path":
        return {}
    path = query.path
    content = {"undirected": path.undirected}
    for position in ("source", "destination"):
        if path.HasField(position):
            content[position] = _node(getattr(path, position))
    if path.HasField("relation"):
        relation = path.relation
        constraint = {}
        if relation.HasField("relation_type"):
            constraint["type"] = relation.relation_type
        if relation.HasField("value"):
            constraint["value"] = relation.value
        if relation.WhichOneof("match_kind") == "vector":
            constraint["vector"] = list(relation.vector.vector)
        content["relation"] = constraint
    return {"path": content}


def graph_expression(
    request: nodereader_pb2.GraphSearchRequest, *, literal: Callable[[Any], str] = json.dumps
) -> str:
    if not request.HasField("query") or not request.query.HasField("path") or not request.top_k:
        return "[]"
    return render_template(
        "graph_expression.js",
        query=literal(_query(request.query.path)),
        kind=literal(request.kind),
        limit=literal(request.top_k),
        node_model=literal(request.graph_node_vectorset),
        edge_model=literal(request.graph_edge_vectorset),
        node_threshold=literal(request.min_score_node_semantic),
        edge_threshold=literal(request.min_score_edge_semantic),
        executor=load_template("graph_executor.js"),
    )


def append_graph_results(response: nodereader_pb2.GraphSearchResponse, matches: list[dict]) -> None:
    def node(row, position):
        return utils_pb2.RelationNode(
            value=row[position + "_value"],
            ntype=utils_pb2.RelationNode.NodeType.Value(
                utils_pb2.RelationNode.NodeType.DESCRIPTOR.values_by_number[
                    int(row[position + "_type"])
                ].name
            ),
            subtype=row[position + "_group"],
        )

    for row in matches:
        response.scores.append(float(row["score"]))
        if row.get("position"):
            response.nodes.append(node(row, row["position"]))
            continue
        relation = nodereader_pb2.GraphSearchResponse.Relation(
            label=row["label"],
            relation_type=utils_pb2.Relation.RelationType.Value(
                utils_pb2.Relation.RelationType.DESCRIPTOR.values_by_number[
                    int(row["relation_type"])
                ].name
            ),
        )
        if "source_value" not in row:
            response.relations.append(relation)
            continue
        source = len(response.nodes)
        response.nodes.extend([node(row, "source"), node(row, "destination")])
        relation_index = len(response.relations)
        response.relations.append(relation)
        path = response.graph.add(
            source=source,
            destination=source + 1,
            relation=relation_index,
            resource_field_id=row["resource_field_id"],
            facets=json.loads(row["facets_json"]),
        )
        ParseDict(json.loads(row["metadata_json"]), path.metadata)
