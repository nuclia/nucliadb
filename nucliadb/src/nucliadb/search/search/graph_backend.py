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

from google.protobuf.json_format import ParseDict
from nidx_protos import nodereader_pb2

from nucliadb.common.marklogic import graph
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


_EXECUTOR = r"""
const op = require('/MarkLogic/optic');
const scope = cts.orQuery([
  cts.andQuery([cts.collectionQuery(COLLECTIONS.fieldsProcessed), securityQuery,
    ...(fieldDirs === null ? [] : [dirsQuery(fieldDirs)])]),
  typeof graphResourceQuery === 'undefined' ? cts.falseQuery() : graphResourceQuery
]);
const semanticCache = new Map();
function semanticValues(kind, vector) {
  const model = kind === 'node' ? nodeModel : edgeModel;
  const threshold = kind === 'node' ? nodeThreshold : edgeThreshold;
  const key = JSON.stringify([kind, model, vector]);
  if (semanticCache.has(key)) return semanticCache.get(key);
  const values = new Map();
  for (const doc of cts.search(scope, ['filtered', 'score-zero'])) {
    for (const entry of doc.root.toObject().graph_vectors || []) {
      if (entry.kind !== kind || entry.model_id !== model || entry.vector.length !== vector.length) continue;
      const score = 1 - vec.cosineDistance(vec.vector(entry.vector), vec.vector(vector));
      const value = kind === 'node' ? entry.normalized : entry.value;
      if (score >= threshold) values.set(value, values.has(value) ? Math.max(values.get(value), score) : score);
    }
  }
  semanticCache.set(key, values);
  return values;
}
function distance(left, right, prefix) {
  const matrix = Array.from({length: left.length + 1}, () => Array(right.length + 1).fill(0));
  for (let index = 0; index <= left.length; index++) matrix[index][0] = index;
  for (let index = 0; index <= right.length; index++) matrix[0][index] = index;
  for (let source = 1; source <= left.length; source++) {
    for (let target = 1; target <= right.length; target++) {
      matrix[source][target] = Math.min(matrix[source - 1][target] + 1,
        matrix[source][target - 1] + 1,
        matrix[source - 1][target - 1] + (left[source - 1] === right[target - 1] ? 0 : 1));
      if (source > 1 && target > 1 && left[source - 1] === right[target - 2] && left[source - 2] === right[target - 1]) {
        matrix[source][target] = Math.min(matrix[source][target], matrix[source - 2][target - 2] + 1);
      }
    }
  }
  return prefix ? Math.min(...matrix[left.length]) : matrix[left.length][right.length];
}
function nodeScore(node, row, position) {
  if (node.type !== undefined && node.type !== row[position + '_type']) return null;
  if (node.group !== undefined && node.group !== row[position + '_group']) return null;
  if (node.value === undefined) return 0;
  const value = row[position + '_normalized'];
  if (node.match === 'vector') {
    const score = semanticValues('node', node.vector).get(value);
    return score === undefined ? null : score;
  }
  const prefix = node.location === 1 || node.location === 3;
  const words = node.location === 2 || node.location === 3;
  if (words) {
    const terms = node.value.match(/[a-z0-9]+/g) || [];
    const tokens = value.match(/[a-z0-9]+/g) || [];
    if (!terms.length || !tokens.length) return null;
    const edits = terms.map(term => Math.min(...tokens.map(token => distance(term, token, prefix))));
    if (node.match === 'fuzzy') return edits.every(edit => edit <= node.distance) ? 1 / (1 + edits.reduce((sum, edit) => sum + edit, 0)) : null;
    return edits.some(edit => edit === 0) ? 1 : null;
  }
  if (node.match === 'fuzzy') {
    const edits = distance(node.value, value, prefix);
    return edits <= node.distance ? 1 / (1 + edits) : null;
  }
  return (prefix ? value.startsWith(node.value) : value === node.value) ? 1 : null;
}
function relationScore(relation, row) {
  if (relation.type !== undefined && relation.type !== row.relation_type) return null;
  if (relation.vector) {
    const score = semanticValues('edge', relation.vector).get(row.label);
    return score === undefined ? null : score;
  }
  return relation.value === undefined ? 0 : relation.value === row.label ? 1 : null;
}
function pathScore(path, row, reverse) {
  const scores = [path.source ? nodeScore(path.source, row, reverse ? 'destination' : 'source') : 0,
    path.destination ? nodeScore(path.destination, row, reverse ? 'source' : 'destination') : 0,
    path.relation ? relationScore(path.relation, row) : 0];
  return scores.some(score => score === null) ? null : scores.reduce((sum, score) => sum + score, 0);
}
function evaluate(query, row, position) {
  if (query.bool_and) {
    const scores = query.bool_and.map(operand => evaluate(operand, row, position));
    return scores.some(score => score === null) ? null : scores.reduce((sum, score) => sum + score, 0);
  }
  if (query.bool_or) {
    const scores = query.bool_or.map(operand => evaluate(operand, row, position)).filter(score => score !== null);
    return scores.length ? Math.max(...scores) : null;
  }
  if (query.bool_not) return evaluate(query.bool_not, row, position) === null ? 0 : null;
  if (query.facet !== undefined) return JSON.parse(row.facets_json).some(facet => facet === query.facet || facet.startsWith(query.facet + '/')) ? 0 : null;
  if (!query.path) return null;
  if (position) return nodeScore(query.path.source || query.path.destination || {}, row, position);
  const forward = pathScore(query.path, row, false);
  if (!query.path.undirected) return forward;
  const reverse = pathScore(query.path, row, true);
  return forward === null ? reverse : reverse === null ? forward : Math.max(forward, reverse);
}
function combine(operation, expressions) {
  if (!expressions.length) return op.eq(1, operation === 'and' ? 1 : 0);
  return expressions.reduce((left, right) => operation === 'and' ? op.and(left, right) : op.or(left, right));
}
function nodePredicate(node, position) {
  const predicates = [];
  if (node.type !== undefined) predicates.push(op.eq(op.col(position + '_type'), node.type));
  if (node.group !== undefined) predicates.push(op.eq(op.col(position + '_group'), node.group));
  if (node.value !== undefined && node.match === 'exact' && node.location === 0) predicates.push(op.eq(op.col(position + '_normalized'), node.value));
  if (node.match === 'fuzzy' && node.distance === 1 && (node.location === 0 || node.location === 1)) {
    const reference = cts.pathReference('/relations/' + position + '_normalized', ['type=string', 'collation=' + COLLATION]);
    const values = new Set();
    for (const pattern of node.patterns) {
      for (const value of cts.valueMatch(reference, pattern, ['case-sensitive', 'diacritic-sensitive'], scope)) values.add(String(value));
    }
    predicates.push(combine('or', Array.from(values).map(value => op.eq(op.col(position + '_normalized'), value))));
  }
  if (node.match === 'vector') predicates.push(combine('or', Array.from(semanticValues('node', node.vector).keys()).map(value => op.eq(op.col(position + '_normalized'), value))));
  return combine('and', predicates);
}
function pathPredicate(path, reverse) {
  const predicates = [];
  if (path.source) predicates.push(nodePredicate(path.source, reverse ? 'destination' : 'source'));
  if (path.destination) predicates.push(nodePredicate(path.destination, reverse ? 'source' : 'destination'));
  if (path.relation) {
    if (path.relation.type !== undefined) predicates.push(op.eq(op.col('relation_type'), path.relation.type));
    if (path.relation.vector) predicates.push(combine('or', Array.from(semanticValues('edge', path.relation.vector).keys()).map(value => op.eq(op.col('label'), value))));
    else if (path.relation.value !== undefined) predicates.push(op.eq(op.col('label'), path.relation.value));
  }
  return combine('and', predicates);
}
function predicate(query, position) {
  if (query.bool_and) return combine('and', query.bool_and.map(operand => predicate(operand, position)));
  if (query.bool_or) return combine('or', query.bool_or.map(operand => predicate(operand, position)));
  if (!query.path) return op.eq(1, 1);
  if (position) return nodePredicate(query.path.source || query.path.destination || {}, position);
  const forward = pathPredicate(query.path, false);
  return query.path.undirected ? op.or(forward, pathPredicate(query.path, true)) : forward;
}
let results = [];
for (const position of (kind === 1 ? ['source', 'destination'] : [null])) {
  const rows = op.fromView('nucliadb', 'relations', '', op.fragmentIdCol('fid'))
    .where(scope).where(predicate(query, position)).select([
      'resource_field_id', 'occurrence', 'source_value', 'source_normalized', 'source_type', 'source_group',
      'destination_value', 'destination_normalized', 'destination_type', 'destination_group',
      'label', 'relation_type', 'facets_json', 'metadata_json'
    ]).result();
  for (const row of rows) {
    const score = evaluate(query, row, position);
    if (score !== null) results.push(Object.assign({}, row, {score, position}));
  }
}
results.sort((left, right) => right.score - left.score ||
  String(left.resource_field_id).localeCompare(String(right.resource_field_id)) || left.occurrence - right.occurrence);
if (kind !== 0) {
  const seen = new Set();
  results = results.filter(row => {
    const key = kind === 1 ? JSON.stringify([row[row.position + '_value'], row[row.position + '_type'], row[row.position + '_group']]) : JSON.stringify([row.label, row.relation_type]);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}
return results.slice(0, limit);
"""


def graph_expression(request: nodereader_pb2.GraphSearchRequest) -> str:
    if not request.HasField("query") or not request.query.HasField("path") or not request.top_k:
        return "[]"
    return (
        "(() => {"
        + " ".join(
            [
                f"const query = {json.dumps(_query(request.query.path))};",
                f"const kind = {request.kind}; const limit = {request.top_k};",
                f"const nodeModel = {json.dumps(request.graph_node_vectorset)};",
                f"const edgeModel = {json.dumps(request.graph_edge_vectorset)};",
                f"const nodeThreshold = {request.min_score_node_semantic};",
                f"const edgeThreshold = {request.min_score_edge_semantic};",
                _EXECUTOR,
            ]
        )
        + "})()"
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
