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

import json

from google.protobuf.json_format import ParseDict
from nidx_protos.nodereader_pb2 import (
    DocumentScored,
    DocumentVectorIdentifier,
    GraphSearchRequest,
    GraphSearchResponse,
    ParagraphResult,
    SearchRequest,
    SearchResponse,
    SuggestRequest,
    SuggestResponse,
)

from nucliadb.common.ids import ParagraphId
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.marklogic import MarkLogicDriver
from nucliadb.common.maindb.utils import get_driver
from nucliadb.common.marklogic import documents
from nucliadb.search.search.keyword_terms import keyword_terms


def _filter_expression_data(expression):
    kind = expression.WhichOneof("expr")
    if kind == "bool_and":
        return {"and": [_filter_expression_data(item) for item in expression.bool_and.operands]}
    if kind == "bool_or":
        return {"or": [_filter_expression_data(item) for item in expression.bool_or.operands]}
    if kind == "bool_not":
        return {"not": _filter_expression_data(expression.bool_not)}
    if kind == "resource":
        return {"resource": expression.resource.resource_id}
    if kind == "field":
        field = {"field_type": expression.field.field_type}
        if expression.field.HasField("field_id"):
            field["field_id"] = expression.field.field_id
        return {"field": field}
    if kind == "facet":
        return {"facet": expression.facet.facet}
    if kind == "keyword":
        return {"keyword": expression.keyword.keyword}
    if kind == "resource_field_prefix":
        prefix = expression.resource_field_prefix
        return {
            "resource_field_prefix": {
                "resource_id": prefix.resource_id,
                "field_type": prefix.field_type,
                "field_id_prefix": prefix.field_id_prefix,
            }
        }
    raise ValueError(f"Unsupported filter expression type: {kind}")


def _paragraph_filter_query(expression) -> str:
    kind = expression.WhichOneof("expr")
    if kind in ("bool_and", "bool_or"):
        operands = [_paragraph_filter_query(item) for item in getattr(expression, kind).operands]
        operation = "andQuery" if kind == "bool_and" else "orQuery"
        return f"cts.{operation}([{', '.join(operands)}])"
    if kind == "bool_not":
        return f"cts.notQuery({_paragraph_filter_query(expression.bool_not)})"
    if kind == "facet":
        return f"cts.jsonPropertyValueQuery('facets', {json.dumps(expression.facet.facet)})"
    if kind == "keyword":
        return (
            f"cts.jsonPropertyWordQuery('text', {json.dumps(expression.keyword.keyword)}, "
            "['case-insensitive', 'unstemmed', 'unwildcarded'])"
        )
    if kind == "resource":
        return (
            f"cts.jsonPropertyValueQuery('resource_id', {json.dumps(expression.resource.resource_id)})"
        )
    if kind == "field":
        predicates = [
            f"cts.jsonPropertyValueQuery('field_type', {json.dumps(expression.field.field_type)})"
        ]
        if expression.field.HasField("field_id"):
            predicates.append(
                f"cts.jsonPropertyValueQuery('field_id', {json.dumps(expression.field.field_id)})"
            )
        return f"cts.andQuery([{', '.join(predicates)}])"
    if kind == "resource_field_prefix":
        prefix = expression.resource_field_prefix
        field_prefix = (
            prefix.field_id_prefix.replace("\\", "\\\\").replace("*", "\\*").replace("?", "\\?")
        )
        return (
            "cts.andQuery(["
            f"cts.jsonPropertyValueQuery('resource_id', {json.dumps(prefix.resource_id)}), "
            f"cts.jsonPropertyValueQuery('field_type', {json.dumps(prefix.field_type)}), "
            f"cts.jsonPropertyValueQuery('field_id', {json.dumps(field_prefix + '*')}, ['wildcarded'])])"
        )
    raise ValueError(f"Unsupported filter expression type: {kind}")


def _filter_context(query: SearchRequest) -> str:
    field_expression = (
        _filter_expression_data(query.field_filter) if query.HasField("field_filter") else None
    )
    paragraph_expression = (
        _filter_expression_data(query.paragraph_filter) if query.HasField("paragraph_filter") else None
    )
    security_groups = list(query.security.access_groups) if query.HasField("security") else None
    setup = [
        f"const fieldExpression = {json.dumps(field_expression)};",
        f"const paragraphExpression = {json.dumps(paragraph_expression)};",
        f"const securityGroups = {json.dumps(security_groups)};",
        f"const filterOperator = {json.dumps(query.filter_operator)};",
        "const resourcesById = new Map();",
        "if (fieldExpression || securityGroups !== null) {",
        f"  for (const doc of cts.search(cts.collectionQuery({json.dumps(MarkLogicCollections.RESOURCES)})).toArray()) {{",
        "    const resource = JSON.parse(xdmp.toJsonString(doc.root));",
        "    const resourceId = xdmp.nodeUri(doc).split('/').pop().replace('.json', '');",
        "    resourcesById.set(resourceId, resource);",
        "  }",
        "}",
        "function securityMatches(target) {",
        "  if (securityGroups === null) return true;",
        "  const resource = resourcesById.get(target.resource_id);",
        "  const accessGroups = resource && resource.security && resource.security.access_groups || [];",
        "  return accessGroups.length === 0 || accessGroups.some(group => securityGroups.includes(group));",
        "}",
        "function matchesExpression(expression, target, resource, isField) {",
        "  if (!expression) return true;",
        "  if (expression.and) return expression.and.every(item => matchesExpression(item, target, resource, isField));",
        "  if (expression.or) return expression.or.some(item => matchesExpression(item, target, resource, isField));",
        "  if (expression.not) return !matchesExpression(expression.not, target, resource, isField);",
        "  if (expression.resource !== undefined) return String(target.resource_id) === expression.resource;",
        "  if (expression.field) return String(target.field_type) === expression.field.field_type && (expression.field.field_id === undefined || String(target.field_id) === expression.field.field_id);",
        "  if (expression.facet !== undefined) return (target.facets || []).includes(expression.facet) || (isField && ((resource && resource.facets) || []).includes(expression.facet));",
        "  if (expression.keyword !== undefined) return cts.contains(xdmp.toJSON({text: target.extracted_text || target.text || ''}), cts.jsonPropertyWordQuery('text', expression.keyword, ['case-insensitive', 'unstemmed', 'unwildcarded']));",
        "  if (expression.resource_field_prefix) return String(target.resource_id) === expression.resource_field_prefix.resource_id && String(target.field_type) === expression.resource_field_prefix.field_type && String(target.field_id).startsWith(expression.resource_field_prefix.field_id_prefix);",
        "  return false;",
        "}",
        "const matchingFields = new Set();",
        "const matchingFieldDirectories = [];",
        "if (fieldExpression) {",
        f"  for (const doc of cts.search(cts.collectionQuery({json.dumps(MarkLogicCollections.FIELDS_PROCESSED)})).toArray()) {{",
        "    const field = JSON.parse(xdmp.toJsonString(doc.root));",
        "    const resource = resourcesById.get(field.resource_id);",
        "    if (matchesExpression(fieldExpression, field, resource, true)) {",
        "      matchingFields.add(field.resource_id + '\\u001f' + field.field_type + '\\u001f' + field.field_id);",
        "      matchingFieldDirectories.push('/resources/' + field.resource_id + '/fields/' + field.field_type + '/' + field.field_id + '/');",
        "    }",
        "  }",
        "}",
        "const fieldMatches = target => !fieldExpression || matchingFields.has(target.resource_id + '\\u001f' + target.field_type + '\\u001f' + target.field_id);",
        "const paragraphMatches = target => !paragraphExpression || matchesExpression(paragraphExpression, target, resourcesById.get(target.resource_id), false);",
        "function passesFilters(target, paragraph) {",
        "  if (!securityMatches(target)) return false;",
        "  const fieldMatch = fieldMatches(target);",
        "  const paragraphMatch = !paragraphExpression || (paragraph && paragraphMatches(paragraph));",
        "  if (fieldExpression && paragraphExpression) return filterOperator === 1 ? fieldMatch || paragraphMatch : fieldMatch && paragraphMatch;",
        "  return fieldExpression ? fieldMatch : paragraphMatch;",
        "}",
    ]
    paragraph_query = (
        _paragraph_filter_query(query.paragraph_filter) if paragraph_expression else "cts.trueQuery()"
    )
    setup.extend(
        [
            "const fieldCandidateQuery = !fieldExpression ? cts.trueQuery() : "
            "matchingFieldDirectories.length ? cts.directoryQuery(matchingFieldDirectories, 'infinity') : cts.falseQuery();",
            f"const paragraphCandidateQuery = {paragraph_query};",
            "const combinedCandidateQuery = fieldExpression && paragraphExpression ? "
            "(filterOperator === 1 ? cts.orQuery([fieldCandidateQuery, paragraphCandidateQuery]) : "
            "cts.andQuery([fieldCandidateQuery, paragraphCandidateQuery])) : "
            "fieldExpression ? fieldCandidateQuery : paragraphCandidateQuery;",
            "const allowedResourceIds = [];",
            "if (securityGroups !== null) resourcesById.forEach((resource, resourceId) => { "
            "if (securityMatches({resource_id: resourceId})) allowedResourceIds.push(resourceId); });",
            "const securityCandidateQuery = securityGroups === null ? cts.trueQuery() : "
            "allowedResourceIds.length ? cts.jsonPropertyValueQuery('resource_id', allowedResourceIds) : cts.falseQuery();",
            "const candidateQuery = cts.andQuery([securityCandidateQuery, combinedCandidateQuery]);",
            "const vectorCandidateQuery = fieldExpression && paragraphExpression && filterOperator === 1 ? "
            "securityCandidateQuery : cts.andQuery([securityCandidateQuery, fieldCandidateQuery]);",
        ]
    )
    if paragraph_expression:
        setup.extend(
            [
                "const chunksByParagraph = new Map();",
                f"for (const doc of cts.search(cts.collectionQuery({json.dumps(MarkLogicCollections.CHUNKS)})).toArray()) {{",
                "  const chunk = JSON.parse(xdmp.toJsonString(doc.root));",
                "  const position = chunk.data && chunk.data.position;",
                "  if (position) chunksByParagraph.set(paragraphKey(chunk, position), chunk);",
                "}",
                "function paragraphKey(target, position) { return target.resource_id + '/' + target.field_type + '/' + target.field_id + '/' + position.start + '-' + position.end; }",
                "function paragraphForEmbedding(embedding) { const position = embedding.data && embedding.data.position; return position ? chunksByParagraph.get(paragraphKey(embedding, position)) : null; }",
            ]
        )
    else:
        setup.append("function paragraphForEmbedding(embedding) { return embedding; }")
    return " ".join(setup)


async def hybrid_search(kbid: str, query: SearchRequest) -> SearchResponse:
    driver = get_driver()
    if not isinstance(driver, MarkLogicDriver):
        return SearchResponse()

    response = SearchResponse()
    has_lexical_query = query.paragraph and bool(query.body)
    has_vector_query = bool(query.vector)
    if has_lexical_query and has_vector_query:
        await _search_hybrid(driver, kbid, query, response)
    elif has_lexical_query:
        await _search_chunks(driver, kbid, query, response)
    elif has_vector_query:
        await _search_vectors(driver, kbid, query, response)
    return response


def _chunk_results_expression(query: SearchRequest) -> str:
    limit = max(0, query.result_per_page)
    terms = keyword_terms(query.body)
    exact_terms = [term.strip('"') for term in terms]
    variants = set()
    for term in terms:
        if not term.isalnum() or len(term) < 4:
            continue
        for index in range(len(term)):
            variants.add(term[:index] + "?" + term[index + 1 :])
            variants.add(term[:index] + term[index + 1 :])
            variants.add(term[:index] + "?" + term[index:])
            if index + 1 < len(term):
                variants.add(term[:index] + term[index + 1] + term[index] + term[index + 2 :])
        variants.add(term + "?")
    return (
        "(() => { "
        f"const exactQuery = cts.jsonPropertyWordQuery('text', {json.dumps(exact_terms)}, ['case-insensitive', 'unstemmed', 'unwildcarded']); "
        f"const limit = {limit}; "
        "function search(textQuery, fuzzy) { const results = []; "
        "if (!limit) return results; "
        "for (const doc of cts.search(cts.andQuery(["
        f"cts.collectionQuery({json.dumps(MarkLogicCollections.CHUNKS)}), "
        "candidateQuery, textQuery"
        "]))) { "
        "const content = JSON.parse(xdmp.toJsonString(doc.root)); "
        "if (!passesFilters(content, content)) continue; "
        "const score = Math.max(0, cts.score(doc)); "
        "results.push({content: {chunk_id: content.chunk_id, facets: content.facets, "
        "data: content.data}, fuzzy, score: (fuzzy ? 0 : 1) + score / (1 + score)}); "
        "if (results.length >= limit) break; "
        "} return results; } "
        "const exactResults = search(exactQuery, false); "
        "if (exactResults.length) return exactResults; "
        f"const fuzzyQuery = cts.jsonPropertyWordQuery('text', {json.dumps(sorted(variants))}, ['case-insensitive', 'unstemmed', 'wildcarded']); "
        "return search(fuzzyQuery, true); "
        "})()"
    )


async def _search_chunks(
    driver: MarkLogicDriver,
    kbid: str,
    query: SearchRequest,
    response: SearchResponse,
) -> None:
    filter_context = _filter_context(query)
    javascript = f"(() => {{ {filter_context} return {_chunk_results_expression(query)}; }})()"
    async with driver.ro_transaction(kbid=kbid) as txn:
        response.paragraph.query = query.body
        matches = await documents.evaluate(txn, javascript)
        _append_chunk_results(query, response, _flatten_eval_array(matches))


def _flatten_eval_array(value):
    while len(value) == 1 and isinstance(value[0], list):
        value = value[0]
    return value


def _append_chunk_results(query: SearchRequest, response: SearchResponse, matches) -> None:
    response.paragraph.query = query.body
    for match in matches:
        if not isinstance(match, dict):
            continue
        content = match.get("content")
        if not isinstance(content, dict):
            continue
        paragraph_id = ParagraphId.from_string(str(content["chunk_id"]))
        result = ParagraphResult(
            uuid=paragraph_id.rid,
            field=paragraph_id.field_id.short_without_subfield(),
            start=paragraph_id.paragraph_start,
            end=paragraph_id.paragraph_end,
            paragraph=content["chunk_id"],
            index=int(content.get("data", {}).get("position", {}).get("index", 0)),
            labels=content.get("facets", []),
        )
        result.score.bm25 = float(match.get("score", 0.0))
        if match.get("fuzzy", False):
            result.matches.append(query.body)
        ParseDict(content.get("data", {}), result.metadata)
        response.paragraph.results.append(result)
    response.paragraph.total = len(response.paragraph.results)


async def _search_vectors(
    driver: MarkLogicDriver, kbid: str, query: SearchRequest, response: SearchResponse
) -> None:
    javascript = _vector_search_expression(query)
    async with driver.ro_transaction(kbid=kbid) as txn:
        results = await documents.evaluate(txn, javascript)
        _append_vector_results(query, response, _flatten_eval_array(results))


def _append_vector_results(query: SearchRequest, response: SearchResponse, embeddings) -> None:
    for result in embeddings:
        if not isinstance(result, dict):
            continue
        embedding = result.get("content")
        if not isinstance(embedding, dict):
            continue
        result = DocumentScored(
            doc_id=DocumentVectorIdentifier(id=embedding["chunk_id"]),
            score=float(result["score"]),
            labels=embedding.get("facets", []),
        )
        ParseDict(embedding.get("data", {}), result.metadata)
        response.vector.documents.append(result)


def _vector_search_expression(query: SearchRequest) -> str:
    filter_context = _filter_context(query)
    vector_results = _vector_results_expression(query)
    return f"(() => {{ {filter_context} return {vector_results}; }})()"


def _vector_results_expression(query: SearchRequest) -> str:
    query_vector = f"vec.vector({json.dumps(list(query.vector))})"
    limit = max(0, query.result_per_page)
    return (
        "cts.search(cts.andQuery([cts.collectionQuery("
        f"{json.dumps(MarkLogicCollections.EMBEDDINGS)}), vectorCandidateQuery, "
        f"cts.jsonPropertyValueQuery('model_id', {json.dumps(query.vectorset)})])).toArray().map(doc => {{"
        "const root = JSON.parse(xdmp.toJsonString(doc.root)); "
        "if (!passesFilters(root, paragraphForEmbedding(root))) return null; "
        f"if (root.model_id != {json.dumps(query.vectorset)}) return null; "
        "const score = 1 - vec.cosineDistance(vec.vector(root.vector), "
        f"{query_vector}); "
        f"if (score < {json.dumps(query.min_score_semantic)}) return null; "
        "return {score, content: {chunk_id: root.chunk_id, facets: root.facets, data: root.data}}; "
        "}).filter(result => result !== null)"
        ".sort((left, right) => right.score - left.score)"
        f".slice(0, {limit})"
    )


async def _search_hybrid(
    driver: MarkLogicDriver,
    kbid: str,
    query: SearchRequest,
    response: SearchResponse,
) -> None:
    filter_context = _filter_context(query)
    vector_results = _vector_results_expression(query)
    javascript = (
        "(() => { "
        f"{filter_context} "
        f"const chunks = {_chunk_results_expression(query)}; "
        f"const embeddings = {vector_results}; "
        "return {chunks, embeddings}; "
        "})()"
    )
    async with driver.ro_transaction(kbid=kbid) as txn:
        eval_result = _flatten_eval_array(await documents.evaluate(txn, javascript))
        if len(eval_result) == 1 and isinstance(eval_result[0], dict):
            result = eval_result[0]
        elif isinstance(eval_result, dict):
            result = eval_result
        else:
            return
        _append_chunk_results(query, response, _flatten_eval_array(result.get("chunks", [])))
        _append_vector_results(query, response, _flatten_eval_array(result.get("embeddings", [])))


async def suggest(kbid: str, query: SuggestRequest) -> SuggestResponse:
    # TODO(Marklogic): Execute the suggestion request against the indexing backend.
    return SuggestResponse()


async def graph_search(kbid: str, query: GraphSearchRequest) -> GraphSearchResponse:
    # TODO(Marklogic): Execute the graph request against the indexing backend.
    return GraphSearchResponse()
