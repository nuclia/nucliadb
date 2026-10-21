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
from typing import Any, NamedTuple

from google.protobuf.json_format import ParseDict
from nidx_protos.nodereader_pb2 import (
    DocumentScored,
    DocumentVectorIdentifier,
    FilterExpression,
    FilterOperator,
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
from nucliadb.common.maindb.index_paths import MarkLogicIndexPaths
from nucliadb.common.maindb.marklogic import MarkLogicDriver
from nucliadb.common.maindb.marklogic_schema import STRING_COLLATION
from nucliadb.common.maindb.utils import get_driver
from nucliadb.common.marklogic import documents, vector_views
from nucliadb.common.marklogic.uris import MarkLogicURIs
from nucliadb.search.search.keyword_terms import keyword_terms
from nucliadb.search.search.metrics import semantic_search_mode

# Below this many candidate embeddings, an exact scan is cheaper and more precise than ANN
EXACT_VECTOR_SEARCH_MAX_CANDIDATES = 1000
ANN_OVERSAMPLING = 4

_WORD_OPTIONS = "['case-insensitive', 'unstemmed', 'unwildcarded']"
# Results come sorted by descending score, which the min score cut relies on
_BM25_OPTIONS = ["score-bm25", "bm25-length-weight=0.5"]
# Same boost as nidx gives to its fuzzy fallback query
FUZZY_SCORE_BOOST = 0.5

_CONSTANTS_JS = (
    "const COLLECTIONS = "
    + json.dumps(
        {
            "resources": MarkLogicCollections.RESOURCES,
            "fieldsProcessed": MarkLogicCollections.FIELDS_PROCESSED,
            "chunks": MarkLogicCollections.CHUNKS,
            "embeddings": MarkLogicCollections.EMBEDDINGS,
        }
    )
    + "; const PATHS = "
    + json.dumps(
        {
            "facets": MarkLogicIndexPaths.FACETS,
            "resourceFacets": MarkLogicIndexPaths.RESOURCE_FACETS,
            "accessGroups": MarkLogicIndexPaths.ACCESS_GROUPS,
        }
    )
    + "; const VECTOR_SCHEMA = "
    + json.dumps(vector_views.SCHEMA_NAME)
    + "; const COLLATION = "
    + json.dumps(STRING_COLLATION)
    + ";"
)

# Field ids never contain '/', so directory URIs select resources and fields exactly via the URI index
_HELPERS_JS = r"""
let semanticMode = null;
function dirsQuery(dirs) { return dirs.length ? cts.directoryQuery(dirs, 'infinity') : cts.falseQuery(); }
function matchingUris(query, filtered) {
  if (!filtered) return cts.uris('', [], query).toArray().map(String);
  const uris = [];
  for (const doc of cts.search(query, ['filtered', 'score-zero'])) uris.push(String(xdmp.nodeUri(doc)));
  return uris;
}
// Facets are hierarchical: a labelset or entity type also matches all the facets below it
function facetValues(path, facet) {
  const values = [facet];
  const prefix = facet + '/';
  const reference = cts.pathReference(path, ['type=string', 'collation=' + COLLATION]);
  for (const value of cts.values(reference, prefix)) {
    const text = String(value);
    if (!text.startsWith(prefix)) break;
    values.push(text);
  }
  return values;
}
function facetQuery(facet) { return cts.pathRangeQuery(PATHS.facets, '=', facetValues(PATHS.facets, facet)); }
// Processed fields hold a copy of their resource facets
function fieldFacetQuery(facet) {
  const resourceFacets = facetValues(PATHS.resourceFacets, facet);
  return cts.orQuery([facetQuery(facet), cts.pathRangeQuery(PATHS.resourceFacets, '=', resourceFacets)]);
}
function fieldPrefixQuery(base, prefix) {
  const start = base + prefix;
  const uris = cts.uriMatch(start + '*/processed.json').toArray().map(String).filter(uri => uri.startsWith(start));
  return dirsQuery(uris.map(uri => uri.replace(/processed\.json$/, '')));
}
// Documents restricted to access groups, none of them in `groups`
function deniedQuery(groups) {
  const restricted = cts.pathRangeQuery(PATHS.accessGroups, '>', '');
  if (!groups.length) return restricted;
  return cts.andQuery([restricted, cts.notQuery(cts.pathRangeQuery(PATHS.accessGroups, '=', groups))]);
}
function deniedResources(groups) {
  const query = cts.andQuery([cts.collectionQuery(COLLECTIONS.resources), deniedQuery(groups)]);
  return cts.uris('', [], query).toArray().map(uri => String(uri).replace(/\.json$/, '/'));
}
function fieldDir(root) { return '/resources/' + root.resource_id + '/fields/' + root.field_type + '/' + root.field_id + '/'; }
function chunkKey(uri) {
  const match = /^(\/resources\/[^\/]+\/fields\/[^\/]+\/[^\/]+\/)chunks\/(.+)\/data\.json$/.exec(uri);
  return match ? match[1] + match[2] : null;
}
function embeddingKey(root) {
  // Vector ids are rid/field_type/field_id/[split/]index/start-end
  const parts = String(root.chunk_id).split('/');
  const split = parts.length === 6 ? parts[3] + '/' : '';
  const position = root.data && root.data.position;
  return position ? fieldDir(root) + split + position.start + '-' + position.end : null;
}
function semanticSearch(search) {
  semanticMode = 'exact';
  if (!search.limit) return [];
  const similarity = root => 1 - vec.cosineDistance(vec.vector(root.vector), search.queryVector);
  const keep = (scored, root) => {
    if (search.accept !== null && !search.accept(root)) return;
    const score = similarity(root);
    if (score >= search.minScore) scored.push({score, root});
  };
  function exact() {
    const scored = [];
    for (const doc of cts.search(search.scope, ['unfiltered', 'score-zero'])) {
      const root = doc.root.toObject();
      if (root.model_id === search.model) keep(scored, root);
    }
    return scored;
  }
  // Optic filters only apply after the ANN candidates are chosen, so they are oversampled
  function ann(k) {
    const op = require('/MarkLogic/optic');
    const rows = op.fromView(VECTOR_SCHEMA, search.view, '', op.fragmentIdCol('fid'))
      .annTopK(k, op.col('vector'), search.queryVector, op.col('distance'), {maxDistance: 1 - search.minScore})
      .joinDocUri(op.col('uri'), op.fragmentIdCol('fid'))
      .select(['uri']).result().toArray();
    const uris = rows.map(row => String(row.uri));
    const allowed = uris.length
      ? new Set(cts.uris('', [], cts.andQuery([search.scope, cts.documentQuery(uris)])).toArray().map(String))
      : new Set();
    const scored = [];
    for (const uri of uris) if (allowed.has(uri)) keep(scored, cts.doc(uri).toObject());
    return {scored, saturated: rows.length >= k};
  }
  let scored = null;
  if (cts.estimate(search.scope) > search.exactMax) {
    try {
      const base = Math.max(search.limit * search.oversampling, 100);
      for (const k of [base, base * 8]) {
        const result = ann(k);
        if (result.scored.length >= search.limit || !result.saturated) { scored = result.scored; semanticMode = 'ann'; break; }
      }
    } catch (error) {
      // Views of dimensions never written, or still being reindexed
      if (!/SQL-TABLE(NOTFOUND|REINDEXING)/.test(String(error.name) + String(error.message))) throw error;
    }
  }
  if (scored === null) scored = exact();
  scored.sort((left, right) => right.score - left.score);
  return scored.slice(0, search.limit).map(({score, root}) =>
    ({score, content: {chunk_id: root.chunk_id, facets: root.facets, data: root.data}}));
}
"""


class _CtsQuery(NamedTuple):
    js: str
    # Universal index resolution may be inexact (e.g. phrases), so documents must be filtered
    filtered: bool


def _value_query(prop: str, value: str) -> str:
    return f"cts.jsonPropertyValueQuery({json.dumps(prop)}, {json.dumps(value)}, ['exact'])"


def _word_query(prop: str, value: str) -> str:
    return f"cts.jsonPropertyWordQuery({json.dumps(prop)}, {json.dumps(value)}, {_WORD_OPTIONS})"


def _expression_query(expression: FilterExpression, *, field_level: bool) -> _CtsQuery:
    """Field level expressions run on processed fields, paragraph level ones on chunks."""
    kind = expression.WhichOneof("expr")
    if kind in ("bool_and", "bool_or"):
        operands = [
            _expression_query(item, field_level=field_level)
            for item in getattr(expression, kind).operands
        ]
        operation = "andQuery" if kind == "bool_and" else "orQuery"
        return _CtsQuery(
            f"cts.{operation}([{', '.join(item.js for item in operands)}])",
            any(item.filtered for item in operands),
        )
    if kind == "bool_not":
        inner = _expression_query(expression.bool_not, field_level=field_level)
        return _CtsQuery(f"cts.notQuery({inner.js})", inner.filtered)
    if kind == "resource":
        directory = MarkLogicURIs.resource_directory(expression.resource.resource_id)
        return _CtsQuery(f"cts.directoryQuery({json.dumps(directory)}, 'infinity')", False)
    if kind == "field":
        predicates = [_value_query("field_type", expression.field.field_type)]
        if expression.field.HasField("field_id"):
            predicates.append(_value_query("field_id", expression.field.field_id))
        return _CtsQuery(f"cts.andQuery([{', '.join(predicates)}])", False)
    if kind == "resource_field_prefix":
        prefix = expression.resource_field_prefix
        base = f"{MarkLogicURIs.fields_directory(prefix.resource_id)}{prefix.field_type}/"
        return _CtsQuery(
            f"fieldPrefixQuery({json.dumps(base)}, {json.dumps(prefix.field_id_prefix)})", False
        )
    if kind == "facet":
        facet = json.dumps(expression.facet.facet)
        return _CtsQuery(f"{'fieldFacetQuery' if field_level else 'facetQuery'}({facet})", False)
    if kind == "keyword":
        prop = "extracted_text" if field_level else "text"
        return _CtsQuery(_word_query(prop, expression.keyword.keyword), True)
    raise ValueError(f"Unsupported filter expression type: {kind}")


def _prefilter_javascript(
    field_filter: FilterExpression | None,
    security_groups: list[str] | None,
    *,
    paragraph_or: bool = False,
) -> str:
    """Defines `fieldDirs`: null when every allowed field matches, otherwise the directories of the
    matching fields. Also `securityQuery`, the security restriction not already in `fieldDirs`."""
    lines = [
        f"const groups = {json.dumps(security_groups)};",
        "const fieldSecurityQuery = groups === null ? cts.trueQuery() : cts.notQuery(deniedQuery(groups));",
    ]
    if field_filter is None:
        lines.append("const fieldDirs = null;")
    else:
        query = _expression_query(field_filter, field_level=True)
        lines.append(
            "const fieldDirs = (() => { "
            "const scope = cts.andQuery([cts.collectionQuery(COLLECTIONS.fieldsProcessed), fieldSecurityQuery]); "
            f"const uris = matchingUris(cts.andQuery([scope, {query.js}]), {json.dumps(query.filtered)}); "
            "if (uris.length && uris.length >= cts.estimate(scope)) return null; "
            "return uris.map(uri => uri.replace(/processed\\.json$/, '')); "
            "})();"
        )
    # Matching field directories already exclude the denied fields, unless they are not applied
    lines.append(
        f"const denied = groups !== null && (fieldDirs === null || {json.dumps(paragraph_or)}) "
        "? deniedResources(groups) : [];"
    )
    lines.append(
        "const securityQuery = denied.length ? "
        "cts.notQuery(cts.directoryQuery(denied, 'infinity')) : cts.trueQuery();"
    )
    return " ".join(lines)


def _search_javascript(query: SearchRequest, *, lexical: bool, semantic: bool) -> str:
    field_filter = query.field_filter if query.HasField("field_filter") else None
    paragraph_filter = query.paragraph_filter if query.HasField("paragraph_filter") else None
    security_groups = list(query.security.access_groups) if query.HasField("security") else None
    paragraph_or = (
        field_filter is not None
        and paragraph_filter is not None
        and query.filter_operator == FilterOperator.OR
    )
    paragraph_query = (
        _expression_query(paragraph_filter, field_level=False) if paragraph_filter is not None else None
    )
    chunks = _chunk_results_expression(query) if lexical else "[]"
    embeddings = (
        _vector_results_expression(query, paragraph_query is not None and paragraph_query.filtered)
        if semantic
        else "[]"
    )
    return " ".join(
        [
            "(() => {",
            _CONSTANTS_JS,
            _HELPERS_JS,
            _prefilter_javascript(field_filter, security_groups, paragraph_or=paragraph_or),
            f"const paragraphOr = {json.dumps(paragraph_or)};",
            "if (fieldDirs !== null && !fieldDirs.length && !paragraphOr) return {chunks: [], embeddings: []};",
            f"const paragraphQuery = {paragraph_query.js if paragraph_query else 'null'};",
            "const fieldQuery = fieldDirs === null ? null : dirsQuery(fieldDirs);",
            "const filterQuery = paragraphQuery === null ? fieldQuery : "
            "fieldQuery === null ? (paragraphOr ? null : paragraphQuery) : "
            "paragraphOr ? cts.orQuery([fieldQuery, paragraphQuery]) : cts.andQuery([fieldQuery, paragraphQuery]);",
            "const chunkScope = filterQuery === null ? securityQuery : cts.andQuery([securityQuery, filterQuery]);",
            "const embeddingScope = !paragraphOr && fieldQuery !== null ? "
            "cts.andQuery([securityQuery, fieldQuery]) : securityQuery;",
            f"return {{chunks: {chunks}, embeddings: {embeddings}, semantic_mode: semanticMode}};",
            "})()",
        ]
    )


async def hybrid_search(kbid: str, query: SearchRequest) -> SearchResponse:
    driver = get_driver()
    if not isinstance(driver, MarkLogicDriver):
        return SearchResponse()

    response = SearchResponse()
    lexical = query.paragraph and bool(query.body)
    semantic = bool(query.vector)
    if not lexical and not semantic:
        return response

    javascript = _search_javascript(query, lexical=lexical, semantic=semantic)
    async with driver.ro_transaction(kbid=kbid) as txn:
        result = _eval_object(await documents.evaluate(txn, javascript))
    if lexical:
        _append_chunk_results(query, response, _flatten_eval_array(result.get("chunks", [])))
    if semantic:
        _append_vector_results(response, _flatten_eval_array(result.get("embeddings", [])))
        if result.get("semantic_mode"):
            semantic_search_mode.inc({"mode": result["semantic_mode"]})
    return response


def _eval_object(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    value = _flatten_eval_array(value)
    if len(value) == 1 and isinstance(value[0], dict):
        return value[0]
    return {}


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
        f"const limit = {limit}; "
        f"const minScore = {json.dumps(query.min_score_bm25)}; "
        "if (!limit) return []; "
        "function run(textQuery, fuzzy, boost) { "
        "const results = []; "
        "const search = cts.search(cts.andQuery([cts.collectionQuery(COLLECTIONS.chunks), chunkScope, textQuery]), "
        f"{json.dumps(_BM25_OPTIONS)}); "
        "for (const doc of search) { "
        "const score = cts.score(doc) * boost; "
        "if (score < minScore) break; "
        "const content = doc.root.toObject(); "
        "results.push({content: {chunk_id: content.chunk_id, facets: content.facets, data: content.data}, fuzzy, score}); "
        "if (results.length >= limit) break; "
        "} "
        "return results; "
        "} "
        f"const exact = run(cts.jsonPropertyWordQuery('text', {json.dumps(exact_terms)}, {_WORD_OPTIONS}), false, 1); "
        # Like nidx, fuzzy matching is only a fallback for unfiltered scores without keyword matches
        f"const patterns = {json.dumps(sorted(variants))}; "
        "if (exact.length || minScore !== 0 || !patterns.length) return exact; "
        "let fuzzyQuery; "
        "try { "
        "const words = patterns.flatMap(pattern => cts.jsonPropertyWordMatch('text', pattern, "
        f"{json.dumps(['case-insensitive', f'collation={STRING_COLLATION}'])}).toArray().map(String)); "
        f"fuzzyQuery = words.length ? cts.jsonPropertyWordQuery('text', words, {_WORD_OPTIONS}) : null; "
        "} catch (error) { "
        # Without the word lexicon wildcards still match, but BM25 gives them no score
        "if (!/LXCNNOTFOUND/.test(String(error.name) + String(error.message))) throw error; "
        "fuzzyQuery = cts.jsonPropertyWordQuery('text', patterns, ['case-insensitive', 'unstemmed', 'wildcarded']); "
        "} "
        f"return fuzzyQuery === null ? [] : run(fuzzyQuery, true, {FUZZY_SCORE_BOOST}); "
        "})()"
    )


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


def _append_vector_results(response: SearchResponse, embeddings) -> None:
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


def _vector_results_expression(query: SearchRequest, paragraph_filtered: bool) -> str:
    limit = max(0, query.result_per_page)
    return (
        "(() => { "
        # Embeddings have no paragraph facets: paragraph filters go through the matching chunk keys
        "let accept = null; "
        f"if ({limit} && paragraphQuery !== null && !(paragraphOr && fieldQuery === null)) {{ "
        "const chunkQuery = cts.andQuery([cts.collectionQuery(COLLECTIONS.chunks), securityQuery, "
        "...(!paragraphOr && fieldQuery !== null ? [fieldQuery] : []), paragraphQuery]); "
        f"const keys = new Set(matchingUris(chunkQuery, {json.dumps(paragraph_filtered)}).map(chunkKey)); "
        "const fields = paragraphOr ? new Set(fieldDirs) : null; "
        "accept = root => (fields !== null && fields.has(fieldDir(root))) || keys.has(embeddingKey(root)); "
        "} "
        f"const model = {json.dumps(query.vectorset)}; "
        "const scope = cts.andQuery([cts.collectionQuery(COLLECTIONS.embeddings), embeddingScope, "
        "cts.jsonPropertyValueQuery('model_id', model, ['exact'])]); "
        "return semanticSearch({"
        f"limit: {limit}, model, scope, accept, "
        f"queryVector: vec.vector({json.dumps(list(query.vector))}), "
        f"minScore: {json.dumps(query.min_score_semantic)}, "
        f"view: {json.dumps(vector_views.view_name(query.vectorset, len(query.vector)))}, "
        f"exactMax: {EXACT_VECTOR_SEARCH_MAX_CANDIDATES}, "
        f"oversampling: {ANN_OVERSAMPLING}"
        "}); "
        "})()"
    )


async def suggest(kbid: str, query: SuggestRequest) -> SuggestResponse:
    # TODO(Marklogic): Execute the suggestion request against the indexing backend, scoped by
    # _prefilter_javascript(query.field_filter, query.security.access_groups) like hybrid_search.
    return SuggestResponse()


async def graph_search(kbid: str, query: GraphSearchRequest) -> GraphSearchResponse:
    # TODO(Marklogic): Execute the graph request against the indexing backend, scoped by
    # _prefilter_javascript(query.field_filter, query.security.access_groups) like hybrid_search.
    return GraphSearchResponse()
