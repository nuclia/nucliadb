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
from collections.abc import Callable
from typing import Any, NamedTuple

from google.protobuf.json_format import ParseDict
from nidx_protos.nodereader_pb2 import (
    ENTITIES,
    DocumentScored,
    DocumentVectorIdentifier,
    FilterExpression,
    FilterOperator,
    GraphQuery,
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
from nucliadb.search.search.graph_backend import append_graph_results, graph_expression
from nucliadb.search.search.keyword_terms import keyword_terms
from nucliadb.search.search.metrics import semantic_search_mode
from nucliadb.search.search.query_templates import load_template, render_template
from nucliadb_protos import utils_pb2

# Below this many candidate embeddings, an exact scan is cheaper and more precise than ANN
EXACT_VECTOR_SEARCH_MAX_CANDIDATES = 1000
ANN_OVERSAMPLING = 4

_WORD_OPTIONS = "['case-insensitive', 'unstemmed', 'unwildcarded']"
# Results come sorted by descending score, which the min score cut relies on
_BM25_OPTIONS = ["score-bm25", "bm25-length-weight=0.5"]
# Same boost as nidx gives to its fuzzy fallback query
FUZZY_SCORE_BOOST = 0.5

_CONSTANTS_JS = render_template(
    "search_constants.js",
    collections=json.dumps(
        {
            "resources": MarkLogicCollections.RESOURCES,
            "fieldsProcessed": MarkLogicCollections.FIELDS_PROCESSED,
            "chunks": MarkLogicCollections.CHUNKS,
            "embeddings": MarkLogicCollections.EMBEDDINGS,
        }
    ),
    paths=json.dumps(
        {
            "facets": MarkLogicIndexPaths.FACETS,
            "resourceFacets": MarkLogicIndexPaths.RESOURCE_FACETS,
            "accessGroups": MarkLogicIndexPaths.ACCESS_GROUPS,
        }
    ),
    vector_schema=json.dumps(vector_views.SCHEMA_NAME),
    collation=json.dumps(STRING_COLLATION),
)

# Field ids never contain '/', so directory URIs select resources and fields exactly via the URI index
_HELPERS_JS = load_template("search_helpers.js")


class _CtsQuery(NamedTuple):
    js: str
    # Universal index resolution may be inexact (e.g. phrases), so documents must be filtered
    filtered: bool


class _QueryParameters:
    """Keep request values outside eval source; templates receive array references instead."""

    def __init__(self) -> None:
        self.values: list[Any] = []

    def literal(self, value: Any) -> str:
        # Builders share this ordered array, so nested templates use the same bindings.
        self.values.append(value)
        return f"searchParams[{len(self.values) - 1}]"

    @property
    def variables(self) -> dict[str, str]:
        return {"searchParameters": json.dumps(self.values)}

    @staticmethod
    def bind(javascript: str) -> str:
        # REST eval binds the declared external variable to a JSON string, parsed once per request.
        return "var searchParameters; const searchParams = JSON.parse(searchParameters); " + javascript


def _value_query(prop: str, value: str, literal: Callable[[Any], str] = json.dumps) -> str:
    return f"cts.jsonPropertyValueQuery({json.dumps(prop)}, {literal(value)}, ['exact'])"


def _word_query(prop: str, value: str, literal: Callable[[Any], str] = json.dumps) -> str:
    return f"cts.jsonPropertyWordQuery({json.dumps(prop)}, {literal(value)}, {_WORD_OPTIONS})"


def _expression_query(
    expression: FilterExpression,
    *,
    field_level: bool,
    literal: Callable[[Any], str] = json.dumps,
) -> _CtsQuery:
    """Translate protobuf predicates into CTS constructors, not query results.

    Field expressions target processed fields; paragraph expressions target chunks.
    The filtered flag tells URI enumeration whether index matches need document
    verification. It propagates through boolean expressions containing text predicates.
    """
    kind = expression.WhichOneof("expr")
    if kind in ("bool_and", "bool_or"):
        operands = [
            _expression_query(item, field_level=field_level, literal=literal)
            for item in getattr(expression, kind).operands
        ]
        operation = "andQuery" if kind == "bool_and" else "orQuery"
        return _CtsQuery(
            f"cts.{operation}([{', '.join(item.js for item in operands)}])",
            any(item.filtered for item in operands),
        )
    if kind == "bool_not":
        inner = _expression_query(expression.bool_not, field_level=field_level, literal=literal)
        return _CtsQuery(f"cts.notQuery({inner.js})", inner.filtered)
    if kind == "resource":
        # A resource directory covers its descendant fields, chunks and embeddings.
        directory = MarkLogicURIs.resource_directory(expression.resource.resource_id)
        return _CtsQuery(f"cts.directoryQuery({literal(directory)}, 'infinity')", False)
    if kind == "field":
        # Type-only predicates select all fields of that type; an optional id narrows them.
        predicates = [_value_query("field_type", expression.field.field_type, literal)]
        if expression.field.HasField("field_id"):
            predicates.append(_value_query("field_id", expression.field.field_id, literal))
        return _CtsQuery(f"cts.andQuery([{', '.join(predicates)}])", False)
    if kind == "resource_field_prefix":
        prefix = expression.resource_field_prefix
        base = f"{MarkLogicURIs.fields_directory(prefix.resource_id)}{prefix.field_type}/"
        return _CtsQuery(f"fieldPrefixQuery({literal(base)}, {literal(prefix.field_id_prefix)})", False)
    if kind == "facet":
        # Field predicates also consider copied resource facets; chunks use their own facets.
        facet = literal(expression.facet.facet)
        return _CtsQuery(f"{'fieldFacetQuery' if field_level else 'facetQuery'}({facet})", False)
    if kind == "keyword":
        # Processed fields contain full extracted text; chunks contain paragraph text.
        prop = "extracted_text" if field_level else "text"
        return _CtsQuery(_word_query(prop, expression.keyword.keyword, literal), True)
    raise ValueError(f"Unsupported filter expression type: {kind}")


def _prefilter_javascript(
    field_filter: FilterExpression | None,
    security_groups: list[str] | None,
    *,
    paragraph_or: bool = False,
    literal: Callable[[Any], str] = json.dumps,
) -> str:
    """Defines `fieldDirs`: null when every allowed field matches, otherwise the directories of the
    matching fields. Also `securityQuery`, the security restriction not already in `fieldDirs`."""
    groups = literal(security_groups)
    if field_filter is None:
        field_dirs = "null"
    else:
        query = _expression_query(field_filter, field_level=True, literal=literal)
        field_dirs = render_template(
            "search_field_dirs.js", query=query.js, filtered=json.dumps(query.filtered)
        )
    # Matching field directories already exclude the denied fields, unless they are not applied
    return render_template(
        "search_prefilter.js",
        groups=groups,
        field_dirs=field_dirs,
        paragraph_or=json.dumps(paragraph_or),
    )


def _graph_resource_scope(
    field_filter: FilterExpression | None,
    groups: list[str] | None,
    literal: Callable[[Any], str] = json.dumps,
) -> str:
    # Resource-owned relations live on resource documents, outside field directories.
    # Project resource/facet predicates separately; field-only leaves cannot match them.
    def resource_expression(expression: FilterExpression) -> str:
        kind = expression.WhichOneof("expr")
        if kind in ("bool_and", "bool_or"):
            operation = "andQuery" if kind == "bool_and" else "orQuery"
            operands = [resource_expression(item) for item in getattr(expression, kind).operands]
            return f"cts.{operation}([{', '.join(operands)}])"
        if kind == "bool_not":
            return f"cts.notQuery({resource_expression(expression.bool_not)})"
        if kind == "resource":
            return (
                f"cts.documentQuery({literal(MarkLogicURIs.resource(expression.resource.resource_id))})"
            )
        if kind == "facet":
            return f"facetQuery({literal(expression.facet.facet)})"
        return "cts.falseQuery()"

    restriction = resource_expression(field_filter) if field_filter is not None else "cts.trueQuery()"
    security = (
        f"cts.notQuery(deniedQuery({literal(groups)}))" if groups is not None else "cts.trueQuery()"
    )
    return render_template("search_graph_scope.js", security=security, restriction=restriction)


def _search_javascript(
    query: SearchRequest,
    *,
    lexical: bool,
    semantic: bool,
    parameters: _QueryParameters | None = None,
) -> str:
    # Production emits parameter references; the default keeps direct builder calls self-contained.
    literal = parameters.literal if parameters is not None else json.dumps
    field_filter = query.field_filter if query.HasField("field_filter") else None
    paragraph_filter = query.paragraph_filter if query.HasField("paragraph_filter") else None
    security_groups = list(query.security.access_groups) if query.HasField("security") else None
    # Hybrid security filtering is deliberately disabled pending its permission-based replacement.
    # The templates still support security constraints for callers that supply groups directly.
    security_groups = None
    paragraph_or = (
        field_filter is not None
        and paragraph_filter is not None
        and query.filter_operator == FilterOperator.OR
    )
    # The OR flag applies only when both sides exist; a lone predicate is not a union.
    paragraph_query = (
        _expression_query(paragraph_filter, field_level=False, literal=literal)
        if paragraph_filter is not None
        else None
    )
    # Build branch expressions first; they execute later inside the shared JavaScript scope.
    chunks = _chunk_results_expression(query, literal=literal) if lexical else "[]"
    embeddings = (
        _vector_results_expression(
            query, paragraph_query is not None and paragraph_query.filtered, literal=literal
        )
        if semantic
        else "[]"
    )
    # Graph retrieval is optional and shares field/resource scoping, not paragraph scoring.
    graph_query = GraphSearchRequest(top_k=max(0, query.result_per_page))
    if query.HasField("graph_search"):
        graph_query.query.CopyFrom(query.graph_search.query)
        graph_query.graph_node_vectorset = query.graph_node_vectorset
        graph_query.graph_edge_vectorset = query.graph_edge_vectorset
        graph_query.min_score_node_semantic = query.min_score_node_semantic
        graph_query.min_score_edge_semantic = query.min_score_edge_semantic
    return render_template(
        "search_hybrid.js",
        constants=_CONSTANTS_JS,
        helpers=_HELPERS_JS,
        prefilter=_prefilter_javascript(
            field_filter, security_groups, paragraph_or=paragraph_or, literal=literal
        ),
        graph_resource_scope=_graph_resource_scope(field_filter, security_groups, literal),
        paragraph_or=json.dumps(paragraph_or),
        no_graph=json.dumps(not query.HasField("graph_search")),
        paragraph_query=paragraph_query.js if paragraph_query else "null",
        chunks=chunks,
        embeddings=embeddings,
        graph=graph_expression(graph_query, literal=literal),
    )


async def hybrid_search(kbid: str, query: SearchRequest) -> SearchResponse:
    """Run lexical, vector and optional graph retrieval in one KB-scoped MarkLogic eval.

    Results remain separate in SearchResponse. Rank fusion and reranking belong to
    the retrieval layer consuming this response, not this backend.
    """
    driver = get_driver()
    if not isinstance(driver, MarkLogicDriver):
        return SearchResponse()

    response = SearchResponse()
    # An empty keyword query does not request a match-all lexical scan.
    lexical = query.paragraph and bool(query.body)
    semantic = bool(query.vector)
    if not lexical and not semantic and not query.HasField("graph_search"):
        return response

    # 1. Assemble source from templates and bind all request-dependent values separately.
    parameters = _QueryParameters()
    javascript = parameters.bind(
        _search_javascript(query, lexical=lexical, semantic=semantic, parameters=parameters)
    )
    # 2. Run prefilters and all enabled branches server-side against the KB database.
    async with driver.ro_transaction(kbid=kbid) as txn:
        result = _eval_object(await documents.evaluate(txn, javascript, variables=parameters.variables))
    # 3. Translate each result family into its protobuf section without combining scores.
    if lexical:
        _append_chunk_results(query, response, _flatten_eval_array(result.get("chunks", [])))
    if semantic:
        _append_vector_results(response, _flatten_eval_array(result.get("embeddings", [])))
        if result.get("semantic_mode"):
            semantic_search_mode.inc({"mode": result["semantic_mode"]})
    append_graph_results(response.graph, _flatten_eval_array(result.get("graph", [])))
    return response


def _eval_object(value: Any) -> dict:
    # REST eval can return an object directly or wrap it in one or more result arrays.
    if isinstance(value, dict):
        return value
    value = _flatten_eval_array(value)
    if len(value) == 1 and isinstance(value[0], dict):
        return value[0]
    return {}


def _chunk_results_expression(
    query: SearchRequest, *, literal: Callable[[Any], str] = json.dumps
) -> str:
    limit = max(0, query.result_per_page)
    terms = keyword_terms(query.body)
    exact_terms = [term.strip('"') for term in terms]
    # Prepare one-edit wildcard patterns only for sufficiently long standalone words.
    # Quoted phrases are excluded from this fuzzy fallback by the alphanumeric check.
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
    return render_template(
        "search_lexical.js",
        limit=literal(limit),
        min_score=literal(query.min_score_bm25),
        bm25_options=json.dumps(_BM25_OPTIONS),
        exact_terms=literal(exact_terms),
        word_options=_WORD_OPTIONS,
        patterns=literal(sorted(variants)),
        lexicon_options=json.dumps(["case-insensitive", f"collation={STRING_COLLATION}"]),
        fuzzy_boost=FUZZY_SCORE_BOOST,
    )


def _flatten_eval_array(value):
    while len(value) == 1 and isinstance(value[0], list):
        value = value[0]
    return value


def _append_chunk_results(query: SearchRequest, response: SearchResponse, matches) -> None:
    # Chunk IDs encode resource, field and character offsets; metadata adds paragraph positions.
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
    # This is the returned, limited result count, not a separate count of every matching chunk.
    response.paragraph.total = len(response.paragraph.results)


def _append_vector_results(response: SearchResponse, embeddings) -> None:
    # Preserve the embedding's vector identifier and cosine score for downstream rank fusion.
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


def _vector_results_expression(
    query: SearchRequest,
    paragraph_filtered: bool,
    *,
    literal: Callable[[Any], str] = json.dumps,
) -> str:
    # Paragraph predicates are evaluated on chunks, then joined to embeddings by
    # field/split/paragraph offsets in search_vector.js. They cannot be applied
    # directly to embedding documents, which do not store paragraph facets.
    limit = max(0, query.result_per_page)
    return render_template(
        "search_vector.js",
        filter_limit=literal(limit),
        paragraph_filtered=json.dumps(paragraph_filtered),
        model=literal(query.vectorset),
        limit=literal(limit),
        vector=literal(list(query.vector)),
        min_score=literal(query.min_score_semantic),
        view=literal(vector_views.view_name(query.vectorset, len(query.vector))),
        exact_max=EXACT_VECTOR_SEARCH_MAX_CANDIDATES,
        oversampling=ANN_OVERSAMPLING,
    )


async def suggest(kbid: str, query: SuggestRequest) -> SuggestResponse:
    response = SuggestResponse(query=query.body)
    if ENTITIES in query.features and query.body.strip():
        graph_query = GraphSearchRequest(kind=GraphSearchRequest.NODES, top_k=query.top_k or 20)
        for name in ("security", "field_filter"):
            if query.HasField(name):
                getattr(graph_query, name).CopyFrom(getattr(query, name))
        terms = query.body.split()
        for start in range(max(0, len(terms) - 3), len(terms)):
            path = graph_query.query.path.bool_or.operands.add().path
            path.source.value = " ".join(terms[start:])
            path.source.node_type = utils_pb2.RelationNode.ENTITY
            path.source.fuzzy.kind = GraphQuery.Node.PREFIX
            path.source.fuzzy.distance = 1
        result = await graph_search(kbid, graph_query)
        response.entity_results.nodes.extend(result.nodes)
    return response


async def graph_search(kbid: str, query: GraphSearchRequest) -> GraphSearchResponse:
    driver = get_driver()
    response = GraphSearchResponse()
    if not isinstance(driver, MarkLogicDriver):
        return response
    field_filter = query.field_filter if query.HasField("field_filter") else None
    groups = list(query.security.access_groups) if query.HasField("security") else None
    parameters = _QueryParameters()
    javascript = parameters.bind(
        render_template(
            "search_graph.js",
            constants=_CONSTANTS_JS,
            helpers=_HELPERS_JS,
            prefilter=_prefilter_javascript(field_filter, groups, literal=parameters.literal),
            graph_resource_scope=_graph_resource_scope(field_filter, groups, parameters.literal),
            graph=graph_expression(query, literal=parameters.literal),
        )
    )
    async with driver.ro_transaction(kbid=kbid) as txn:
        matches = _flatten_eval_array(
            await documents.evaluate(txn, javascript, variables=parameters.variables)
        )
    if query.kind == GraphSearchRequest.RELATIONS:
        for match in matches:
            match.pop("source_value", None)
    append_graph_results(response, matches)
    return response
