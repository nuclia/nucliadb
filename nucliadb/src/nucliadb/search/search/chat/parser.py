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

import logging

from pydantic import ValidationError

from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.common.filter_expression import filter_from_facet
from nucliadb.common.models_utils.from_proto import RelationNodeTypeMap
from nucliadb.search.search.chat.fetcher import RAOFetcher
from nucliadb.search.search.query_parser.exceptions import InternalParserError
from nucliadb.search.search.query_parser.models import (
    RelationQuery,
)
from nucliadb.search.search.query_parser.old_filters import is_paragraph_label, translate_label
from nucliadb.search.search.query_parser.parsers.common import (
    parse_keyword_min_score,
    should_disable_vector_search,
)
from nucliadb.search.search.rerankers import NoopReranker, PredictReranker, Reranker
from nucliadb_models import retrieval as retrieval_models
from nucliadb_models import search as search_models
from nucliadb_models.common import FieldTypeName
from nucliadb_models.filters import (
    And,
    DateCreated,
    DateModified,
    Field,
    FieldFilterExpression,
    FilterExpression,
    Keyword,
    Not,
    Or,
    ParagraphFilterExpression,
    Resource,
)
from nucliadb_models.retrieval import RetrievalRequest
from nucliadb_models.search import Filter, FindRequest
from nucliadb_protos import knowledgebox_pb2, utils_pb2

logger = logging.getLogger(__name__)

DEFAULT_GENERIC_SEMANTIC_THRESHOLD = 0.7


async def rao_parse_find(
    kbid: str, find_request: FindRequest
) -> tuple[RAOFetcher, RetrievalRequest, Reranker]:
    # This is a thin layer to convert a FindRequest into a RetrievalRequest +
    # some bw/c stuff we need while refactoring and decoupling code

    fetcher = RAOFetcher(
        kbid,
        query=find_request.query,
        user_vector=find_request.vector,
        vectorset=find_request.vectorset,
        rephrase=find_request.rephrase,
        rephrase_prompt=find_request.rephrase_prompt,
        generative_model=find_request.generative_model,
        query_image=find_request.query_image,
    )
    parser = RAOFindParser(kbid, find_request, fetcher)
    retrieval_request, reranker = await parser.parse()
    return fetcher, retrieval_request, reranker


class RAOFindParser:
    def __init__(self, kbid: str, item: FindRequest, fetcher: RAOFetcher):
        self.kbid = kbid
        self.item = item
        self.fetcher = fetcher

        # cached data while parsing
        self._query: retrieval_models.Query | None = None

    async def parse(self) -> tuple[RetrievalRequest, Reranker]:
        self._validate_request()

        top_k = self.item.top_k

        # parse search types (features)

        self._query = retrieval_models.Query()

        if search_models.FindOptions.KEYWORD in self.item.features:
            self._query.keyword = await parse_keyword_query(self.item, fetcher=self.fetcher)  # type: ignore

        if search_models.FindOptions.SEMANTIC in self.item.features:
            self._query.semantic = await parse_semantic_query(self.item, fetcher=self.fetcher)  # type: ignore

        if search_models.FindOptions.RELATIONS in self.item.features:
            # skip, we'll do something about this later on
            pass

        if search_models.FindOptions.GRAPH in self.item.features:
            self._query.graph = await self._parse_graph_query()

        filters = await self._parse_filters()

        # rank fusion is just forwarded to /retrieve
        rank_fusion = self.item.rank_fusion

        try:
            reranker = self._parse_reranker()
        except ValidationError as exc:
            raise InternalParserError(f"Parsing error in reranker: {exc!s}") from exc

        # As we'll call /retrieve, that has rank fusion integrated, we have to
        # make sure we ask for enough results to rerank.
        if isinstance(reranker, PredictReranker):
            top_k = max(top_k, reranker.window)

        retrieval = RetrievalRequest(
            query=self._query,
            top_k=top_k,
            filters=filters,
            rank_fusion=rank_fusion,
        )
        return retrieval, reranker

    def _validate_request(self):
        # synonyms are not compatible with vector/graph search
        if (
            self.item.with_synonyms
            and self.item.query
            and (
                search_models.FindOptions.SEMANTIC in self.item.features
                or search_models.FindOptions.RELATIONS in self.item.features
                or search_models.FindOptions.GRAPH in self.item.features
            )
        ):
            raise InvalidQueryError(
                "synonyms",
                "Search with custom synonyms is only supported on paragraph and document search",
            )

        if search_models.FindOptions.SEMANTIC in self.item.features:
            if should_disable_vector_search(self.item):
                self.item.features.remove(search_models.FindOptions.SEMANTIC)

        if self.item.graph_query and search_models.FindOptions.GRAPH not in self.item.features:
            raise InvalidQueryError("graph_query", "Using a graph query requires enabling graph feature")

    async def _parse_relation_query(self) -> RelationQuery:
        detected_entities = await self._get_detected_entities()

        return RelationQuery(
            entry_points=detected_entities, deleted_entity_groups=[], deleted_entities={}
        )

    async def _parse_graph_query(self) -> retrieval_models.GraphQuery:
        if self.item.graph_query is None:
            raise InvalidQueryError(
                "graph_query", "Graph query must be provided when using graph search"
            )
        return retrieval_models.GraphQuery(query=self.item.graph_query)

    async def _get_detected_entities(self) -> list[utils_pb2.RelationNode]:
        """Get entities from request, either automatically detected or
        explicitly set by the user."""

        if self.item.query_entities:
            detected_entities = []
            for entity in self.item.query_entities:
                relation_node = utils_pb2.RelationNode()
                relation_node.value = entity.name
                if entity.type is not None:
                    relation_node.ntype = RelationNodeTypeMap[entity.type]
                if entity.subtype is not None:
                    relation_node.subtype = entity.subtype
                detected_entities.append(relation_node)
        else:
            detected_entities = await self.fetcher.get_detected_entities()

        return detected_entities

    async def _parse_filters(self) -> retrieval_models.Filters:
        assert self._query is not None, "query must be parsed before filters"

        # this is a conversion between /find filters to /retrieve filters. As
        # /find keeps maintaining old filter style, we must convert from one to
        # another

        has_old_filters = (
            len(self.item.filters) > 0
            or len(self.item.resource_filters) > 0
            or len(self.item.fields) > 0
            or len(self.item.keyword_filters) > 0
            or self.item.range_creation_start is not None
            or self.item.range_creation_end is not None
            or self.item.range_modification_start is not None
            or self.item.range_modification_end is not None
        )
        if self.item.filter_expression is not None and has_old_filters:
            raise InvalidQueryError("filter_expression", "Cannot mix old filters with filter_expression")

        filter_expression = None

        if has_old_filters:
            # convert old filters into a filter expression

            operator = FilterExpression.Operator.AND
            field_expression: list[FieldFilterExpression] = []
            paragraph_expression: list[ParagraphFilterExpression] = []

            if self.item.range_creation_start or self.item.range_creation_end:
                field_expression.append(
                    DateCreated(
                        since=self.item.range_creation_start,
                        until=self.item.range_creation_end,
                    )
                )

            if self.item.range_modification_start or self.item.range_modification_end:
                field_expression.append(
                    DateModified(
                        since=self.item.range_modification_start,
                        until=self.item.range_modification_end,
                    )
                )

            if self.item.filters:
                classification_labels = await self.fetcher.get_classification_labels()
                field_exprs, paragraph_expr = convert_labels_to_filter_expressions(
                    self.item.filters, classification_labels
                )
                if field_exprs:
                    field_expression.extend(field_exprs)
                if paragraph_expr:
                    paragraph_expression.append(paragraph_expr)

            if self.item.keyword_filters:
                # keyword filters
                for keyword_filter in self.item.keyword_filters:
                    if isinstance(keyword_filter, str):
                        field_expression.append(Keyword(word=keyword_filter))
                    else:
                        # model validates that one and only one of these match
                        if keyword_filter.all:
                            field_expression.append(
                                And(operands=[Keyword(word=word) for word in keyword_filter.all])
                            )
                        elif keyword_filter.any:
                            field_expression.append(
                                Or(operands=[Keyword(word=word) for word in keyword_filter.any])
                            )
                        elif keyword_filter.none:
                            field_expression.append(
                                Not(
                                    operand=Or(
                                        operands=[Keyword(word=word) for word in keyword_filter.none]
                                    )
                                )
                            )
                        elif keyword_filter.not_all:
                            field_expression.append(
                                Not(
                                    operand=And(
                                        operands=[Keyword(word=word) for word in keyword_filter.not_all]
                                    )
                                )
                            )

            if self.item.fields:
                operands: list[FieldFilterExpression] = []
                for key in self.item.fields:
                    parts = key.split("/")
                    try:
                        field_type = FieldTypeName.from_abbreviation(parts[0])
                    except KeyError:  # pragma: no cover
                        raise InvalidQueryError(
                            "fields", f"field filter {key} has an invalid field type: {parts[0]}"
                        )
                    field_id = parts[1] if len(parts) > 1 else None
                    operands.append(Field(type=field_type, name=field_id))

                if len(operands) == 1:
                    field_expression.append(operands[0])
                elif len(operands) > 1:
                    field_expression.append(Or(operands=operands))

            if self.item.resource_filters:
                operands = []
                for key in self.item.resource_filters:
                    parts = key.split("/")
                    if len(parts) == 1:
                        operands.append(Resource(id=parts[0]))
                    else:
                        rid = parts[0]
                        try:
                            field_type = FieldTypeName.from_abbreviation(parts[1])
                        except KeyError:  # pragma: no cover
                            raise InvalidQueryError(
                                "resource_filters",
                                f"resource filter {key} has an invalid field type: {parts[1]}",
                            )
                        field_id = parts[2] if len(parts) > 2 else None
                        operands.append(
                            And(operands=[Resource(id=rid), Field(type=field_type, name=field_id)])
                        )

                if len(operands) == 1:
                    field_expression.append(operands[0])
                elif len(operands) > 1:
                    field_expression.append(Or(operands=operands))

            field = None
            if len(field_expression) == 1:
                field = field_expression[0]
            elif len(field_expression) > 1:
                field = And(operands=field_expression)

            paragraph = None
            if len(paragraph_expression) == 1:
                paragraph = paragraph_expression[0]
            elif len(paragraph_expression) > 1:
                paragraph = And(operands=paragraph_expression)

            if field or paragraph:
                filter_expression = FilterExpression(field=field, paragraph=paragraph, operator=operator)

        if self.item.filter_expression is not None:
            filter_expression = self.item.filter_expression

        return retrieval_models.Filters(
            filter_expression=filter_expression,
            show_hidden=self.item.show_hidden,
            security=self.item.security,
            with_duplicates=self.item.with_duplicates,
        )

    def _parse_reranker(self) -> Reranker:
        reranker: Reranker
        top_k = self.item.top_k

        if isinstance(self.item.reranker, search_models.RerankerName):
            if self.item.reranker == search_models.RerankerName.NOOP:
                reranker = NoopReranker()

            elif self.item.reranker == search_models.RerankerName.PREDICT_RERANKER:
                # for predict rearnker, by default, we want a x2 factor with a
                # top of 200 results
                reranker = PredictReranker(window=min(top_k * 2, 200))

            else:
                raise InternalParserError(f"Unknown reranker algorithm: {self.item.reranker}")

        elif isinstance(self.item.reranker, search_models.PredictReranker):
            user_window = self.item.reranker.window
            reranker = PredictReranker(window=min(max(user_window or 0, top_k), 200))

        else:
            raise InternalParserError(f"Unknown reranker {self.item.reranker}")

        return reranker


async def parse_keyword_query(
    item: search_models.BaseSearchRequest,
    *,
    fetcher: RAOFetcher,
) -> retrieval_models.KeywordQuery:
    query = item.query

    # If there was a rephrase with image, we should use the rephrased query for keyword search
    rephrased_query = await fetcher.get_rephrased_query()
    if item.query_image is not None and rephrased_query is not None:
        query = rephrased_query

    min_score = parse_keyword_min_score(item.min_score)

    return retrieval_models.KeywordQuery(
        query=query,
        # Synonym checks are done at the retrieval endpoint already
        with_synonyms=item.with_synonyms,
        min_score=min_score,
    )


async def parse_semantic_query(
    item: search_models.SearchRequest | search_models.FindRequest,
    *,
    fetcher: RAOFetcher,
) -> retrieval_models.SemanticQuery:
    vectorset = await fetcher.get_vectorset()
    query = await fetcher.get_query_vector()

    min_score = await parse_semantic_min_score(item.min_score, fetcher=fetcher)

    return retrieval_models.SemanticQuery(query=query, vectorset=vectorset, min_score=min_score)


async def parse_semantic_min_score(
    min_score: float | search_models.MinScore | None,
    *,
    fetcher: RAOFetcher,
) -> float:
    if min_score is None:
        min_score = None
    elif isinstance(min_score, float):
        min_score = min_score
    else:
        min_score = min_score.semantic
    if min_score is None:
        # min score not defined by the user, we'll try to get the default
        # from Predict API
        min_score = await fetcher.get_semantic_min_score()
        if min_score is None:
            logger.warning(
                "Semantic threshold not found in query information, using default",
                extra={"kbid": fetcher.kbid},
            )
            min_score = DEFAULT_GENERIC_SEMANTIC_THRESHOLD

    return min_score


def convert_labels_to_filter_expressions(
    label_filters: list[str] | list[Filter], classification_labels: knowledgebox_pb2.Labels
) -> tuple[list[FieldFilterExpression], ParagraphFilterExpression | None]:
    field_expressions: list[FieldFilterExpression] = []
    paragraph_expressions: list[ParagraphFilterExpression] = []

    for label_filter in label_filters:
        if isinstance(label_filter, str):
            # translate_label
            if len(label_filter) == 0:
                raise InvalidQueryError("filters", "Invalid empty label")
            if label_filter[0] != "/":
                raise InvalidQueryError(
                    "filters", f"Invalid label. It must start with a `/`: {label_filter}"
                )

            label = translate_label(label_filter)
            facet_filter = filter_from_facet(label)

            if is_paragraph_label(label, classification_labels):
                paragraph_expressions.append(facet_filter)  # type: ignore[arg-type]
            else:
                field_expressions.append(facet_filter)  # type: ignore[arg-type]

        else:
            combinator: type[And[FieldFilterExpression]] | type[Or[FieldFilterExpression]]
            if label_filter.all:
                labels = label_filter.all
                combinator, negate = And, False
            if label_filter.any:
                labels = label_filter.any
                combinator, negate = Or, False
            if label_filter.none:
                labels = label_filter.none
                combinator, negate = And, True
            if label_filter.not_all:
                labels = label_filter.not_all
                combinator, negate = Or, True

            # equivalent to split_labels
            field = []
            paragraph = []
            for label in labels:
                label = translate_label(label)
                expr = filter_from_facet(label)

                if negate:
                    expr = Not(operand=expr)  # type: ignore

                if is_paragraph_label(label, classification_labels):
                    paragraph.append(expr)
                else:
                    field.append(expr)

            if len(paragraph) > 0 and not (combinator == And and negate is False):
                raise InvalidQueryError(
                    "filters",
                    "Paragraph labels can only be used with 'all' filter",
                )

            if len(field) == 1:
                field_expressions.append(field[0])  # type: ignore
            elif len(field) > 1:
                field_expressions.append(combinator(operands=field))  # type: ignore

            if len(paragraph) == 1:
                paragraph_expressions.append(paragraph[0])  # type: ignore
            elif len(paragraph) > 1:
                paragraph_expressions.append(combinator(operands=paragraph))  # type: ignore

    if len(paragraph_expressions) == 1:
        paragraph_expression = paragraph_expressions[0]  # type: ignore
    elif len(paragraph_expressions) > 1:
        paragraph_expression = And(operands=paragraph_expressions)  # type: ignore
    else:
        paragraph_expression = None

    return field_expressions, paragraph_expression
