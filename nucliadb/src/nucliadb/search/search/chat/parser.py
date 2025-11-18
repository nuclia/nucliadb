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
from typing import Optional, Union

from nidx_protos import nodereader_pb2
from pydantic import ValidationError

from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.common.filter_expression import parse_expression
from nucliadb.common.models_utils.from_proto import RelationNodeTypeMap
from nucliadb.models.internal import retrieval as retrieval_models
from nucliadb.models.internal.retrieval import KeywordQuery, RetrievalRequest, SemanticQuery
from nucliadb.search.search.chat.fetcher import RAOFetcher
from nucliadb.search.search.query_parser.exceptions import InternalParserError
from nucliadb.search.search.query_parser.models import (
    Filters,
    GraphQuery,
    NoopReranker,
    ParsedQuery,
    PredictReranker,
    Query,
    RankFusion,
    ReciprocalRankFusion,
    RelationQuery,
    Reranker,
    UnitRetrieval,
)
from nucliadb.search.search.query_parser.old_filters import OldFilterParams, parse_old_filters
from nucliadb.search.search.query_parser.parsers.common import (
    parse_keyword_min_score,
    should_disable_vector_search,
)
from nucliadb.search.search.utils import filter_hidden_resources
from nucliadb_models import search as search_models
from nucliadb_models.filters import FilterExpression
from nucliadb_models.search import (
    FindRequest,
)
from nucliadb_protos import utils_pb2

logger = logging.getLogger(__name__)

DEFAULT_GENERIC_SEMANTIC_THRESHOLD = 0.7


async def rao_parse_find(kbid: str, find_request: FindRequest) -> ParsedQuery:
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
    retrieval_unit = await parser.parse()
    return ParsedQuery(fetcher=fetcher, retrieval=retrieval_unit, generation=None)


def build_retrieval_request(kbid: str, parsed: ParsedQuery) -> RetrievalRequest:
    retrieval_query = retrieval_models.Query()
    parsed_keyword = parsed.retrieval.query.keyword
    if parsed_keyword:
        retrieval_query.keyword = retrieval_models.KeywordQuery(
            query=parsed_keyword.query,
            min_score=parsed_keyword.min_score,
            with_synonyms=parsed_keyword.is_synonyms_query,
        )
    parsed_semantic = parsed.retrieval.query.semantic
    if parsed_semantic:
        assert parsed_semantic.query is not None
        retrieval_query.semantic = retrieval_models.SemanticQuery(
            query=parsed_semantic.query,
            vectorset=parsed_semantic.vectorset,
            min_score=parsed_semantic.min_score,
        )
    parsed_graph = parsed.retrieval.query.graph
    if parsed_graph:
        retrieval_query.graph = retrieval_models.GraphQuery(
            query=parsed_graph.query,
        )
    retrieval_request = RetrievalRequest(
        query=retrieval_query,
        top_k=parsed.retrieval.top_k,
        # TODO convert to internal filters
        filters=parsed.retrieval.filters,
        rank_fusion=parsed.retrieval.rank_fusion,
    )
    return retrieval_request


class RAOFindParser:
    def __init__(self, kbid: str, item: FindRequest, fetcher: RAOFetcher):
        self.kbid = kbid
        self.item = item
        self.fetcher = fetcher

        # cached data while parsing
        self._query: Optional[Query] = None
        self._top_k: Optional[int] = None

    async def parse(self) -> UnitRetrieval:
        self._validate_request()

        self._top_k = self.item.top_k

        # parse search types (features)

        self._query = Query()

        if search_models.FindOptions.KEYWORD in self.item.features:
            self._query.keyword = await parse_keyword_query(self.item, fetcher=self.fetcher)  # type: ignore

        if search_models.FindOptions.SEMANTIC in self.item.features:
            self._query.semantic = await parse_semantic_query(self.item, fetcher=self.fetcher)  # type: ignore

        if search_models.FindOptions.RELATIONS in self.item.features:
            self._query.relation = await self._parse_relation_query()

        if search_models.FindOptions.GRAPH in self.item.features:
            self._query.graph = await self._parse_graph_query()

        filters = await self._parse_filters()

        try:
            rank_fusion = self._parse_rank_fusion()
        except ValidationError as exc:
            raise InternalParserError(f"Parsing error in rank fusion: {str(exc)}") from exc
        try:
            reranker = self._parse_reranker()
        except ValidationError as exc:
            raise InternalParserError(f"Parsing error in reranker: {str(exc)}") from exc

        # Adjust retrieval windows. Our current implementation assume:
        # `top_k <= reranker.window <= rank_fusion.window`
        # and as rank fusion is done before reranking, we must ensure rank
        # fusion window is at least, the reranker window
        if isinstance(reranker, PredictReranker):
            rank_fusion.window = max(rank_fusion.window, reranker.window)

        retrieval = UnitRetrieval(
            query=self._query,
            top_k=self._top_k,
            filters=filters,
            rank_fusion=rank_fusion,
            reranker=reranker,
        )
        return retrieval

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

    async def _parse_graph_query(self) -> GraphQuery:
        if self.item.graph_query is None:
            raise InvalidQueryError(
                "graph_query", "Graph query must be provided when using graph search"
            )
        return GraphQuery(query=self.item.graph_query)

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

    async def _parse_filters(self) -> Filters:
        assert self._query is not None, "query must be parsed before filters"

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

        field_expr = None
        paragraph_expr = None
        filter_operator = nodereader_pb2.FilterOperator.AND

        if has_old_filters:
            old_filters = OldFilterParams(
                label_filters=self.item.filters,
                keyword_filters=self.item.keyword_filters,
                range_creation_start=self.item.range_creation_start,
                range_creation_end=self.item.range_creation_end,
                range_modification_start=self.item.range_modification_start,
                range_modification_end=self.item.range_modification_end,
                fields=self.item.fields,
                key_filters=self.item.resource_filters,
            )
            field_expr, paragraph_expr = await parse_old_filters(old_filters, self.fetcher)

        if self.item.filter_expression is not None:
            if self.item.filter_expression.field:
                field_expr = await parse_expression(self.item.filter_expression.field, self.kbid)
            if self.item.filter_expression.paragraph:
                paragraph_expr = await parse_expression(self.item.filter_expression.paragraph, self.kbid)
            if self.item.filter_expression.operator == FilterExpression.Operator.OR:
                filter_operator = nodereader_pb2.FilterOperator.OR
            else:
                filter_operator = nodereader_pb2.FilterOperator.AND

        hidden = await filter_hidden_resources(self.kbid, self.item.show_hidden)

        return Filters(
            facets=[],
            field_expression=field_expr,
            paragraph_expression=paragraph_expr,
            filter_expression_operator=filter_operator,
            security=self.item.security,
            hidden=hidden,
            with_duplicates=self.item.with_duplicates,
        )

    def _parse_rank_fusion(self) -> RankFusion:
        rank_fusion: RankFusion

        top_k = self.item.top_k
        window = min(top_k, 500)

        if isinstance(self.item.rank_fusion, search_models.RankFusionName):
            if self.item.rank_fusion == search_models.RankFusionName.RECIPROCAL_RANK_FUSION:
                rank_fusion = ReciprocalRankFusion(window=window)
            else:
                raise InternalParserError(f"Unknown rank fusion algorithm: {self.item.rank_fusion}")

        elif isinstance(self.item.rank_fusion, search_models.ReciprocalRankFusion):
            user_window = self.item.rank_fusion.window
            rank_fusion = ReciprocalRankFusion(
                k=self.item.rank_fusion.k,
                boosting=self.item.rank_fusion.boosting,
                window=min(max(user_window or 0, top_k), 500),
            )

        else:
            raise InternalParserError(f"Unknown rank fusion {self.item.rank_fusion}")

        return rank_fusion

    def _parse_reranker(self) -> Reranker:
        reranking: Reranker

        top_k = self.item.top_k

        if isinstance(self.item.reranker, search_models.RerankerName):
            if self.item.reranker == search_models.RerankerName.NOOP:
                reranking = NoopReranker()

            elif self.item.reranker == search_models.RerankerName.PREDICT_RERANKER:
                # for predict rearnker, by default, we want a x2 factor with a
                # top of 200 results
                reranking = PredictReranker(window=min(top_k * 2, 200))

            else:
                raise InternalParserError(f"Unknown reranker algorithm: {self.item.reranker}")

        elif isinstance(self.item.reranker, search_models.PredictReranker):
            user_window = self.item.reranker.window
            reranking = PredictReranker(window=min(max(user_window or 0, top_k), 200))

        else:
            raise InternalParserError(f"Unknown reranker {self.item.reranker}")

        return reranking


async def parse_keyword_query(
    item: search_models.BaseSearchRequest,
    *,
    fetcher: RAOFetcher,
) -> KeywordQuery:
    query = item.query

    # If there was a rephrase with image, we should use the rephrased query for keyword search
    rephrased_query = await fetcher.get_rephrased_query()
    if item.query_image is not None and rephrased_query is not None:
        query = rephrased_query

    min_score = parse_keyword_min_score(item.min_score)

    return KeywordQuery(
        query=query,
        # Synonym checks are done at the retrieval endpoint already
        with_synonyms=item.with_synonyms,
        min_score=min_score,
    )


async def parse_semantic_query(
    item: Union[search_models.SearchRequest, search_models.FindRequest],
    *,
    fetcher: RAOFetcher,
) -> SemanticQuery:
    vectorset = await fetcher.get_vectorset()
    query = await fetcher.get_query_vector()

    min_score = await parse_semantic_min_score(item.min_score, fetcher=fetcher)

    return SemanticQuery(query=query, vectorset=vectorset, min_score=min_score)


async def parse_semantic_min_score(
    min_score: Optional[Union[float, search_models.MinScore]],
    *,
    fetcher: RAOFetcher,
):
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
