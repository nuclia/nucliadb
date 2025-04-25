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

from typing import Optional

from nidx_protos import nodereader_pb2
from pydantic import ValidationError

from nucliadb.common.models_utils.from_proto import RelationNodeTypeMap
from nucliadb.search.search.metrics import query_parser_observer
from nucliadb.search.search.query import expand_entities
from nucliadb.search.search.query_parser.exceptions import InternalParserError, InvalidQueryError
from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb.search.search.query_parser.filter_expression import parse_expression
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
from nucliadb.search.search.utils import filter_hidden_resources
from nucliadb_models import search as search_models
from nucliadb_models.filters import FilterExpression
from nucliadb_models.search import (
    FindRequest,
)
from nucliadb_protos import utils_pb2

from .common import (
    parse_keyword_query,
    parse_semantic_query,
    parse_top_k,
    should_disable_vector_search,
    validate_query_syntax,
)


@query_parser_observer.wrap({"type": "parse_find"})
async def parse_find(
    kbid: str,
    item: FindRequest,
    *,
    fetcher: Optional[Fetcher] = None,
) -> ParsedQuery:
    fetcher = fetcher or fetcher_for_find(kbid, item)
    parser = _FindParser(kbid, item, fetcher)
    retrieval = await parser.parse()
    return ParsedQuery(fetcher=fetcher, retrieval=retrieval, generation=None)


def fetcher_for_find(kbid: str, item: FindRequest) -> Fetcher:
    return Fetcher(
        kbid=kbid,
        query=item.query,
        user_vector=item.vector,
        vectorset=item.vectorset,
        rephrase=item.rephrase,
        rephrase_prompt=item.rephrase_prompt,
        generative_model=item.generative_model,
    )


class _FindParser:
    def __init__(self, kbid: str, item: FindRequest, fetcher: Fetcher):
        self.kbid = kbid
        self.item = item
        self.fetcher = fetcher

        # cached data while parsing
        self._query: Optional[Query] = None
        self._top_k: Optional[int] = None

    async def parse(self) -> UnitRetrieval:
        self._validate_request()

        self._top_k = parse_top_k(self.item)

        # parse search types (features)

        self._query = Query()

        if search_models.FindOptions.KEYWORD in self.item.features:
            self._query.keyword = await parse_keyword_query(self.item, fetcher=self.fetcher)

        if search_models.FindOptions.SEMANTIC in self.item.features:
            self._query.semantic = await parse_semantic_query(self.item, fetcher=self.fetcher)

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
        validate_query_syntax(self.item.query)

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

        deleted_entity_groups = await self.fetcher.get_deleted_entity_groups()

        meta_cache = await self.fetcher.get_entities_meta_cache()
        deleted_entities = meta_cache.deleted_entities

        return RelationQuery(
            entry_points=detected_entities,
            deleted_entity_groups=deleted_entity_groups,
            deleted_entities=deleted_entities,
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

        meta_cache = await self.fetcher.get_entities_meta_cache()
        detected_entities = expand_entities(meta_cache, detected_entities)

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

        autofilter = None
        if self.item.autofilter:
            if self._query.relation is not None:
                autofilter = self._query.relation.entry_points
            else:
                autofilter = await self._get_detected_entities()

        hidden = await filter_hidden_resources(self.kbid, self.item.show_hidden)

        return Filters(
            autofilter=autofilter,
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

        top_k = parse_top_k(self.item)
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

        top_k = parse_top_k(self.item)

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
