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
from pydantic import ValidationError

from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.common.filter_expression import parse_expression
from nucliadb.models.internal.retrieval import RetrievalRequest
from nucliadb.search.search.metrics import query_parser_observer
from nucliadb.search.search.query_parser.exceptions import InternalParserError
from nucliadb.search.search.query_parser.models import (
    Filters,
    GraphQuery,
    KeywordQuery,
    Query,
    RankFusion,
    ReciprocalRankFusion,
    SemanticQuery,
    UnitRetrieval,
)
from nucliadb.search.search.utils import filter_hidden_resources
from nucliadb_models import search as search_models
from nucliadb_models.filters import FilterExpression
from nucliadb_models.search import MAX_RANK_FUSION_WINDOW


@query_parser_observer.wrap({"type": "parse_retrieve"})
async def parse_retrieve(kbid: str, item: RetrievalRequest) -> UnitRetrieval:
    parser = _RetrievalParser(kbid, item)
    retrieval = await parser.parse()
    return retrieval


class _RetrievalParser:
    def __init__(self, kbid: str, item: RetrievalRequest):
        self.kbid = kbid
        self.item = item

    async def parse(self) -> UnitRetrieval:
        top_k = self.item.top_k
        query = self._parse_query()
        filters = await self._parse_filters()
        rank_fusion = self._parse_rank_fusion()

        try:
            rank_fusion = self._parse_rank_fusion()
        except ValidationError as exc:
            raise InternalParserError(f"Parsing error in rank fusion: {str(exc)}") from exc

        # ensure top_k and rank_fusion are coherent
        if top_k > rank_fusion.window:
            raise InvalidQueryError(
                "rank_fusion.window", "Rank fusion window must be greater or equal to top_k"
            )

        retrieval = UnitRetrieval(
            query=query,
            top_k=top_k,
            filters=filters,
            rank_fusion=rank_fusion,
            reranker=None,
        )
        return retrieval

    def _parse_query(self) -> Query:
        keyword = None
        if self.item.query.keyword is not None:
            keyword = KeywordQuery(
                query=self.item.query.keyword.query,
                is_synonyms_query=False,
                min_score=self.item.query.keyword.min_score,
            )

        semantic = None
        if self.item.query.semantic is not None:
            semantic = SemanticQuery(
                query=self.item.query.semantic.query,
                vectorset=self.item.query.semantic.vectorset,
                min_score=self.item.query.semantic.min_score,
            )

        graph = None
        if self.item.query.graph is not None:
            graph = GraphQuery(query=self.item.query.graph.query)

        return Query(keyword=keyword, semantic=semantic, graph=graph)

    async def _parse_filters(self) -> Filters:
        filters = Filters()
        if self.item.filters is None:
            return filters

        if self.item.filters.filter_expression is not None:
            # FIXME: remove this type ignore and fix the Union issue
            filters.field_expression = await parse_expression(
                self.item.filters.filter_expression.field,  # type: ignore
                self.kbid,
            )
            # FIXME: remove this type ignore and fix the Union issue
            filters.paragraph_expression = await parse_expression(
                self.item.filters.filter_expression.paragraph,  # type: ignore
                self.kbid,
            )
            if self.item.filters.filter_expression.operator == FilterExpression.Operator.OR:
                filter_operator = nodereader_pb2.FilterOperator.OR
            else:
                filter_operator = nodereader_pb2.FilterOperator.AND
            filters.filter_expression_operator = filter_operator

        filters.hidden = await filter_hidden_resources(self.kbid, self.item.filters.show_hidden)
        filters.security = self.item.filters.security
        filters.with_duplicates = self.item.filters.with_duplicates

        return filters

    # TODO: adapted from find parser, we may want to put it in common
    def _parse_rank_fusion(self) -> RankFusion:
        rank_fusion: RankFusion

        top_k = self.item.top_k
        window = min(top_k, MAX_RANK_FUSION_WINDOW)

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
