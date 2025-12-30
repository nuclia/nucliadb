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
from nucliadb.search.search.metrics import query_parser_observer
from nucliadb.search.search.query_parser.exceptions import InternalParserError
from nucliadb.search.search.query_parser.fetcher import Fetcher
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
from nucliadb.search.search.query_parser.parsers.common import query_with_synonyms, validate_query_syntax
from nucliadb.search.search.utils import filter_hidden_resources
from nucliadb_models import search as search_models
from nucliadb_models.filters import FilterExpression
from nucliadb_models.retrieval import RetrievalRequest
from nucliadb_models.search import MAX_RANK_FUSION_WINDOW


@query_parser_observer.wrap({"type": "parse_retrieve"})
async def parse_retrieve(kbid: str, item: RetrievalRequest) -> UnitRetrieval:
    fetcher = Fetcher(
        kbid=kbid,
        query=item.query.keyword.query if item.query.keyword else "",
        user_vector=item.query.semantic.query if item.query.semantic else None,
        vectorset=item.query.semantic.vectorset if item.query.semantic else None,
        # Retrieve doesn't use images for now
        query_image=None,
        # Retrieve doesn't do rephrasing
        rephrase=False,
        rephrase_prompt=None,
        generative_model=None,
    )
    parser = _RetrievalParser(kbid, item, fetcher)
    retrieval = await parser.parse()
    return retrieval


class _RetrievalParser:
    def __init__(self, kbid: str, item: RetrievalRequest, fetcher: Fetcher):
        self.kbid = kbid
        self.item = item
        self.fetcher = fetcher

    async def parse(self) -> UnitRetrieval:
        top_k = self.item.top_k
        query = await self._parse_query()
        filters = await self._parse_filters()
        try:
            rank_fusion = self._parse_rank_fusion()
        except ValidationError as exc:
            raise InternalParserError(f"Parsing error in rank fusion: {exc!s}") from exc

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

    async def _parse_query(self) -> Query:
        keyword = None
        if self.item.query.keyword is not None:
            keyword_query, is_synonyms_query = await self._parse_keyword_query()
            keyword = KeywordQuery(
                query=keyword_query,
                is_synonyms_query=is_synonyms_query,
                min_score=self.item.query.keyword.min_score,
            )

        semantic = None
        if self.item.query.semantic is not None:
            vectorset, query_vector = await self._parse_semantic_query()
            semantic = SemanticQuery(
                query=query_vector,
                vectorset=vectorset,
                min_score=self.item.query.semantic.min_score,
            )

        graph = None
        if self.item.query.graph is not None:
            graph = GraphQuery(query=self.item.query.graph.query)

        return Query(keyword=keyword, semantic=semantic, graph=graph)

    async def _parse_keyword_query(self) -> tuple[str, bool]:
        assert self.item.query.keyword is not None
        keyword_query = self.item.query.keyword.query
        is_synonyms_query = False
        if self.item.query.keyword.with_synonyms:
            synonyms_query = await query_with_synonyms(keyword_query, fetcher=self.fetcher)
            if synonyms_query is not None:
                keyword_query = synonyms_query
                is_synonyms_query = True

        # after all query transformations, pass a validator that can fix some
        # queries that trigger a panic on the index
        keyword_query = validate_query_syntax(keyword_query)
        return keyword_query, is_synonyms_query

    async def _parse_semantic_query(self) -> tuple[str, list[float]]:
        # Make sure the vectorset exists in the KB
        assert self.item.query.semantic is not None
        vectorset = self.item.query.semantic.vectorset
        await self.fetcher.validate_vectorset(self.kbid, vectorset)

        # Calculate the matryoshka dimension if applicable
        user_vector = self.item.query.semantic.query
        matryoshka_dimension = await self.fetcher.get_matryoshka_dimension_cached(self.kbid, vectorset)
        if matryoshka_dimension is not None:
            if len(user_vector) < matryoshka_dimension:
                raise InvalidQueryError(
                    "vector",
                    f"Invalid vector length, please check valid embedding size for {vectorset} model",
                )

            # KB using a matryoshka embeddings model, cut the query vector
            # accordingly
            query_vector = user_vector[:matryoshka_dimension]
        return vectorset, query_vector

    async def _parse_filters(self) -> Filters:
        filters = Filters()
        if self.item.filters is None:
            return filters

        if self.item.filters.filter_expression is not None:
            if self.item.filters.filter_expression.field is not None:
                filters.field_expression = await parse_expression(
                    self.item.filters.filter_expression.field,
                    self.kbid,
                )
            if self.item.filters.filter_expression.paragraph is not None:
                filters.paragraph_expression = await parse_expression(
                    self.item.filters.filter_expression.paragraph,
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
