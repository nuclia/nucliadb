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
from typing_extensions import assert_never

import nucliadb_models.retrieval
from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.common.filter_expression import parse_expression
from nucliadb.search.search.metrics import query_parser_observer
from nucliadb.search.search.query_parser.exceptions import InternalParserError
from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb.search.search.query_parser.models import (
    Filters,
    GraphQuery,
    KeywordQuery,
    ParsedQuery,
    PredictReranker,
    Query,
    SemanticQuery,
    UnitRetrieval,
)
from nucliadb.search.search.query_parser.parsers.common import query_with_synonyms, validate_query_syntax
from nucliadb.search.search.utils import filter_hidden_resources
from nucliadb_models.filters import FilterExpression
from nucliadb_models.retrieval import RetrievalRequest

from .common import parse_rank_fusion, parse_reranker


@query_parser_observer.wrap({"type": "parse_retrieve"})
async def parse_retrieve(kbid: str, item: RetrievalRequest) -> ParsedQuery:
    if isinstance(item.query, nucliadb_models.retrieval.RawQuery):
        query = ""
        if item.query.keyword is not None:
            query = item.query.keyword.query

        user_vector = None
        vectorset = None
        if item.query.semantic is not None:
            user_vector = item.query.semantic.query
            vectorset = item.query.semantic.vectorset

        # Low-level queries don't support images (for now)
        query_image = None

        # nor rephrasing
        rephrase = False
        rephrase_prompt = None

    elif isinstance(item.query, nucliadb_models.retrieval.Query):
        query = item.query.query or ""

        user_vector = None
        vectorset = None
        if isinstance(item.query.override.semantic, nucliadb_models.retrieval.SemanticOverrides):
            if item.query.override.semantic.vector is not None:
                user_vector = item.query.override.semantic.vector
            if item.query.override.semantic.vectorset is not None:
                vectorset = item.query.override.semantic.vectorset

        query_image = item.query.image

        rephrase = False
        rephrase_prompt = None
        if item.query.rephrase:
            rephrase = True
            if isinstance(item.query.rephrase, nucliadb_models.retrieval.Rephrase):
                rephrase_prompt = item.query.rephrase.prompt

    else:  # pragma: no cover
        assert_never(item.query)

    fetcher = Fetcher(
        kbid=kbid,
        query=query,
        user_vector=user_vector,
        vectorset=vectorset,
        query_image=query_image,
        rephrase=rephrase,
        rephrase_prompt=rephrase_prompt,
        generative_model=None,
    )
    parser = _RetrievalParser(kbid, item, fetcher)
    retrieval = await parser.parse()

    return ParsedQuery(
        fetcher=fetcher,
        retrieval=retrieval,
        generation=None,
    )


class _RetrievalParser:
    def __init__(self, kbid: str, item: RetrievalRequest, fetcher: Fetcher):
        self.kbid = kbid
        self.item: RetrievalRequest = item
        self.fetcher = fetcher

    async def parse(self) -> UnitRetrieval:
        top_k = self.item.top_k
        query = await self._parse_query()
        filters = await self._parse_filters()
        try:
            rank_fusion = parse_rank_fusion(self.item.rank_fusion, self.item.top_k)
        except ValidationError as exc:
            raise InternalParserError(f"Parsing error in rank fusion: {exc!s}") from exc

        reranker = None
        if self.item.reranker is not None:
            try:
                reranker = parse_reranker(self.item.reranker, self.item.top_k)
            except ValidationError as exc:
                raise InternalParserError(f"Parsing error in reranker: {exc!s}") from exc

        # ensure top_k and rank_fusion are coherent
        if top_k > rank_fusion.window:
            raise InvalidQueryError(
                "rank_fusion.window", "Rank fusion window must be greater or equal to top_k"
            )

        # Adjust retrieval windows. Our current implementation assume:
        # `top_k <= reranker.window <= rank_fusion.window`
        # and as rank fusion is done before reranking, we must ensure rank
        # fusion window is at least, the reranker window
        if isinstance(reranker, PredictReranker):
            rank_fusion.window = max(rank_fusion.window, reranker.window)

        retrieval = UnitRetrieval(
            query=query,
            top_k=top_k,
            filters=filters,
            rank_fusion=rank_fusion,
            reranker=reranker,
        )
        return retrieval

    @query_parser_observer.wrap({"type": "retrieve_parse_query"})
    async def _parse_query(self) -> Query:
        if isinstance(self.item.query, nucliadb_models.retrieval.RawQuery):
            query = self.item.query
        elif isinstance(self.item.query, nucliadb_models.retrieval.Query):
            query = await self._into_raw_query(self.item.query)
        else:  # pragma: no cover
            assert_never(self.item.query)
            assert False, "SAST tools don't trust this yet"

        keyword = None
        if query.keyword is not None:
            keyword_query, is_synonyms_query = await self._parse_keyword_query(query.keyword)
            keyword = KeywordQuery(
                query=keyword_query,
                is_synonyms_query=is_synonyms_query,
                min_score=query.keyword.min_score,
            )

        semantic = None
        if query.semantic is not None:
            vectorset, query_vector = await self._parse_semantic_query(query.semantic)
            semantic = SemanticQuery(
                query=query_vector,
                vectorset=vectorset,
                min_score=query.semantic.min_score,
            )

        graph = None
        if query.graph is not None:
            graph = GraphQuery(query=query.graph.query)

        return Query(keyword=keyword, semantic=semantic, graph=graph)

    async def _into_raw_query(
        self, query: nucliadb_models.retrieval.Query
    ) -> nucliadb_models.retrieval.RawQuery:
        if query.override.keyword == "disabled":
            keyword = None
        else:
            keyword = nucliadb_models.retrieval.KeywordQuery(
                query=query.query,
            )
            if query.override.keyword is None:
                # nothing to override
                pass
            elif isinstance(query.override.keyword, nucliadb_models.retrieval.KeywordOverrides):
                if query.override.keyword.min_score is not None:
                    keyword.min_score = query.override.keyword.min_score
                if query.override.keyword.with_synonyms:
                    keyword.with_synonyms = query.override.keyword.with_synonyms
            else:  # pragma: no cover
                assert_never(query.override.keyword)

        semantic = None
        if query.override.semantic != "disabled":
            vector = None
            vectorset = None
            user_semantic_min_score = None

            if query.override.semantic is None:
                # nothing to override
                pass

            elif isinstance(query.override.semantic, nucliadb_models.retrieval.SemanticOverrides):
                if query.override.semantic.vector is not None:
                    vector = query.override.semantic.vector
                if query.override.semantic.vectorset is not None:
                    vectorset = query.override.semantic.vectorset
                if query.override.semantic.min_score is not None:
                    user_semantic_min_score = query.override.semantic.min_score

            else:  # pragma: no cover
                assert_never(query.override.semantic)

            if vectorset is None:
                vectorset = await self.fetcher.get_vectorset()
            if vector is None:
                vector = await self.fetcher.get_query_vector()

            if vector is None:
                # can't semantic search, user didn't provide a vector and we
                # didn't manage to obtain it from Predict API either. We skip
                # semantic search in order to, at least, return some results
                pass
            else:
                semantic = nucliadb_models.retrieval.SemanticQuery(
                    query=vector,
                    vectorset=vectorset,
                )

                if user_semantic_min_score is not None:
                    semantic.min_score = user_semantic_min_score

        if query.override.graph == "disabled":
            graph = None
        elif query.override.graph is None:
            # we haven't implemented yet an automatic graph query from user input
            graph = None
        elif isinstance(query.override.graph, nucliadb_models.retrieval.GraphOverrides):
            if query.override.graph.query is None:
                graph = None
            else:
                graph = nucliadb_models.retrieval.GraphQuery(query=query.override.graph.query)
        else:  # pragma: no cover
            assert_never(query.override.graph)

        return nucliadb_models.retrieval.RawQuery(
            keyword=keyword,
            semantic=semantic,
            graph=graph,
        )

    async def _parse_keyword_query(
        self, keyword: nucliadb_models.retrieval.KeywordQuery
    ) -> tuple[str, bool]:
        keyword_query = keyword.query
        is_synonyms_query = False
        if keyword.with_synonyms:
            synonyms_query = await query_with_synonyms(keyword_query, fetcher=self.fetcher)
            if synonyms_query is not None:
                keyword_query = synonyms_query
                is_synonyms_query = True

        # after all query transformations, pass a validator that can fix some
        # queries that trigger a panic on the index
        keyword_query = validate_query_syntax(keyword_query)
        return keyword_query, is_synonyms_query

    async def _parse_semantic_query(
        self, semantic: nucliadb_models.retrieval.SemanticQuery
    ) -> tuple[str, list[float]]:
        # Make sure the vectorset exists in the KB and is valid
        vectorset = await self.fetcher.get_user_vectorset()
        assert vectorset is not None, "retrieve always enforces a vectorset on semantic search"

        # Calculate the matryoshka dimension if applicable
        user_vector = semantic.query
        matryoshka_dimension = await self.fetcher.get_matryoshka_dimension()

        query_vector = user_vector
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

    @query_parser_observer.wrap({"type": "retrieve_parse_filters"})
    async def _parse_filters(self) -> Filters:
        filters = Filters()

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
