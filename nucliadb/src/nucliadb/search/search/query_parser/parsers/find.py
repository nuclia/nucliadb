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

from pydantic import ValidationError

from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.common.models_utils.from_proto import RelationNodeTypeMap
from nucliadb.search.search.metrics import query_parser_observer
from nucliadb.search.search.query_parser.exceptions import InternalParserError
from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb.search.search.query_parser.models import (
    GraphQuery,
    ParsedQuery,
    PredictReranker,
    Query,
    RelationQuery,
    UnitRetrieval,
)
from nucliadb.search.search.query_parser.parsers.common import parse_filters
from nucliadb_models import search as search_models
from nucliadb_models.search import FindRequest
from nucliadb_protos import utils_pb2

from .common import (
    parse_keyword_query,
    parse_rank_fusion,
    parse_reranker,
    parse_semantic_query,
    parse_top_k,
    should_disable_vector_search,
)
from .graph import _calculate_graph_vectors


@query_parser_observer.wrap({"type": "parse_find"})
async def parse_find(
    kbid: str,
    item: FindRequest,
    *,
    fetcher: Fetcher | None = None,
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
        query_image=item.query_image,
    )


class _FindParser:
    def __init__(self, kbid: str, item: FindRequest, fetcher: Fetcher):
        self.kbid = kbid
        self.item = item
        self.fetcher = fetcher

        # cached data while parsing
        self._query: Query | None = None
        self._top_k: int | None = None

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

        filters = await parse_filters(
            self.kbid,
            self.fetcher,
            show_hidden=self.item.show_hidden,
            security=self.item.security,
            with_duplicates=self.item.with_duplicates,
            filter_expression=self.item.filter_expression,
            label_filters=self.item.filters,
            keyword_filters=self.item.keyword_filters,
            resource_filters=self.item.resource_filters,
            fields=self.item.fields,
            range_creation_start=self.item.range_creation_start,
            range_creation_end=self.item.range_creation_end,
            range_modification_start=self.item.range_modification_start,
            range_modification_end=self.item.range_modification_end,
        )

        try:
            rank_fusion = parse_rank_fusion(self.item.rank_fusion, self.item.top_k)
        except ValidationError as exc:
            raise InternalParserError(f"Parsing error in rank fusion: {exc!s}") from exc
        try:
            reranker = parse_reranker(self.item.reranker, self.item.top_k)
        except ValidationError as exc:
            raise InternalParserError(f"Parsing error in reranker: {exc!s}") from exc

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
        vectors = await _calculate_graph_vectors(self.kbid, self.item.graph_query)
        return GraphQuery(query=self.item.graph_query, vectors=vectors)

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
