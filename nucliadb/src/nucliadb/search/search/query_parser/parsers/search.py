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

from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.search.search.metrics import query_parser_observer
from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb.search.search.query_parser.models import (
    Filters,
    ParsedQuery,
    Query,
    RelationQuery,
    UnitRetrieval,
    _TextQuery,
)
from nucliadb.search.search.query_parser.parsers.common import parse_filters
from nucliadb_models import search as search_models
from nucliadb_models.search import (
    SearchRequest,
    SortField,
    SortOptions,
    SortOrder,
)
from nucliadb_protos import utils_pb2

from .common import (
    parse_keyword_query,
    parse_semantic_query,
    parse_top_k,
    should_disable_vector_search,
)

INDEX_SORTABLE_FIELDS = [
    SortField.CREATED,
    SortField.MODIFIED,
]


@query_parser_observer.wrap({"type": "parse_search"})
async def parse_search(kbid: str, item: SearchRequest, *, fetcher: Fetcher | None = None) -> ParsedQuery:
    fetcher = fetcher or fetcher_for_search(kbid, item)
    parser = _SearchParser(kbid, item, fetcher)
    retrieval = await parser.parse()
    return ParsedQuery(fetcher=fetcher, retrieval=retrieval, generation=None)


def fetcher_for_search(kbid: str, item: SearchRequest) -> Fetcher:
    return Fetcher(
        kbid=kbid,
        query=item.query,
        user_vector=item.vector,
        vectorset=item.vectorset,
        rephrase=item.rephrase,
        rephrase_prompt=item.rephrase_prompt,
        generative_model=None,
        query_image=None,
    )


class _SearchParser:
    def __init__(self, kbid: str, item: SearchRequest, fetcher: Fetcher):
        self.kbid = kbid
        self.item = item
        self.fetcher = fetcher

        # cached data while parsing
        self._query: Query | None = None
        self._top_k: int | None = None

    async def parse(self) -> UnitRetrieval:
        self._validate_request()

        self._top_k = parse_top_k(self.item)

        if self._top_k > 0 and self.item.offset > 0:
            self._top_k += self.item.offset

        # parse search types (features)

        self._query = Query()

        if search_models.SearchOptions.KEYWORD in self.item.features:
            keyword = await self._parse_text_query()
            self._query.keyword = keyword

        if search_models.SearchOptions.FULLTEXT in self.item.features:
            # copy from keyword, as everything is the same and we can't search
            # anything different right now
            keyword = self._query.keyword or (await self._parse_text_query())
            self._query.fulltext = keyword

        if search_models.SearchOptions.SEMANTIC in self.item.features:
            self._query.semantic = await parse_semantic_query(self.item, fetcher=self.fetcher)

        if search_models.SearchOptions.RELATIONS in self.item.features:
            self._query.relation = await self._parse_relation_query()

        filters = await self._parse_filters()

        retrieval = UnitRetrieval(
            query=self._query,
            top_k=self._top_k,
            filters=filters,
        )
        return retrieval

    def _validate_request(self):
        # synonyms are not compatible with vector/graph search
        if (
            self.item.with_synonyms
            and self.item.query
            and (
                search_models.SearchOptions.SEMANTIC in self.item.features
                or search_models.SearchOptions.RELATIONS in self.item.features
            )
        ):
            raise InvalidQueryError(
                "synonyms",
                "Search with custom synonyms is only supported on paragraph and document search",
            )

        if search_models.SearchOptions.SEMANTIC in self.item.features:
            if should_disable_vector_search(self.item):
                self.item.features.remove(search_models.SearchOptions.SEMANTIC)

    async def _parse_text_query(self) -> _TextQuery:
        assert self._top_k is not None, "top_k must be parsed before text query"

        keyword = await parse_keyword_query(self.item, fetcher=self.fetcher)
        sort, order_by = self._parse_sorting()
        keyword.sort = sort
        keyword.order_by = order_by

        return keyword

    async def _parse_relation_query(self) -> RelationQuery:
        detected_entities = await self._get_detected_entities()

        return RelationQuery(
            entry_points=detected_entities, deleted_entity_groups=[], deleted_entities={}
        )

    async def _get_detected_entities(self) -> list[utils_pb2.RelationNode]:
        detected_entities = await self.fetcher.get_detected_entities()
        return detected_entities

    def _parse_sorting(self) -> tuple[search_models.SortOrder, search_models.SortField]:
        sort = self.item.sort
        if sort is None:
            if len(self.item.query) == 0:
                sort = SortOptions(
                    field=SortField.CREATED,
                    order=SortOrder.DESC,
                )
            else:
                sort = SortOptions(
                    field=SortField.SCORE,
                    order=SortOrder.DESC,
                )

        return (sort.order, sort.field)

    async def _parse_filters(self) -> Filters:
        assert self._query is not None, "query must be parsed before filters"

        return await parse_filters(
            self.kbid,
            self.fetcher,
            show_hidden=self.item.show_hidden,
            security=self.item.security,
            with_duplicates=self.item.with_duplicates,
            filter_expression=self.item.filter_expression,
            label_filters=self.item.filters,
            keyword_filters=[],
            resource_filters=self.item.resource_filters,
            fields=self.item.fields,
            range_creation_start=self.item.range_creation_start,
            range_creation_end=self.item.range_creation_end,
            range_modification_start=self.item.range_modification_start,
            range_modification_end=self.item.range_modification_end,
        )
