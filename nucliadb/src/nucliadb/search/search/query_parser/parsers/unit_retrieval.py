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

from nucliadb.search.search.filters import (
    translate_label,
)
from nucliadb.search.search.metrics import (
    node_features,
    query_parser_observer,
)
from nucliadb.search.search.query import (
    apply_entities_filter,
    get_sort_field_proto,
)
from nucliadb.search.search.query_parser.filter_expression import add_and_expression
from nucliadb.search.search.query_parser.models import (
    ParsedQuery,
    PredictReranker,
    UnitRetrieval,
)
from nucliadb_models.labels import LABEL_HIDDEN, translate_system_to_alias_label
from nucliadb_models.search import (
    SortOrderMap,
)
from nucliadb_protos import nodereader_pb2, utils_pb2
from nucliadb_protos.nodereader_pb2 import SearchRequest


@query_parser_observer.wrap({"type": "convert_retrieval_to_proto"})
async def convert_retrieval_to_proto(
    parsed: ParsedQuery,
) -> tuple[SearchRequest, bool, list[str], Optional[str]]:
    converter = _Converter(parsed.retrieval)
    request = converter.into_search_request()

    # XXX: legacy values that were returned by QueryParser but not always
    # needed. We should find a better abstraction

    incomplete = is_incomplete(parsed.retrieval)
    autofilter = converter._autofilter

    rephrased_query = None
    if parsed.retrieval.query.semantic:
        rephrased_query = await parsed.fetcher.get_rephrased_query()

    return request, incomplete, autofilter, rephrased_query


class _Converter:
    def __init__(self, retrieval: UnitRetrieval):
        self.req = nodereader_pb2.SearchRequest()
        self.retrieval = retrieval

        self._autofilter: list[str] = []

    def into_search_request(self) -> nodereader_pb2.SearchRequest:
        """Generate a SearchRequest proto from a retrieval operation."""
        self._apply_text_queries()
        self._apply_semantic_query()
        self._apply_graph_query()
        self._apply_filters()
        self._apply_top_k()
        return self.req

    def _apply_text_queries(self):
        text_query = self.retrieval.query.keyword or self.retrieval.query.fulltext
        if text_query is None:
            return

        if self.retrieval.query.keyword and self.retrieval.query.fulltext:
            assert self.retrieval.query.keyword == self.retrieval.query.fulltext, (
                "search proto doesn't support different queries for fulltext and keyword search"
            )

        if self.retrieval.query.fulltext:
            self.req.document = True
            node_features.inc({"type": "documents"})
        if self.retrieval.query.keyword:
            self.req.paragraph = True
            node_features.inc({"type": "paragraphs"})

        self.req.min_score_bm25 = text_query.min_score

        if text_query.is_synonyms_query:
            self.req.advanced_query = text_query.query
        else:
            self.req.body = text_query.query

        # sort order
        sort_field = get_sort_field_proto(text_query.order_by)
        if sort_field is not None:
            self.req.order.sort_by = sort_field
            self.req.order.type = SortOrderMap[text_query.sort]  # type: ignore

    def _apply_semantic_query(self):
        if self.retrieval.query.semantic is None:
            return

        node_features.inc({"type": "vectors"})

        self.req.min_score_semantic = self.retrieval.query.semantic.min_score

        query_vector = self.retrieval.query.semantic.query
        if query_vector is not None:
            self.req.vectorset = self.retrieval.query.semantic.vectorset
            self.req.vector.extend(query_vector)

    def _apply_graph_query(self):
        if self.retrieval.query.relation is None:
            return

        node_features.inc({"type": "relations"})

        self.req.relation_subgraph.entry_points.extend(self.retrieval.query.relation.detected_entities)
        self.req.relation_subgraph.depth = 1
        self.req.relation_subgraph.deleted_groups.extend(
            self.retrieval.query.relation.deleted_entity_groups
        )
        for group_id, deleted_entities in self.retrieval.query.relation.deleted_entities.items():
            self.req.relation_subgraph.deleted_entities.append(
                nodereader_pb2.EntitiesSubgraphRequest.DeletedEntities(
                    node_subtype=group_id, node_values=deleted_entities
                )
            )

    def _apply_filters(self):
        self.req.with_duplicates = self.retrieval.filters.with_duplicates

        self.req.faceted.labels.extend(
            [translate_label(facet) for facet in self.retrieval.filters.facets]
        )

        if (
            self.retrieval.filters.security is not None
            and len(self.retrieval.filters.security.groups) > 0
        ):
            security_pb = utils_pb2.Security()
            for group_id in self.retrieval.filters.security.groups:
                if group_id not in security_pb.access_groups:
                    security_pb.access_groups.append(group_id)
            self.req.security.CopyFrom(security_pb)

        if self.retrieval.filters.field_expression:
            self.req.field_filter.CopyFrom(self.retrieval.filters.field_expression)
        if self.retrieval.filters.paragraph_expression:
            self.req.paragraph_filter.CopyFrom(self.retrieval.filters.paragraph_expression)
        self.req.filter_operator = self.retrieval.filters.filter_expression_operator

        if self.retrieval.filters.autofilter:
            entity_filters = apply_entities_filter(self.req, self.retrieval.filters.autofilter)
            self._autofilter.extend([translate_system_to_alias_label(e) for e in entity_filters])

        if self.retrieval.filters.hidden is not None:
            expr = nodereader_pb2.FilterExpression()
            if self.retrieval.filters.hidden:
                expr.facet.facet = LABEL_HIDDEN
            else:
                expr.bool_not.facet.facet = LABEL_HIDDEN

            add_and_expression(self.req.field_filter, expr)

    def _apply_top_k(self):
        """Adjust requested page size depending on rank fusion and reranking
        algorithms.

        Some rerankers want more results than the requested by the user so
        reranking can have more choices.
        """
        rank_fusion_window = 0
        if self.retrieval.rank_fusion is not None:
            rank_fusion_window = self.retrieval.rank_fusion.window

        reranker_window = 0
        if self.retrieval.reranker is not None and isinstance(self.retrieval.reranker, PredictReranker):
            reranker_window = self.retrieval.reranker.window

        self.req.result_per_page = max(
            self.req.result_per_page,
            rank_fusion_window,
            reranker_window,
        )


def is_incomplete(retrieval: UnitRetrieval) -> bool:
    if retrieval.query.semantic is None:
        return False
    incomplete = retrieval.query.semantic.query is None or len(retrieval.query.semantic.query) == 0
    return incomplete
