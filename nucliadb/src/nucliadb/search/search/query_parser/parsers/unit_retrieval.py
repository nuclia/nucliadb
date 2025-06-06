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
from nidx_protos.nodereader_pb2 import SearchRequest

from nucliadb.common.filter_expression import add_and_expression
from nucliadb.search.search.filters import translate_label
from nucliadb.search.search.metrics import node_features, query_parser_observer
from nucliadb.search.search.query import apply_entities_filter, get_sort_field_proto
from nucliadb.search.search.query_parser.models import ParsedQuery, PredictReranker, UnitRetrieval
from nucliadb.search.search.query_parser.parsers.graph import parse_path_query
from nucliadb_models.labels import LABEL_HIDDEN, translate_system_to_alias_label
from nucliadb_models.search import SortOrderMap
from nucliadb_protos import utils_pb2


@query_parser_observer.wrap({"type": "convert_retrieval_to_proto"})
async def legacy_convert_retrieval_to_proto(
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


@query_parser_observer.wrap({"type": "convert_retrieval_to_proto"})
def convert_retrieval_to_proto(retrieval: UnitRetrieval) -> SearchRequest:
    converter = _Converter(retrieval)
    request = converter.into_search_request()
    return request


class _Converter:
    def __init__(self, retrieval: UnitRetrieval):
        self.req = nodereader_pb2.SearchRequest()
        self.retrieval = retrieval

        self._autofilter: list[str] = []

    def into_search_request(self) -> nodereader_pb2.SearchRequest:
        """Generate a SearchRequest proto from a retrieval operation."""
        self._apply_text_queries()
        self._apply_semantic_query()
        self._apply_relation_query()
        self._apply_graph_query()
        self._apply_filters()
        self._apply_top_k()
        return self.req

    def _apply_text_queries(self) -> None:
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

    def _apply_semantic_query(self) -> None:
        if self.retrieval.query.semantic is None:
            return

        node_features.inc({"type": "vectors"})

        self.req.min_score_semantic = self.retrieval.query.semantic.min_score

        query_vector = self.retrieval.query.semantic.query
        if query_vector is not None:
            self.req.vectorset = self.retrieval.query.semantic.vectorset
            self.req.vector.extend(query_vector)

    def _apply_relation_query(self) -> None:
        """Relation queries are the legacy way to query the knowledge graph.
        Given a set of entry points and some subtypes and entities to exclude
        from search, it'd find the distance 1 neighbours (BFS)."""

        if self.retrieval.query.relation is None:
            return

        node_features.inc({"type": "relations"})

        # Entry points are source or target nodes we want to search for. We want
        # any undirected path containing any entry point
        entry_points_queries = []
        for entry_point in self.retrieval.query.relation.entry_points:
            q = nodereader_pb2.GraphQuery.PathQuery()
            if entry_point.value:
                q.path.source.value = entry_point.value
            q.path.source.node_type = entry_point.ntype
            if entry_point.subtype:
                q.path.source.node_subtype = entry_point.subtype
            q.path.undirected = True
            entry_points_queries.append(q)

        # A query can specifiy nodes marked as deleted in the db (but not
        # removed from the index). We want to exclude any path containing any of
        # those nodes.
        #
        # The request groups values per subtype (to optimize request size) but,
        # as we don't support OR at node value level, we'll split them.
        deleted_nodes_queries = []
        for subtype, deleted_entities in self.retrieval.query.relation.deleted_entities.items():
            if len(deleted_entities) == 0:
                continue
            for deleted_entity_value in deleted_entities:
                q = nodereader_pb2.GraphQuery.PathQuery()
                q.path.source.value = deleted_entity_value
                q.path.source.node_subtype = subtype
                q.path.undirected = True
                deleted_nodes_queries.append(q)

        # Subtypes can also be marked as deleted in the db (but kept in the
        # index). We also want to exclude any triplet containg a node with such
        # subtypes
        excluded_subtypes_queries = []
        for deleted_subtype in self.retrieval.query.relation.deleted_entity_groups:
            q = nodereader_pb2.GraphQuery.PathQuery()
            q.path.source.node_subtype = deleted_subtype
            q.path.undirected = True
            excluded_subtypes_queries.append(q)

        subqueries = []

        if len(entry_points_queries) > 0:
            if len(entry_points_queries) == 1:
                q = entry_points_queries[0]
            else:
                q = nodereader_pb2.GraphQuery.PathQuery()
                q.bool_or.operands.extend(entry_points_queries)
            subqueries.append(q)

        if len(deleted_nodes_queries) > 0:
            q = nodereader_pb2.GraphQuery.PathQuery()
            if len(deleted_nodes_queries) == 1:
                q.bool_not.CopyFrom(deleted_nodes_queries[0])
            else:
                q.bool_not.bool_or.operands.extend(deleted_nodes_queries)
            subqueries.append(q)

        if len(excluded_subtypes_queries) > 0:
            q = nodereader_pb2.GraphQuery.PathQuery()
            if len(excluded_subtypes_queries) == 1:
                q.bool_not.CopyFrom(excluded_subtypes_queries[0])
            else:
                q.bool_not.bool_or.operands.extend(excluded_subtypes_queries)
            subqueries.append(q)

        if len(subqueries) == 0:
            # don't set anything, no graph query
            pass
        elif len(subqueries) == 1:
            q = subqueries[0]
            self.req.graph_search.query.path.CopyFrom(q)
        else:
            self.req.graph_search.query.path.bool_and.operands.extend(subqueries)

    def _apply_graph_query(self) -> None:
        if self.retrieval.query.graph is None:
            return

        q = parse_path_query(self.retrieval.query.graph.query)
        self.req.graph_search.query.path.CopyFrom(q)

    def _apply_filters(self) -> None:
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

    def _apply_top_k(self) -> None:
        """Adjust requested page size depending on rank fusion and reranking
        algorithms.

        Some rerankers want more results than the requested by the user so
        reranking can have more choices.
        """
        top_k = self.retrieval.top_k

        rank_fusion_window = 0
        if self.retrieval.rank_fusion is not None:
            rank_fusion_window = self.retrieval.rank_fusion.window

        reranker_window = 0
        if self.retrieval.reranker is not None and isinstance(self.retrieval.reranker, PredictReranker):
            reranker_window = self.retrieval.reranker.window

        self.req.result_per_page = max(
            top_k,
            rank_fusion_window,
            reranker_window,
        )


def is_incomplete(retrieval: UnitRetrieval) -> bool:
    """
    Return true if the retrieval had the semantic feature on but the query endpoint
    did not return the vector in the response.
    """
    if retrieval.query.semantic is None:
        return False
    incomplete = retrieval.query.semantic.query is None or len(retrieval.query.semantic.query) == 0
    return incomplete
