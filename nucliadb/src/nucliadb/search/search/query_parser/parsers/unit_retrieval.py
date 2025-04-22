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
from nucliadb.search.search.query_parser.models import ParsedQuery, PredictReranker, UnitRetrieval
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
    request = SearchRequest()

    ## queries

    if parsed.retrieval.query.keyword and parsed.retrieval.query.fulltext:
        assert parsed.retrieval.query.keyword == parsed.retrieval.query.fulltext, (
            "search proto doesn't support different queries for fulltext and keyword search"
        )

    if parsed.retrieval.query.fulltext:
        request.document = True
        node_features.inc({"type": "documents"})
    if parsed.retrieval.query.keyword:
        request.paragraph = True
        node_features.inc({"type": "paragraphs"})

    text_query = parsed.retrieval.query.keyword or parsed.retrieval.query.fulltext
    if text_query is not None:
        request.min_score_bm25 = text_query.min_score

        if text_query.is_synonyms_query:
            request.advanced_query = text_query.query
        else:
            request.body = text_query.query

        # sort order
        sort_field = get_sort_field_proto(text_query.order_by)
        if sort_field is not None:
            request.order.sort_by = sort_field
            request.order.type = SortOrderMap[text_query.sort]  # type: ignore

    if parsed.retrieval.query.semantic:
        node_features.inc({"type": "vectors"})

        request.min_score_semantic = parsed.retrieval.query.semantic.min_score

        query_vector = parsed.retrieval.query.semantic.query
        if query_vector is not None:
            request.vectorset = parsed.retrieval.query.semantic.vectorset
            request.vector.extend(query_vector)

    if parsed.retrieval.query.relation:
        node_features.inc({"type": "relations"})

        request.relation_subgraph.entry_points.extend(parsed.retrieval.query.relation.detected_entities)
        request.relation_subgraph.depth = 1
        request.relation_subgraph.deleted_groups.extend(
            parsed.retrieval.query.relation.deleted_entity_groups
        )
        for group_id, deleted_entities in parsed.retrieval.query.relation.deleted_entities.items():
            request.relation_subgraph.deleted_entities.append(
                nodereader_pb2.EntitiesSubgraphRequest.DeletedEntities(
                    node_subtype=group_id, node_values=deleted_entities
                )
            )

    # filters

    request.with_duplicates = parsed.retrieval.filters.with_duplicates

    request.faceted.labels.extend([translate_label(facet) for facet in parsed.retrieval.filters.facets])

    if (
        parsed.retrieval.filters.security is not None
        and len(parsed.retrieval.filters.security.groups) > 0
    ):
        security_pb = utils_pb2.Security()
        for group_id in parsed.retrieval.filters.security.groups:
            if group_id not in security_pb.access_groups:
                security_pb.access_groups.append(group_id)
        request.security.CopyFrom(security_pb)

    if parsed.retrieval.filters.field_expression:
        request.field_filter.CopyFrom(parsed.retrieval.filters.field_expression)
    if parsed.retrieval.filters.paragraph_expression:
        request.paragraph_filter.CopyFrom(parsed.retrieval.filters.paragraph_expression)
    request.filter_operator = parsed.retrieval.filters.filter_expression_operator

    autofilter = []
    if parsed.retrieval.filters.autofilter:
        entity_filters = apply_entities_filter(request, parsed.retrieval.filters.autofilter)
        autofilter.extend([translate_system_to_alias_label(e) for e in entity_filters])

    if parsed.retrieval.filters.hidden is not None:
        expr = nodereader_pb2.FilterExpression()
        if parsed.retrieval.filters.hidden:
            expr.facet.facet = LABEL_HIDDEN
        else:
            expr.bool_not.facet.facet = LABEL_HIDDEN

        add_and_expression(request.field_filter, expr)

    # top_k

    # Adjust requested page size depending on rank fusion and reranking algorithms.
    #
    # Some rerankers want more results than the requested by the user so
    # reranking can have more choices.

    rank_fusion_window = 0
    if parsed.retrieval.rank_fusion is not None:
        rank_fusion_window = parsed.retrieval.rank_fusion.window

    reranker_window = 0
    if parsed.retrieval.reranker is not None and isinstance(parsed.retrieval.reranker, PredictReranker):
        reranker_window = parsed.retrieval.reranker.window

    request.result_per_page = max(
        request.result_per_page,
        rank_fusion_window,
        reranker_window,
    )

    # XXX: legacy values that were returned by QueryParser but not always
    # needed. We should find a better abstraction

    incomplete = is_incomplete(parsed.retrieval)

    rephrased_query = None
    if parsed.retrieval.query.semantic:
        rephrased_query = await parsed.fetcher.get_rephrased_query()

    return request, incomplete, autofilter, rephrased_query


def is_incomplete(retrieval: UnitRetrieval) -> bool:
    if retrieval.query.semantic is None:
        return False
    incomplete = retrieval.query.semantic.query is None or len(retrieval.query.semantic.query) == 0
    return incomplete
