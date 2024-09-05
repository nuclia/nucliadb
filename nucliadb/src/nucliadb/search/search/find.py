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
from dataclasses import dataclass
from time import time
from typing import Optional

from nucliadb.common.external_index_providers.base import ExternalIndexManager
from nucliadb.common.external_index_providers.manager import (
    get_external_index_manager,
)
from nucliadb.search.requesters.utils import Method, debug_nodes_info, node_query
from nucliadb.search.search import results_hydrator
from nucliadb.search.search.find_merge import find_merge_results
from nucliadb.search.search.metrics import RAGMetrics
from nucliadb.search.search.query import QueryParser
from nucliadb.search.search.results_hydrator.base import (
    ResourceHydrationOptions,
)
from nucliadb.search.search.utils import (
    min_score_from_payload,
    should_disable_vector_search,
)
from nucliadb.search.settings import settings
from nucliadb_models.search import (
    FindRequest,
    KnowledgeboxFindResults,
    MinScore,
    NucliaDBClientType,
    SearchOptions,
)
from nucliadb_utils.utilities import get_audit

logger = logging.getLogger(__name__)


async def find(
    kbid: str,
    item: FindRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    generative_model: Optional[str] = None,
    metrics: RAGMetrics = RAGMetrics(),
) -> tuple[KnowledgeboxFindResults, bool, QueryParser]:
    external_index_manager = await get_external_index_manager(kbid=kbid)
    if external_index_manager is not None:
        return await _external_index_retrieval(
            kbid,
            item,
            external_index_manager,
            generative_model,
        )
    else:
        return await _index_node_retrieval(
            kbid,
            item,
            x_ndb_client,
            x_nucliadb_user,
            x_forwarded_for,
            generative_model,
            metrics,
        )


async def _index_node_retrieval(
    kbid: str,
    item: FindRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    generative_model: Optional[str] = None,
    metrics: RAGMetrics = RAGMetrics(),
) -> tuple[KnowledgeboxFindResults, bool, QueryParser]:
    audit = get_audit()
    start_time = time()

    item.min_score = min_score_from_payload(item.min_score)

    if SearchOptions.SEMANTIC in item.features:
        if should_disable_vector_search(item):
            item.features.remove(SearchOptions.SEMANTIC)

    query_parser = QueryParser(
        kbid=kbid,
        features=item.features,
        query=item.query,
        label_filters=item.filters,
        keyword_filters=item.keyword_filters,
        faceted=None,
        sort=None,
        page_number=item.page_number,
        page_size=item.page_size,
        min_score=item.min_score,
        range_creation_start=item.range_creation_start,
        range_creation_end=item.range_creation_end,
        range_modification_start=item.range_modification_start,
        range_modification_end=item.range_modification_end,
        fields=item.fields,
        user_vector=item.vector,
        vectorset=item.vectorset,
        with_duplicates=item.with_duplicates,
        with_synonyms=item.with_synonyms,
        autofilter=item.autofilter,
        key_filters=item.resource_filters,
        security=item.security,
        generative_model=generative_model,
        rephrase=item.rephrase,
    )
    with metrics.time("query_parse"):
        pb_query, incomplete_results, autofilters = await query_parser.parse()

    with metrics.time("node_query"):
        results, query_incomplete_results, queried_nodes = await node_query(
            kbid, Method.SEARCH, pb_query, target_shard_replicas=item.shards
        )
    incomplete_results = incomplete_results or query_incomplete_results

    # We need to merge
    with metrics.time("results_merge"):
        search_results = await find_merge_results(
            results,
            count=item.page_size,
            page=item.page_number,
            kbid=kbid,
            show=item.show,
            field_type_filter=item.field_type_filter,
            extracted=item.extracted,
            requested_relations=pb_query.relation_subgraph,
            min_score_bm25=query_parser.min_score.bm25,
            min_score_semantic=query_parser.min_score.semantic,  # type: ignore
            highlight=item.highlight,
        )

    search_time = time() - start_time
    if audit is not None:
        audit.search(
            kbid,
            x_nucliadb_user,
            x_ndb_client.to_proto(),
            x_forwarded_for,
            pb_query,
            search_time,
            len(search_results.resources),
        )

    if item.debug:
        search_results.nodes = debug_nodes_info(queried_nodes)

    queried_shards = [shard_id for _, shard_id in queried_nodes]
    search_results.shards = queried_shards
    search_results.autofilters = autofilters

    if metrics.elapsed("node_query") > settings.slow_node_query_log_threshold:
        logger.warning(
            "Slow node query",
            extra={
                "kbid": kbid,
                "user": x_nucliadb_user,
                "client": x_ndb_client,
                "query": item.model_dump_json(),
                "time": search_time,
                "nodes": debug_nodes_info(queried_nodes),
                "durations": metrics.steps(),
            },
        )
    elif search_time > settings.slow_find_log_threshold:
        logger.warning(
            "Slow find query",
            extra={
                "kbid": kbid,
                "user": x_nucliadb_user,
                "client": x_ndb_client,
                "query": item.model_dump_json(),
                "time": search_time,
                "nodes": debug_nodes_info(queried_nodes),
                "durations": metrics.steps(),
            },
        )

    return search_results, incomplete_results, query_parser


async def _external_index_retrieval(
    kbid: str,
    item: FindRequest,
    external_index_manager: ExternalIndexManager,
    generative_model: Optional[str] = None,
) -> tuple[KnowledgeboxFindResults, bool, QueryParser]:
    """
    Parse the query, query the external index, and hydrate the results.
    """
    # Parse query
    item.min_score = min_score_from_payload(item.min_score)
    query_parser = QueryParser(
        kbid=kbid,
        features=item.features,
        query=item.query,
        label_filters=item.filters,
        keyword_filters=item.keyword_filters,
        faceted=None,
        sort=None,
        page_number=item.page_number,
        page_size=item.page_size,
        min_score=item.min_score,
        range_creation_start=item.range_creation_start,
        range_creation_end=item.range_creation_end,
        range_modification_start=item.range_modification_start,
        range_modification_end=item.range_modification_end,
        fields=item.fields,
        user_vector=item.vector,
        vectorset=item.vectorset,
        with_duplicates=item.with_duplicates,
        with_synonyms=item.with_synonyms,
        autofilter=item.autofilter,
        key_filters=item.resource_filters,
        security=item.security,
        generative_model=generative_model,
        rephrase=item.rephrase,
    )
    search_request, incomplete_results, _ = await query_parser.parse()

    # Query index
    query_results = await external_index_manager.query(search_request)  # noqa

    # Hydrate results
    results_min_score = MinScore(
        bm25=0,
        semantic=item.min_score.semantic or query_parser.min_score.semantic,
    )
    retrieval_results = KnowledgeboxFindResults(
        resources={},
        query=item.query,
        total=0,
        page_number=0,
        page_size=(item.page_number + 1) * item.page_size,
        relations=None,  # Not implemented for external indexes yet
        autofilters=[],  # Not implemented for external indexes yet
        min_score=results_min_score,
        best_matches=[],
        # These are not used for external indexes
        shards=None,
        nodes=None,
    )
    await results_hydrator.hydrate_external(
        retrieval_results,
        query_results,
        kbid=kbid,
        resource_options=ResourceHydrationOptions(
            show=item.show,
            extracted=item.extracted,
            field_type_filter=item.field_type_filter,
        ),
        text_block_min_score=results_min_score.semantic,
        max_parallel_operations=50,
    )

    # Once hydrated, populate best_matches with the paragraphs ids sorted by score
    scored_paragraphs: list[ScoredParagraph] = [
        ScoredParagraph(id=paragraph.id, score=paragraph.score)
        for resource in retrieval_results.resources.values()
        for field in resource.fields.values()
        for paragraph in field.paragraphs.values()
    ]
    scored_paragraphs.sort(key=lambda par: par.score, reverse=True)
    retrieval_results.best_matches = [par.id for par in scored_paragraphs]

    return retrieval_results, incomplete_results, query_parser


@dataclass
class ScoredParagraph:
    id: str
    score: float
