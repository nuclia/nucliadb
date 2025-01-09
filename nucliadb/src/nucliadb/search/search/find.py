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
from nucliadb.common.external_index_providers.manager import get_external_index_manager
from nucliadb.common.models_utils import to_proto
from nucliadb.search.requesters.utils import Method, debug_nodes_info, node_query
from nucliadb.search.search.find_merge import (
    build_find_response,
    compose_find_resources,
    hydrate_and_rerank,
)
from nucliadb.search.search.hydrator import (
    ResourceHydrationOptions,
    TextBlockHydrationOptions,
)
from nucliadb.search.search.metrics import (
    RAGMetrics,
)
from nucliadb.search.search.query import QueryParser
from nucliadb.search.search.query_parser.parser import parse_find
from nucliadb.search.search.rank_fusion import (
    RankFusionAlgorithm,
    get_rank_fusion,
)
from nucliadb.search.search.rerankers import (
    Reranker,
    RerankingOptions,
    get_reranker,
)
from nucliadb.search.search.utils import (
    filter_hidden_resources,
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
            kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for, generative_model, metrics
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

    query_parser, rank_fusion, reranker = await query_parser_from_find_request(
        kbid, item, generative_model=generative_model
    )
    with metrics.time("query_parse"):
        pb_query, incomplete_results, autofilters = await query_parser.parse()

    with metrics.time("node_query"):
        results, query_incomplete_results, queried_nodes = await node_query(
            kbid, Method.SEARCH, pb_query, target_shard_replicas=item.shards
        )
    incomplete_results = incomplete_results or query_incomplete_results

    # Rank fusion merge, cut, hydrate and rerank
    with metrics.time("results_merge"):
        search_results = await build_find_response(
            results,
            kbid=kbid,
            query=pb_query.body,
            relation_subgraph_query=pb_query.relations.subgraph,
            min_score_bm25=pb_query.min_score_bm25,
            min_score_semantic=pb_query.min_score_semantic,
            top_k=item.top_k,
            show=item.show,
            extracted=item.extracted,
            field_type_filter=item.field_type_filter,
            highlight=item.highlight,
            rank_fusion_algorithm=rank_fusion,
            reranker=reranker,
        )

    search_time = time() - start_time
    if audit is not None:
        audit.search(
            kbid,
            x_nucliadb_user,
            to_proto.client_type(x_ndb_client),
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
        logger.info(
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
    query_parser, _, reranker = await query_parser_from_find_request(
        kbid, item, generative_model=generative_model
    )
    search_request, incomplete_results, _ = await query_parser.parse()

    # Query index
    query_results = await external_index_manager.query(search_request)  # noqa

    # Hydrate and rerank results
    text_blocks, resources, best_matches = await hydrate_and_rerank(
        query_results.iter_matching_text_blocks(),
        kbid,
        resource_hydration_options=ResourceHydrationOptions(
            show=item.show,
            extracted=item.extracted,
            field_type_filter=item.field_type_filter,
        ),
        text_block_hydration_options=TextBlockHydrationOptions(),
        reranker=reranker,
        reranking_options=RerankingOptions(
            kbid=kbid,
            query=search_request.body,
        ),
        top_k=query_parser.top_k,
    )
    find_resources = compose_find_resources(text_blocks, resources)

    results_min_score = MinScore(
        bm25=0,
        semantic=query_parser.min_score.semantic,
    )
    retrieval_results = KnowledgeboxFindResults(
        resources=find_resources,
        query=item.query,
        total=0,
        page_number=0,
        page_size=item.top_k,
        relations=None,  # Not implemented for external indexes yet
        autofilters=[],  # Not implemented for external indexes yet
        min_score=results_min_score,
        best_matches=best_matches,
        # These are not used for external indexes
        shards=None,
        nodes=None,
    )

    return retrieval_results, incomplete_results, query_parser


@dataclass
class ScoredParagraph:
    id: str
    score: float


async def query_parser_from_find_request(
    kbid: str, item: FindRequest, *, generative_model: Optional[str] = None
) -> tuple[QueryParser, RankFusionAlgorithm, Reranker]:
    item.min_score = min_score_from_payload(item.min_score)

    if SearchOptions.SEMANTIC in item.features:
        if should_disable_vector_search(item):
            item.features.remove(SearchOptions.SEMANTIC)

    hidden = await filter_hidden_resources(kbid, item.show_hidden)

    # XXX this is becoming the new /find query parsing, this should be moved to
    # a cleaner abstraction

    parsed = parse_find(item)

    rank_fusion = get_rank_fusion(parsed.rank_fusion)
    reranker = get_reranker(parsed.reranker)

    query_parser = QueryParser(
        kbid=kbid,
        features=item.features,
        query=item.query,
        label_filters=item.filters,
        keyword_filters=item.keyword_filters,
        faceted=None,
        sort=None,
        top_k=item.top_k,
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
        rephrase_prompt=item.rephrase_prompt,
        hidden=hidden,
        rank_fusion=rank_fusion,
        reranker=reranker,
    )
    return (query_parser, rank_fusion, reranker)
