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
from time import time

from nucliadb.common.external_index_providers.base import ExternalIndexManager
from nucliadb.common.external_index_providers.manager import get_external_index_manager
from nucliadb.common.models_utils import to_proto
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
    Metrics,
)
from nucliadb.search.search.query_parser.models import ParsedQuery
from nucliadb.search.search.query_parser.parsers import parse_find
from nucliadb.search.search.query_parser.parsers.unit_retrieval import (
    convert_retrieval_to_proto,
    get_rephrased_query,
    is_incomplete,
)
from nucliadb.search.search.rerankers import (
    RerankingOptions,
    get_reranker,
)
from nucliadb.search.search.retrieval import text_block_search
from nucliadb.search.settings import settings
from nucliadb_models.search import (
    FindRequest,
    KnowledgeboxFindResults,
    MinScore,
    NucliaDBClientType,
)
from nucliadb_utils.utilities import get_audit

logger = logging.getLogger(__name__)


async def find(
    kbid: str,
    item: FindRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    metrics: Metrics,
) -> tuple[KnowledgeboxFindResults, bool, ParsedQuery]:
    external_index_manager = await get_external_index_manager(kbid=kbid)
    if external_index_manager is not None:
        return await _external_index_find(
            kbid,
            item,
            external_index_manager,
        )
    else:
        return await _ndb_index_find(kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for, metrics)


async def _ndb_index_find(
    kbid: str,
    item: FindRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    metrics: Metrics,
) -> tuple[KnowledgeboxFindResults, bool, ParsedQuery]:
    audit = get_audit()
    start_time = time()

    with metrics.time("query_parse"):
        parsed = await parse_find(kbid, item)
        assert parsed.retrieval.rank_fusion is not None and parsed.retrieval.reranker is not None, (
            "find parser must provide rank fusion and reranker algorithms"
        )
        reranker = get_reranker(parsed.retrieval.reranker)
        incomplete_results = is_incomplete(parsed.retrieval)
        rephrased_query = get_rephrased_query(parsed)

    with metrics.time("index_search"):
        text_blocks, pb_query, pb_response, queried_shards = await text_block_search(
            kbid, parsed.retrieval
        )

    # Rank fusion merge, cut, hydrate and rerank
    with metrics.time("results_merge"):
        resource_hydration_options = ResourceHydrationOptions(
            show=item.show,
            extracted=item.extracted,
            field_type_filter=item.field_type_filter,
        )
        text_block_hydration_options = TextBlockHydrationOptions(
            highlight=item.highlight,
            ematches=pb_response.paragraph.ematches,  # type: ignore
        )
        search_results = await build_find_response(
            pb_response,
            text_blocks,
            pb_response.graph,
            retrieval=parsed.retrieval,
            kbid=kbid,
            query=item.query,
            rephrased_query=rephrased_query,
            reranker=reranker,
            resource_hydration_options=resource_hydration_options,
            text_block_hydration_options=text_block_hydration_options,
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
            retrieval_rephrased_question=rephrased_query,
        )

    search_results.shards = queried_shards

    ndb_time = metrics["index_search"] + metrics["results_merge"]
    if metrics["index_search"] > settings.slow_node_query_log_threshold:
        logger.warning(
            "Slow nidx query",
            extra={
                "kbid": kbid,
                "user": x_nucliadb_user,
                "client": x_ndb_client,
                "query": item.model_dump_json(),
                "time": search_time,
                "metrics": metrics.to_dict(),
            },
        )
    elif ndb_time > settings.slow_find_log_threshold:
        logger.info(
            "Slow find query",
            extra={
                "kbid": kbid,
                "user": x_nucliadb_user,
                "client": x_ndb_client,
                "query": item.model_dump_json(),
                "time": search_time,
                "metrics": metrics.to_dict(),
            },
        )

    return search_results, incomplete_results, parsed


async def _external_index_find(
    kbid: str,
    item: FindRequest,
    external_index_manager: ExternalIndexManager,
) -> tuple[KnowledgeboxFindResults, bool, ParsedQuery]:
    """
    Parse the query, query the external index, and hydrate the results.
    """
    # Parse query
    parsed = await parse_find(kbid, item)
    assert parsed.retrieval.reranker is not None, "find parser must provide a reranking algorithm"
    reranker = get_reranker(parsed.retrieval.reranker)
    incomplete_results = is_incomplete(parsed.retrieval)
    rephrased_query = get_rephrased_query(parsed)
    search_request = convert_retrieval_to_proto(parsed.retrieval)

    # Query index
    query_results = await external_index_manager.query(search_request)

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
        top_k=parsed.retrieval.top_k,
    )
    find_resources = compose_find_resources(text_blocks, resources)

    results_min_score = MinScore(
        bm25=0,
        semantic=parsed.retrieval.query.semantic.min_score
        if parsed.retrieval.query.semantic is not None
        else 0.0,
    )
    retrieval_results = KnowledgeboxFindResults(
        resources=find_resources,
        query=item.query,
        rephrased_query=rephrased_query,
        total=0,
        page_number=0,
        page_size=item.top_k,
        relations=None,  # Not implemented for external indexes yet
        min_score=results_min_score,
        best_matches=best_matches,
        # These are not used for external indexes
        shards=None,
        nodes=None,
    )

    return retrieval_results, incomplete_results, parsed
