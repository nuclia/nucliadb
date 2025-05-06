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
import asyncio
import logging
from time import time
from typing import Optional

from nidx_protos.nodereader_pb2 import SearchRequest

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.external_index_providers.manager import get_external_index_manager
from nucliadb.common.external_index_providers.pinecone import PineconeIndexManager
from nucliadb.common.models_utils import to_proto
from nucliadb.search.search.find_merge import (
    _round,
    compose_find_resources,
)
from nucliadb.search.search.hydrator import ResourceHydrationOptions, TextBlockHydrationOptions
from nucliadb.search.search.metrics import (
    RAGMetrics,
)
from nucliadb.search.search.plan.cut import Cut
from nucliadb.search.search.plan.hydrate import HydrateResources, HydrateTextBlocks
from nucliadb.search.search.plan.models import IndexResult, Plan, PlanStep
from nucliadb.search.search.plan.query import (
    IndexPostFilter,
    NidxQuery,
    PineconeQuery,
    ProtoIntoTextBlockMatches,
)
from nucliadb.search.search.plan.rank_fusion import RankFusion
from nucliadb.search.search.plan.rerank import Rerank
from nucliadb.search.search.plan.serialize import SerializeRelations
from nucliadb.search.search.plan.utils import CachedStep, Flatten, Group, OptionalStep, Parallel
from nucliadb.search.search.query_parser.models import ParsedQuery, UnitRetrieval
from nucliadb.search.search.query_parser.parsers import parse_find
from nucliadb.search.search.query_parser.parsers.unit_retrieval import legacy_convert_retrieval_to_proto
from nucliadb.search.search.rank_fusion import get_rank_fusion
from nucliadb.search.search.rerankers import Reranker, RerankingOptions, get_reranker
from nucliadb.search.settings import settings
from nucliadb_models.resource import Resource
from nucliadb_models.search import (
    FindRequest,
    KnowledgeboxFindResults,
    MinScore,
    NucliaDBClientType,
    Relations,
)
from nucliadb_utils.utilities import get_audit

logger = logging.getLogger(__name__)


async def find(
    kbid: str,
    item: FindRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    metrics: RAGMetrics = RAGMetrics(),
) -> tuple[KnowledgeboxFindResults, bool, ParsedQuery]:
    audit = get_audit()
    start_time = time()

    with metrics.time("query_parse"):
        parsed = await parse_find(kbid, item)
        (
            pb_query,
            incomplete_results,
            autofilters,
            rephrased_query,
        ) = await legacy_convert_retrieval_to_proto(parsed)

    with metrics.time("query_plan"):
        plan = await plan_find_request(kbid, item, parsed, pb_query)

    with metrics.time("node_query"), metrics.time("query_execute"):
        text_blocks, resources, relations = await plan.execute()

    with metrics.time("results_merge"):
        # compose response
        find_resources = compose_find_resources(text_blocks, resources)

        best_matches = [text_block.paragraph_id.full() for text_block in text_blocks]

        # XXX: we shouldn't need a min score that we haven't used. Previous
        # implementations got this value from the proto request (i.e., default to 0)
        min_score_bm25 = 0.0
        if parsed.retrieval.query.keyword is not None:
            min_score_bm25 = parsed.retrieval.query.keyword.min_score
        min_score_semantic = 0.0
        if parsed.retrieval.query.semantic is not None:
            min_score_semantic = parsed.retrieval.query.semantic.min_score

        find_results = KnowledgeboxFindResults(
            query=pb_query.body,
            rephrased_query=rephrased_query,
            resources=find_resources,
            best_matches=best_matches,
            relations=relations,
            total=plan.context.keyword_result_count,
            page_number=0,  # Bw/c with pagination
            page_size=parsed.retrieval.top_k,
            next_page=plan.context.next_page,
            min_score=MinScore(bm25=_round(min_score_bm25), semantic=_round(min_score_semantic)),
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
            len(find_results.resources),
            retrieval_rephrased_question=rephrased_query,
        )

    find_results.shards = plan.context.nidx_queried_shards
    find_results.autofilters = autofilters

    ndb_time = metrics.elapsed("node_query") + metrics.elapsed("results_merge")
    if metrics.elapsed("node_query") > settings.slow_node_query_log_threshold:
        logger.warning(
            "Slow nidx query",
            extra={
                "kbid": kbid,
                "user": x_nucliadb_user,
                "client": x_ndb_client,
                "query": item.model_dump_json(),
                "time": search_time,
                "durations": metrics.steps(),
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
                "durations": metrics.steps(),
            },
        )

    incomplete = incomplete_results or plan.context.nidx_incomplete
    return find_results, incomplete, parsed


async def plan_find_request(
    kbid: str, request: FindRequest, parsed: ParsedQuery, pb_query: SearchRequest
) -> Plan[tuple[list[TextBlockMatch], list[Resource], Optional[Relations]]]:
    query_step, serialize_relations_step = await _plan_index_query(kbid, parsed, pb_query)
    rank_fusion_step = _plan_rank_fusion(parsed.retrieval, uses=query_step)
    hydrate_and_rerank_step = _plan_hydrate_and_rerank(
        kbid, request, parsed, pb_query, uses=rank_fusion_step
    )

    plan = Plan(
        Flatten(
            Parallel(
                hydrate_and_rerank_step,
                serialize_relations_step,
            )
        )
    )
    return plan


async def _plan_index_query(
    kbid: str,
    parsed: ParsedQuery,
    pb_query: SearchRequest,
) -> tuple[PlanStep[IndexResult], OptionalStep[Relations]]:
    """An index query is the first step in the plan. Depending on the KB
    configuration, this can be done using our internal index (nidx) or an
    external index (currently, only pinecone is supported).

    """
    query_index: PlanStep[IndexResult]
    serialize_relations: OptionalStep[Relations]

    external_index_manager = await get_external_index_manager(kbid=kbid)
    if external_index_manager is not None:
        assert isinstance(external_index_manager, PineconeIndexManager), (
            "every query index has it's own quirks and we only support Pinecone"
        )
        query_index = PineconeQuery(kbid, pb_query, external_index_manager)
        serialize_relations = OptionalStep(None)

    else:
        nidx_query = CachedStep(NidxQuery(kbid, pb_query))
        query_index = ProtoIntoTextBlockMatches(uses=nidx_query)
        serialize_relations = OptionalStep(SerializeRelations(parsed.retrieval, uses=nidx_query))

    # XXX: Previous implementation got this value from the proto request (i.e., default to 0)
    semantic_min_score = 0.0
    if parsed.retrieval.query.semantic:
        semantic_min_score = parsed.retrieval.query.semantic.min_score

    # REVIEW: This shouldn't be needed if the index filters correctly
    query_index = IndexPostFilter(query_index, semantic_min_score)

    return query_index, serialize_relations


def _plan_rank_fusion(
    retrieval: UnitRetrieval, *, uses: PlanStep[IndexResult]
) -> PlanStep[list[TextBlockMatch]]:
    """Rank fusion is applied to an index result. It merges results from
    different indexes into a single list of matches.

    """
    assert retrieval.rank_fusion is not None, "find parser must provide a rank fusion algorithm"
    rank_fusion_algorithm = get_rank_fusion(retrieval.rank_fusion)
    return RankFusion(algorithm=rank_fusion_algorithm, source=uses)


def _plan_hydrate_and_rerank(
    kbid: str,
    request: FindRequest,
    parsed: ParsedQuery,
    pb_query: SearchRequest,
    *,
    uses: PlanStep[list[TextBlockMatch]],
) -> PlanStep[tuple[list[TextBlockMatch], list[Resource]]]:
    """Given a list of matches, a reranker is used to improve the score accuracy
    of the results.

    Reranking depends on matches to be hydrated, so a text block hydration step
    is done before reranking.

    For performance, depending on the reranking parameters, resource hydration
    is done in parallel.

    """

    retrieval_step = uses

    assert parsed.retrieval.reranker is not None, "find parser must provide a reranking algorithm"
    reranker = get_reranker(parsed.retrieval.reranker)

    pre_rerank_cut, rerank, post_rerank_cut = _plan_rerank(kbid, parsed, reranker, pb_query)
    hydrate_text_blocks, hydrate_resources = _plan_hydration(kbid, request)

    # Reranking

    hydrate_and_rerank_step: PlanStep[tuple[list[TextBlockMatch], list[Resource]]]

    if post_rerank_cut is not None:
        # To avoid hydrating unneeded resources, we first rerank, cut and then
        # hydrate them

        reranked = CachedStep(
            post_rerank_cut.plan(
                rerank.plan(
                    hydrate_text_blocks.plan(
                        pre_rerank_cut.plan(
                            retrieval_step,
                        ),
                    ),
                ),
            )
        )
        hydrate_and_rerank_step = Group(
            reranked,
            hydrate_resources.plan(uses=reranked),
        )
    else:
        # As we don't need extra results, we can rerank and hydrate resources in
        # parallel

        pre_rerank = CachedStep(
            pre_rerank_cut.plan(
                retrieval_step,
            ),
        )

        hydrate_and_rerank_step = Parallel(
            rerank.plan(
                hydrate_text_blocks.plan(
                    pre_rerank,
                ),
            ),
            hydrate_resources.plan(
                pre_rerank,
            ),
        )

    return hydrate_and_rerank_step


def _plan_hydration(kbid: str, request: FindRequest) -> tuple[HydrateTextBlocks, HydrateResources]:
    max_hydration_ops = asyncio.Semaphore(50)

    text_block_hydration_options = TextBlockHydrationOptions(
        highlight=request.highlight,
        # TODO: ematches for text block hydration options
        # ematches=search_response.paragraph.ematches,  # type: ignore
        ematches=[],
    )
    hydrate_text_blocks = HydrateTextBlocks(kbid, text_block_hydration_options, max_hydration_ops)

    resource_hydration_options = ResourceHydrationOptions(
        show=request.show, extracted=request.extracted, field_type_filter=request.field_type_filter
    )
    hydrate_resources = HydrateResources(kbid, resource_hydration_options, max_hydration_ops)

    return hydrate_text_blocks, hydrate_resources


def _plan_rerank(
    kbid: str, parsed: ParsedQuery, reranker: Reranker, pb_query: SearchRequest
) -> tuple[Cut, Rerank, Optional[Cut]]:
    """Reranking is done within two windows. We want to rerank N elements and
    obtain the best K. This function returns the steps for reranking and cutting
    before and after.

    """
    # cut before reranking

    if reranker.needs_extra_results:
        # we assume pagination + predict reranker is forbidden and has been already
        # enforced/validated by the query parsing.
        assert reranker.window is not None, "Reranker definition must enforce this condition"
        pre_rerank_cut = Cut(page=reranker.window)
        post_rerank_cut = Cut(page=parsed.retrieval.top_k)
    else:
        pre_rerank_cut = Cut(page=parsed.retrieval.top_k)
        post_rerank_cut = None

    # reranking

    reranking_options = RerankingOptions(
        kbid=kbid,
        # XXX: we are using keyword query for reranking, it doesn't work with semantic or graph only!
        query=pb_query.body,
    )
    rerank = Rerank(kbid, reranker, reranking_options, parsed.retrieval.top_k)

    return pre_rerank_cut, rerank, post_rerank_cut
