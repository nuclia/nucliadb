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
from typing import Iterable, Optional, Union

from nidx_protos.nodereader_pb2 import GraphSearchResponse, SearchResponse

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.ids import ParagraphId
from nucliadb.search import SERVICE_NAME
from nucliadb.search.search.cut import cut_page
from nucliadb.search.search.hydrator import (
    ResourceHydrationOptions,
    TextBlockHydrationOptions,
    hydrate_resource_metadata,
    hydrate_text_block,
    text_block_to_find_paragraph,
)
from nucliadb.search.search.merge import merge_relations_results
from nucliadb.search.search.metrics import merge_observer
from nucliadb.search.search.query_parser.models import UnitRetrieval
from nucliadb.search.search.rerankers import RerankableItem, Reranker, RerankingOptions
from nucliadb_models.internal.retrieval import RerankerScore
from nucliadb_models.resource import Resource
from nucliadb_models.search import FindField, FindResource, KnowledgeboxFindResults, MinScore
from nucliadb_telemetry import metrics

FIND_FETCH_OPS_DISTRIBUTION = metrics.Histogram(
    "nucliadb_find_fetch_operations",
    buckets=[1, 5, 10, 20, 30, 40, 50, 60, 80, 100, 200],
)


@merge_observer.wrap({"type": "find_merge"})
async def build_find_response(
    search_response: SearchResponse,
    merged_text_blocks: list[TextBlockMatch],
    graph_response: GraphSearchResponse,
    *,
    retrieval: UnitRetrieval,
    kbid: str,
    query: str,
    rephrased_query: Optional[str],
    reranker: Reranker,
    resource_hydration_options: ResourceHydrationOptions,
    text_block_hydration_options: TextBlockHydrationOptions,
) -> KnowledgeboxFindResults:
    # XXX: we shouldn't need a min score that we haven't used. Previous
    # implementations got this value from the proto request (i.e., default to 0)
    min_score_bm25 = 0.0
    if retrieval.query.keyword is not None:
        min_score_bm25 = retrieval.query.keyword.min_score
    min_score_semantic = 0.0
    if retrieval.query.semantic is not None:
        min_score_semantic = retrieval.query.semantic.min_score

    # cut
    # we assume pagination + predict reranker is forbidden and has been already
    # enforced/validated by the query parsing.
    if reranker.needs_extra_results:
        assert reranker.window is not None, "Reranker definition must enforce this condition"
        text_blocks_page, next_page = cut_page(merged_text_blocks, reranker.window)
    else:
        text_blocks_page, next_page = cut_page(merged_text_blocks, retrieval.top_k)

    # hydrate and rerank
    reranking_options = RerankingOptions(kbid=kbid, query=query)
    text_blocks, resources, best_matches = await hydrate_and_rerank(
        text_blocks_page,
        kbid,
        resource_hydration_options=resource_hydration_options,
        text_block_hydration_options=text_block_hydration_options,
        reranker=reranker,
        reranking_options=reranking_options,
        top_k=retrieval.top_k,
    )

    # build relations graph
    entry_points = []
    if retrieval.query.relation is not None:
        entry_points = retrieval.query.relation.entry_points
    relations = await merge_relations_results([graph_response], entry_points)

    # compose response
    find_resources = compose_find_resources(text_blocks, resources)

    next_page = search_response.paragraph.next_page or next_page
    total_paragraphs = search_response.paragraph.total

    find_results = KnowledgeboxFindResults(
        query=query,
        rephrased_query=rephrased_query,
        resources=find_resources,
        best_matches=best_matches,
        relations=relations,
        total=total_paragraphs,
        page_number=0,  # Bw/c with pagination
        page_size=retrieval.top_k,
        next_page=next_page,
        min_score=MinScore(bm25=_round(min_score_bm25), semantic=_round(min_score_semantic)),
    )
    return find_results


@merge_observer.wrap({"type": "hydrate_and_rerank"})
async def hydrate_and_rerank(
    text_blocks: Iterable[TextBlockMatch],
    kbid: str,
    *,
    resource_hydration_options: ResourceHydrationOptions,
    text_block_hydration_options: TextBlockHydrationOptions,
    reranker: Reranker,
    reranking_options: RerankingOptions,
    top_k: int,
) -> tuple[list[TextBlockMatch], list[Resource], list[str]]:
    """Given a list of text blocks from a retrieval operation, hydrate and
    rerank the results.

    This function returns either the entire list or a subset of updated
    (hydrated and reranked) text blocks and their corresponding resource
    metadata. It also returns an ordered list of best matches.

    """
    max_operations = asyncio.Semaphore(50)

    # Iterate text blocks and create text block and resource metadata hydration
    # tasks depending on the reranker
    text_blocks_by_id: dict[str, TextBlockMatch] = {}  # useful for faster access to text blocks later
    resource_hydration_ops = {}
    text_block_hydration_ops = []
    for text_block in text_blocks:
        rid = text_block.paragraph_id.rid
        paragraph_id = text_block.paragraph_id.full()

        # If we find multiple results (from different indexes) with different
        # metadata, this statement will only get the metadata from the first on
        # the list. We assume metadata is the same on all indexes, otherwise
        # this would be a BUG
        text_blocks_by_id.setdefault(paragraph_id, text_block)

        # rerankers that need extra results may end with less resources than the
        # ones we see now, so we'll skip this step and recompute the resources
        # later
        if not reranker.needs_extra_results:
            if rid not in resource_hydration_ops:
                resource_hydration_ops[rid] = asyncio.create_task(
                    hydrate_resource_metadata(
                        kbid,
                        rid,
                        options=resource_hydration_options,
                        concurrency_control=max_operations,
                        service_name=SERVICE_NAME,
                    )
                )

        text_block_hydration_ops.append(
            asyncio.create_task(
                hydrate_text_block(
                    kbid,
                    text_block,
                    text_block_hydration_options,
                    concurrency_control=max_operations,
                )
            )
        )

    # hydrate only the strictly needed before rerank
    hydrated_text_blocks: list[TextBlockMatch]
    hydrated_resources: list[Union[Resource, None]]

    ops = [
        *text_block_hydration_ops,
        *resource_hydration_ops.values(),
    ]
    FIND_FETCH_OPS_DISTRIBUTION.observe(len(ops))
    results = await asyncio.gather(*ops)

    hydrated_text_blocks = results[: len(text_block_hydration_ops)]  # type: ignore
    hydrated_resources = results[len(text_block_hydration_ops) :]  # type: ignore

    # with the hydrated text, rerank and apply new scores to the text blocks
    to_rerank = [
        RerankableItem(
            id=text_block.paragraph_id.full(),
            score=text_block.score,
            score_type=text_block.score_type,
            content=text_block.text or "",  # TODO: add a warning, this shouldn't usually happen
        )
        for text_block in hydrated_text_blocks
    ]
    reranked = await reranker.rerank(to_rerank, reranking_options)

    # after reranking, we can cut to the number of results the user wants, so we
    # don't hydrate unnecessary stuff
    reranked = reranked[:top_k]

    matches = []
    for item in reranked:
        paragraph_id = item.id
        score = item.score
        score_type = item.score_type

        text_block = text_blocks_by_id[paragraph_id]
        text_block.scores.append(RerankerScore(score=score))
        text_block.score_type = score_type

        matches.append((paragraph_id, score))

    matches.sort(key=lambda x: x[1], reverse=True)

    best_matches = []
    best_text_blocks = []
    resource_hydration_ops = {}
    for order, (paragraph_id, _) in enumerate(matches):
        text_block = text_blocks_by_id[paragraph_id]
        text_block.order = order
        best_matches.append(paragraph_id)
        best_text_blocks.append(text_block)

        # now we have removed the text block surplus, fetch resource metadata
        if reranker.needs_extra_results:
            rid = ParagraphId.from_string(paragraph_id).rid
            if rid not in resource_hydration_ops:
                resource_hydration_ops[rid] = asyncio.create_task(
                    hydrate_resource_metadata(
                        kbid,
                        rid,
                        options=resource_hydration_options,
                        concurrency_control=max_operations,
                        service_name=SERVICE_NAME,
                    )
                )

    # Finally, fetch resource metadata if we haven't already done it
    if reranker.needs_extra_results:
        ops = list(resource_hydration_ops.values())
        FIND_FETCH_OPS_DISTRIBUTION.observe(len(ops))
        hydrated_resources = await asyncio.gather(*ops)  # type: ignore

    resources = [resource for resource in hydrated_resources if resource is not None]

    return best_text_blocks, resources, best_matches


def compose_find_resources(
    text_blocks: list[TextBlockMatch],
    resources: list[Resource],
) -> dict[str, FindResource]:
    find_resources: dict[str, FindResource] = {}

    for resource in resources:
        rid = resource.id
        if rid not in find_resources:
            find_resources[rid] = FindResource(id=rid, fields={})
            find_resources[rid].updated_from(resource)

    for text_block in text_blocks:
        rid = text_block.paragraph_id.rid
        if rid not in find_resources:
            # resource not found in db, skipping
            continue

        find_resource = find_resources[rid]
        field_id = text_block.paragraph_id.field_id.short_without_subfield()
        find_field = find_resource.fields.setdefault(field_id, FindField(paragraphs={}))

        paragraph_id = text_block.paragraph_id.full()
        find_paragraph = text_block_to_find_paragraph(text_block)

        find_field.paragraphs[paragraph_id] = find_paragraph

    return find_resources


def _round(x: float) -> float:
    return round(x, ndigits=3)
