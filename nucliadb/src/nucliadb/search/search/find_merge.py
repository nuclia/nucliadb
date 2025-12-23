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
from collections.abc import Iterable

from nidx_protos.nodereader_pb2 import GraphSearchResponse, SearchResponse

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.ids import ParagraphId
from nucliadb.models.internal.augment import AugmentedParagraph, Paragraph, ParagraphText
from nucliadb.search.augmentor.paragraphs import augment_paragraphs
from nucliadb.search.augmentor.resources import augment_resources_deep
from nucliadb.search.search.cut import cut_page
from nucliadb.search.search.hydrator import (
    ResourceHydrationOptions,
    TextBlockHydrationOptions,
)
from nucliadb.search.search.merge import merge_relations_results
from nucliadb.search.search.metrics import merge_observer
from nucliadb.search.search.paragraphs import highlight_paragraph
from nucliadb.search.search.query_parser.models import UnitRetrieval
from nucliadb.search.search.rerankers import RerankableItem, Reranker, RerankingOptions
from nucliadb_models.resource import Resource
from nucliadb_models.retrieval import RerankerScore
from nucliadb_models.search import (
    FindField,
    FindParagraph,
    FindResource,
    KnowledgeboxFindResults,
    MinScore,
)
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
    rephrased_query: str | None,
    reranker: Reranker,
    resource_hydration_options: ResourceHydrationOptions,
    text_block_hydration_options: TextBlockHydrationOptions,
) -> KnowledgeboxFindResults:
    # cut
    # we assume pagination + predict reranker is forbidden and has been already
    # enforced/validated by the query parsing.
    if reranker.needs_extra_results:
        assert reranker.window is not None, "Reranker definition must enforce this condition"
        text_blocks_page, next_page = cut_page(merged_text_blocks, reranker.window)
    else:
        text_blocks_page, next_page = cut_page(merged_text_blocks, retrieval.top_k)

    # hydrate and rerank
    reranking_options = RerankingOptions(
        kbid=kbid,
        # if we have a rephrased query, we assume it'll be better for the
        # reranker model. Otherwise, use the user query
        query=rephrased_query or query,
    )
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

    # Compute some misc values for the response

    # XXX: we shouldn't need a min score that we haven't used. Previous
    # implementations got this value from the proto request (i.e., default to 0)
    min_score_bm25 = 0.0
    if retrieval.query.keyword is not None:
        min_score_bm25 = retrieval.query.keyword.min_score
    min_score_semantic = 0.0
    if retrieval.query.semantic is not None:
        min_score_semantic = retrieval.query.semantic.min_score

    # Bw/c with pagination, next page can be obtained from different places. The
    # meaning is whether a greater top_k would have returned more results.
    # Although it doesn't take into account matches on the same paragraphs, an
    # estimate is good enough
    next_page = (
        # when rank fusion window is greater than top_k or the reranker window
        next_page
        # when the keyword index already has more results
        or search_response.paragraph.next_page
        # when rank fusion window is greater than top_k
        or len(merged_text_blocks) > retrieval.top_k
        # when the sum of all indexes makes more than top_k
        or (
            len(search_response.paragraph.results)
            + len(search_response.vector.documents)
            + len([True for path in graph_response.graph if path.metadata.paragraph_id])
            > retrieval.top_k
        )
    )
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

    # Iterate text blocks to create an "index" for faster access by id and get a
    # list of text block ids and resource ids to hydrate
    text_blocks_by_id: dict[str, TextBlockMatch] = {}  # useful for faster access to text blocks later
    resources_to_hydrate = set()
    text_block_id_to_hydrate = set()

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
            resources_to_hydrate.add(rid)

        if text_block_hydration_options.only_hydrate_empty and text_block.text:
            pass
        else:
            text_block_id_to_hydrate.add(paragraph_id)

    # hydrate only the strictly needed before rerank
    ops = [
        augment_paragraphs(
            kbid,
            given=[
                Paragraph.from_text_block_match(text_blocks_by_id[paragraph_id])
                for paragraph_id in text_block_id_to_hydrate
            ],
            select=[ParagraphText()],
            concurrency_control=max_operations,
        ),
        augment_resources_deep(
            kbid,
            given=list(resources_to_hydrate),
            opts=resource_hydration_options,
            concurrency_control=max_operations,
        ),
    ]
    FIND_FETCH_OPS_DISTRIBUTION.observe(len(text_block_id_to_hydrate) + len(resources_to_hydrate))
    results = await asyncio.gather(*ops)

    augmented_paragraphs: dict[ParagraphId, AugmentedParagraph | None] = results[0]  # type: ignore
    augmented_resources: dict[str, Resource | None] = results[1]  # type: ignore

    # add hydrated text to our text blocks
    for text_block in text_blocks:
        augmented = augmented_paragraphs.get(text_block.paragraph_id, None)
        if augmented is not None and augmented.text is not None:
            if text_block_hydration_options.highlight:
                text = highlight_paragraph(
                    augmented.text, words=[], ematches=text_block_hydration_options.ematches
                )
            else:
                text = augmented.text
            text_block.text = text

    # with the hydrated text, rerank and apply new scores to the text blocks
    to_rerank = [
        RerankableItem(
            id=text_block.paragraph_id.full(),
            score=text_block.score,
            score_type=text_block.score_type,
            content=text_block.text or "",  # TODO: add a warning, this shouldn't usually happen
        )
        for text_block in text_blocks
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
    resources_to_hydrate.clear()
    for order, (paragraph_id, _) in enumerate(matches):
        text_block = text_blocks_by_id[paragraph_id]
        text_block.order = order
        best_matches.append(paragraph_id)
        best_text_blocks.append(text_block)

        # now we have removed the text block surplus, fetch resource metadata
        if reranker.needs_extra_results:
            rid = ParagraphId.from_string(paragraph_id).rid
            resources_to_hydrate.add(rid)

    # Finally, fetch resource metadata if we haven't already done it
    if reranker.needs_extra_results:
        FIND_FETCH_OPS_DISTRIBUTION.observe(len(resources_to_hydrate))
        augmented_resources = await augment_resources_deep(
            kbid,
            given=list(resources_to_hydrate),
            opts=resource_hydration_options,
            concurrency_control=max_operations,
        )

    resources = [resource for resource in augmented_resources.values() if resource is not None]

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


def text_block_to_find_paragraph(text_block: TextBlockMatch) -> FindParagraph:
    return FindParagraph(
        id=text_block.paragraph_id.full(),
        text=text_block.text or "",
        score=text_block.score,
        score_type=text_block.score_type,
        order=text_block.order,
        labels=text_block.paragraph_labels,
        fuzzy_result=text_block.fuzzy_search,
        is_a_table=text_block.is_a_table,
        reference=text_block.representation_file,
        page_with_visual=text_block.page_with_visual,
        position=text_block.position,
        relevant_relations=text_block.relevant_relations,
    )


def _round(x: float) -> float:
    return round(x, ndigits=3)
