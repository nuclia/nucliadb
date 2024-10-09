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
from dataclasses import dataclass
from typing import Union, cast

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.ids import ParagraphId, VectorId
from nucliadb.search import SERVICE_NAME, logger
from nucliadb.search.search.merge import merge_relations_results
from nucliadb.search.search.rerankers import (
    MultiMatchBoosterReranker,
    RerankableItem,
    Reranker,
    RerankingOptions,
)
from nucliadb.search.search.results_hydrator.base import (
    ResourceHydrationOptions,
    TextBlockHydrationOptions,
    hydrate_resource_metadata,
    hydrate_text_block,
    text_block_to_find_paragraph,
)
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName, Resource
from nucliadb_models.search import (
    SCORE_TYPE,
    FindField,
    FindParagraph,
    FindResource,
    KnowledgeboxFindResults,
    MinScore,
    ResourceProperties,
    TextPosition,
)
from nucliadb_protos.nodereader_pb2 import (
    DocumentScored,
    EntitiesSubgraphRequest,
    ParagraphResult,
    SearchResponse,
)
from nucliadb_telemetry import metrics

from . import paragraphs
from .metrics import merge_observer

FIND_FETCH_OPS_DISTRIBUTION = metrics.Histogram(
    "nucliadb_find_fetch_operations",
    buckets=[1, 5, 10, 20, 30, 40, 50, 60, 80, 100, 200],
)


def _round(x: float) -> float:
    return round(x, ndigits=3)


@dataclass
class SortableParagraph:
    rid: str
    fid: str
    pid: str
    score: float


@merge_observer.wrap({"type": "set_text_value"})
async def set_text_value(
    kbid: str,
    paragraph_id: ParagraphId,
    find_paragraph: FindParagraph,
    max_operations: asyncio.Semaphore,
    hydration_options: TextBlockHydrationOptions,
):
    async with max_operations:
        find_paragraph.text = await paragraphs.get_paragraph_text(
            kbid=kbid,
            paragraph_id=paragraph_id,
            highlight=hydration_options.highlight,
            ematches=hydration_options.ematches,
            matches=[],  # TODO
        )


@merge_observer.wrap({"type": "hydrate_and_rerank"})
async def hydrate_and_rerank(
    text_blocks: list[TextBlockMatch],
    kbid: str,
    *,
    resource_hydration_options: ResourceHydrationOptions,
    text_block_hydration_options: TextBlockHydrationOptions,
    reranker: Reranker,
    reranking_options: RerankingOptions,
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
    text_blocks_by_id = {}  # useful for faster access to text blocks later
    resource_hydration_ops = {}
    text_block_hydration_ops = {}
    for text_block in text_blocks:
        rid = text_block.paragraph_id.rid
        paragraph_id = text_block.paragraph_id.full()

        text_blocks_by_id[paragraph_id] = text_block

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

        if paragraph_id not in text_block_hydration_ops:
            text_block_hydration_ops[paragraph_id] = asyncio.create_task(
                hydrate_text_block(
                    kbid,
                    text_block,
                    text_block_hydration_options,
                    concurrency_control=max_operations,
                )
            )

    # hydrate only the strictly needed before rerank
    hydrated_text_blocks: list[TextBlockMatch]
    hydrated_resources: list[Union[Resource, None]]

    ops = [
        *text_block_hydration_ops.values(),
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

    matches = []
    for item in reranked:
        paragraph_id = item.id
        score = item.score
        score_type = item.score_type

        text_block = text_blocks_by_id[paragraph_id]
        text_block.score = score
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
        field_id = text_block.paragraph_id.field_id.full()
        find_field = find_resource.fields.setdefault(field_id, FindField(paragraphs={}))

        paragraph_id = text_block.paragraph_id.full()
        find_paragraph = text_block_to_find_paragraph(text_block)

        find_field.paragraphs[paragraph_id] = find_paragraph

    return find_resources


@merge_observer.wrap({"type": "merge_paragraphs_vectors"})
def merge_paragraphs_vectors(
    paragraphs_shards: list[list[ParagraphResult]],
    vectors_shards: list[list[DocumentScored]],
    count: int,
    page: int,
    min_score: float,
) -> tuple[list[TextBlockMatch], bool]:
    # Flatten the results from several shards to be able to sort them globally
    flat_paragraphs = [
        paragraph for paragraph_shard in paragraphs_shards for paragraph in paragraph_shard
    ]
    flat_vectors = [
        vector for vector_shard in vectors_shards for vector in vector_shard if vector.score >= min_score
    ]

    merged_paragraphs: list[TextBlockMatch] = rank_fusion_merge(flat_paragraphs, flat_vectors)

    init_position = count * page
    end_position = init_position + count
    next_page = len(merged_paragraphs) > end_position
    merged_paragraphs = merged_paragraphs[init_position:end_position]

    return merged_paragraphs, next_page


def rank_fusion_merge(
    paragraphs: list[ParagraphResult],
    vectors: list[DocumentScored],
) -> list[TextBlockMatch]:
    """Merge results from different indexes using a rank fusion algorithm.

    Given two list of sorted results from keyword and semantic search, this rank
    fusion algorithm mixes them in the following way:
    - 1st result from keyword search
    - 2nd result from semantic search
    - 2 keyword results and 1 semantic

    """
    merged_paragraphs: list[TextBlockMatch] = []

    # sort results by it's score before merging them
    paragraphs.sort(key=lambda r: r.score.bm25, reverse=True)
    vectors.sort(key=lambda r: r.score, reverse=True)

    for paragraph in paragraphs:
        fuzzy_result = len(paragraph.matches) > 0
        merged_paragraphs.append(
            TextBlockMatch(
                paragraph_id=ParagraphId.from_string(paragraph.paragraph),
                score=paragraph.score.bm25,
                score_type=SCORE_TYPE.BM25,
                order=0,  # NOTE: this will be filled later
                text="",  # NOTE: this will be filled later too
                position=TextPosition(
                    page_number=paragraph.metadata.position.page_number,
                    index=paragraph.metadata.position.index,
                    start=paragraph.start,
                    end=paragraph.end,
                    start_seconds=[x for x in paragraph.metadata.position.start_seconds],
                    end_seconds=[x for x in paragraph.metadata.position.end_seconds],
                ),
                paragraph_labels=[x for x in paragraph.labels],  # XXX could be list(paragraph.labels)?
                fuzzy_search=fuzzy_result,
                is_a_table=paragraph.metadata.representation.is_a_table,
                representation_file=paragraph.metadata.representation.file,
                page_with_visual=paragraph.metadata.page_with_visual,
            )
        )

    nextpos = 1
    for vector in vectors:
        try:
            vector_id = VectorId.from_string(vector.doc_id.id)
        except (IndexError, ValueError):
            logger.warning(f"Skipping invalid doc_id: {vector.doc_id.id}")
            continue
        merged_paragraphs.insert(
            nextpos,
            TextBlockMatch(
                paragraph_id=ParagraphId.from_vector_id(vector_id),
                score=vector.score,
                score_type=SCORE_TYPE.VECTOR,
                order=0,  # NOTE: this will be filled later
                text="",  # NOTE: this will be filled later too
                position=TextPosition(
                    page_number=vector.metadata.position.page_number,
                    index=vector.metadata.position.index,
                    start=vector_id.vector_start,
                    end=vector_id.vector_end,
                    start_seconds=[x for x in vector.metadata.position.start_seconds],
                    end_seconds=[x for x in vector.metadata.position.end_seconds],
                ),
                # TODO: get labels from index
                field_labels=[],
                paragraph_labels=[],
                fuzzy_search=False,  # semantic search doesn't have fuzziness
                is_a_table=vector.metadata.representation.is_a_table,
                representation_file=vector.metadata.representation.file,
                page_with_visual=vector.metadata.page_with_visual,
            ),
        )
        nextpos += 3

    return merged_paragraphs


@merge_observer.wrap({"type": "find_merge"})
async def find_merge_results(
    search_responses: list[SearchResponse],
    count: int,
    page: int,
    kbid: str,
    show: list[ResourceProperties],
    field_type_filter: list[FieldTypeName],
    extracted: list[ExtractedDataTypeName],
    requested_relations: EntitiesSubgraphRequest,
    min_score_bm25: float,
    min_score_semantic: float,
    highlight: bool = False,
) -> KnowledgeboxFindResults:
    paragraphs: list[list[ParagraphResult]] = []
    vectors: list[list[DocumentScored]] = []
    relations = []

    next_page = True
    ematches: list[str] = []
    real_query = ""
    total_paragraphs = 0
    for response in search_responses:
        # Iterate over answers from different logic shards
        ematches.extend(response.paragraph.ematches)
        real_query = response.paragraph.query
        next_page = next_page and response.paragraph.next_page
        total_paragraphs += response.paragraph.total

        paragraphs.append(cast(list[ParagraphResult], response.paragraph.results))
        vectors.append(cast(list[DocumentScored], response.vector.documents))

        relations.append(response.relation)

    result_paragraphs, merged_next_page = merge_paragraphs_vectors(
        paragraphs, vectors, count, page, min_score_semantic
    )
    next_page = next_page or merged_next_page

    api_results = KnowledgeboxFindResults(
        resources={},
        query=real_query,
        total=total_paragraphs,
        page_number=page,
        page_size=count,
        next_page=next_page,
        min_score=MinScore(bm25=_round(min_score_bm25), semantic=_round(min_score_semantic)),
        best_matches=[],
    )

    text_blocks, resources, best_matches = await hydrate_and_rerank(
        result_paragraphs,
        kbid,
        resource_hydration_options=ResourceHydrationOptions(
            show=show,
            extracted=extracted,
            field_type_filter=field_type_filter,
        ),
        text_block_hydration_options=TextBlockHydrationOptions(
            highlight=highlight,
            ematches=ematches,
        ),
        reranker=MultiMatchBoosterReranker(),
        reranking_options=RerankingOptions(
            kbid=kbid,
            query=real_query,
            top_k=count,
        ),
    )
    find_resources = compose_find_resources(text_blocks, resources)

    api_results.resources = find_resources
    api_results.best_matches = best_matches
    api_results.relations = await merge_relations_results(relations, requested_relations)

    return api_results
