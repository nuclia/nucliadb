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

from nidx_protos.nodereader_pb2 import (
    DocumentScored,
    GraphSearchResponse,
    ParagraphResult,
    ParagraphSearchResponse,
    SearchResponse,
    VectorSearchResponse,
)

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.ids import ParagraphId, VectorId
from nucliadb.search import SERVICE_NAME, logger
from nucliadb.search.search.cut import cut_page
from nucliadb.search.search.hydrator import (
    ResourceHydrationOptions,
    TextBlockHydrationOptions,
    hydrate_resource_metadata,
    hydrate_text_block,
    text_block_to_find_paragraph,
)
from nucliadb.search.search.merge import merge_relations_results
from nucliadb.search.search.query_parser.models import UnitRetrieval
from nucliadb.search.search.rank_fusion import IndexSource, RankFusionAlgorithm
from nucliadb.search.search.rerankers import (
    RerankableItem,
    Reranker,
    RerankingOptions,
)
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName, Resource
from nucliadb_models.search import (
    SCORE_TYPE,
    FindField,
    FindResource,
    KnowledgeboxFindResults,
    MinScore,
    ResourceProperties,
    TextPosition,
)
from nucliadb_telemetry import metrics

from .metrics import merge_observer

FIND_FETCH_OPS_DISTRIBUTION = metrics.Histogram(
    "nucliadb_find_fetch_operations",
    buckets=[1, 5, 10, 20, 30, 40, 50, 60, 80, 100, 200],
)

# Constant score given to all graph results until we implement graph scoring
FAKE_GRAPH_SCORE = 1.0


@merge_observer.wrap({"type": "find_merge"})
async def build_find_response(
    search_responses: list[SearchResponse],
    *,
    retrieval: UnitRetrieval,
    kbid: str,
    query: str,
    rephrased_query: Optional[str],
    rank_fusion_algorithm: RankFusionAlgorithm,
    reranker: Reranker,
    show: list[ResourceProperties] = [],
    extracted: list[ExtractedDataTypeName] = [],
    field_type_filter: list[FieldTypeName] = [],
    highlight: bool = False,
) -> KnowledgeboxFindResults:
    # XXX: we shouldn't need a min score that we haven't used. Previous
    # implementations got this value from the proto request (i.e., default to 0)
    min_score_bm25 = 0.0
    if retrieval.query.keyword is not None:
        min_score_bm25 = retrieval.query.keyword.min_score
    min_score_semantic = 0.0
    if retrieval.query.semantic is not None:
        min_score_semantic = retrieval.query.semantic.min_score

    # merge
    search_response = merge_shard_responses(search_responses)

    keyword_results = keyword_results_to_text_block_matches(search_response.paragraph.results)
    semantic_results = semantic_results_to_text_block_matches(
        filter(
            lambda x: x.score >= min_score_semantic,
            search_response.vector.documents,
        )
    )
    graph_results = graph_results_to_text_block_matches(search_response.graph)

    merged_text_blocks = rank_fusion_algorithm.fuse(
        {
            IndexSource.KEYWORD: keyword_results,
            IndexSource.SEMANTIC: semantic_results,
            IndexSource.GRAPH: graph_results,
        }
    )

    # cut
    # we assume pagination + predict reranker is forbidden and has been already
    # enforced/validated by the query parsing.
    if reranker.needs_extra_results:
        assert reranker.window is not None, "Reranker definition must enforce this condition"
        text_blocks_page, next_page = cut_page(merged_text_blocks, reranker.window)
    else:
        text_blocks_page, next_page = cut_page(merged_text_blocks, retrieval.top_k)

    # hydrate and rerank
    resource_hydration_options = ResourceHydrationOptions(
        show=show, extracted=extracted, field_type_filter=field_type_filter
    )
    text_block_hydration_options = TextBlockHydrationOptions(
        highlight=highlight,
        ematches=search_response.paragraph.ematches,  # type: ignore
    )
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
    relations = await merge_relations_results([search_response.graph], entry_points)

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


def merge_shard_responses(
    responses: list[SearchResponse],
) -> SearchResponse:
    """Merge search responses into a single response as if there were no shards
    involved.

    ATENTION! This is not a complete merge, we are only merging the fields
    needed to compose a /find response.

    """
    paragraphs = []
    vectors = []
    graphs = []
    for response in responses:
        paragraphs.append(response.paragraph)
        vectors.append(response.vector)
        graphs.append(response.graph)

    merged = SearchResponse(
        paragraph=merge_shards_keyword_responses(paragraphs),
        vector=merge_shards_semantic_responses(vectors),
        graph=merge_shards_graph_responses(graphs),
    )
    return merged


def merge_shards_keyword_responses(
    keyword_responses: list[ParagraphSearchResponse],
) -> ParagraphSearchResponse:
    """Merge keyword (paragraph) search responses into a single response as if
    there were no shards involved.

    ATENTION! This is not a complete merge, we are only merging the fields
    needed to compose a /find response.

    """
    merged = ParagraphSearchResponse()
    for response in keyword_responses:
        merged.query = response.query
        merged.next_page = merged.next_page or response.next_page
        merged.total += response.total
        merged.results.extend(response.results)
        merged.ematches.extend(response.ematches)

    return merged


def merge_shards_semantic_responses(
    semantic_responses: list[VectorSearchResponse],
) -> VectorSearchResponse:
    """Merge semantic (vector) search responses into a single response as if
    there were no shards involved.

    ATENTION! This is not a complete merge, we are only merging the fields
    needed to compose a /find response.

    """
    merged = VectorSearchResponse()
    for response in semantic_responses:
        merged.documents.extend(response.documents)

    return merged


def merge_shards_graph_responses(
    graph_responses: list[GraphSearchResponse],
):
    merged = GraphSearchResponse()

    for response in graph_responses:
        nodes_offset = len(merged.nodes)
        relations_offset = len(merged.relations)

        # paths contain indexes to nodes and relations, we must offset them
        # while merging responses to maintain valid data
        for path in response.graph:
            merged_path = GraphSearchResponse.Path()
            merged_path.CopyFrom(path)
            merged_path.source += nodes_offset
            merged_path.relation += relations_offset
            merged_path.destination += nodes_offset
            merged.graph.append(merged_path)

        merged.nodes.extend(response.nodes)
        merged.relations.extend(response.relations)

    return merged


def keyword_result_to_text_block_match(item: ParagraphResult) -> TextBlockMatch:
    fuzzy_result = len(item.matches) > 0
    return TextBlockMatch(
        paragraph_id=ParagraphId.from_string(item.paragraph),
        score=item.score.bm25,
        score_type=SCORE_TYPE.BM25,
        order=0,  # NOTE: this will be filled later
        text="",  # NOTE: this will be filled later too
        position=TextPosition(
            page_number=item.metadata.position.page_number,
            index=item.metadata.position.index,
            start=item.start,
            end=item.end,
            start_seconds=[x for x in item.metadata.position.start_seconds],
            end_seconds=[x for x in item.metadata.position.end_seconds],
        ),
        # XXX: we should split labels
        field_labels=[],
        paragraph_labels=list(item.labels),
        fuzzy_search=fuzzy_result,
        is_a_table=item.metadata.representation.is_a_table,
        representation_file=item.metadata.representation.file,
        page_with_visual=item.metadata.page_with_visual,
    )


def keyword_results_to_text_block_matches(items: Iterable[ParagraphResult]) -> list[TextBlockMatch]:
    return [keyword_result_to_text_block_match(item) for item in items]


class InvalidDocId(Exception):
    """Raised while parsing an invalid id coming from semantic search"""

    def __init__(self, invalid_vector_id: str):
        self.invalid_vector_id = invalid_vector_id
        super().__init__(f"Invalid vector ID: {invalid_vector_id}")


def semantic_result_to_text_block_match(item: DocumentScored) -> TextBlockMatch:
    try:
        vector_id = VectorId.from_string(item.doc_id.id)
    except (IndexError, ValueError):
        raise InvalidDocId(item.doc_id.id)

    return TextBlockMatch(
        paragraph_id=ParagraphId.from_vector_id(vector_id),
        score=item.score,
        score_type=SCORE_TYPE.VECTOR,
        order=0,  # NOTE: this will be filled later
        text="",  # NOTE: this will be filled later too
        position=TextPosition(
            page_number=item.metadata.position.page_number,
            index=item.metadata.position.index,
            start=vector_id.vector_start,
            end=vector_id.vector_end,
            start_seconds=[x for x in item.metadata.position.start_seconds],
            end_seconds=[x for x in item.metadata.position.end_seconds],
        ),
        # XXX: we should split labels
        field_labels=[],
        paragraph_labels=list(item.labels),
        fuzzy_search=False,  # semantic search doesn't have fuzziness
        is_a_table=item.metadata.representation.is_a_table,
        representation_file=item.metadata.representation.file,
        page_with_visual=item.metadata.page_with_visual,
    )


def semantic_results_to_text_block_matches(items: Iterable[DocumentScored]) -> list[TextBlockMatch]:
    text_blocks: list[TextBlockMatch] = []
    for item in items:
        try:
            text_block = semantic_result_to_text_block_match(item)
        except InvalidDocId as exc:
            logger.warning(f"Skipping invalid doc_id: {exc.invalid_vector_id}")
            continue
        text_blocks.append(text_block)
    return text_blocks


def graph_results_to_text_block_matches(item: GraphSearchResponse) -> list[TextBlockMatch]:
    matches = []
    for path in item.graph:
        metadata = path.metadata

        if not metadata.paragraph_id:
            continue

        paragraph_id = ParagraphId.from_string(metadata.paragraph_id)
        matches.append(
            TextBlockMatch(
                paragraph_id=paragraph_id,
                score=FAKE_GRAPH_SCORE,
                score_type=SCORE_TYPE.RELATION_RELEVANCE,
                order=0,  # NOTE: this will be filled later
                text="",  # NOTE: this will be filled later too
                position=TextPosition(
                    page_number=0,
                    index=0,
                    start=paragraph_id.paragraph_start,
                    end=paragraph_id.paragraph_end,
                    start_seconds=[],
                    end_seconds=[],
                ),
                # XXX: we should split labels
                field_labels=[],
                paragraph_labels=[],
                fuzzy_search=False,  # TODO: this depends on the query, should we populate it?
                is_a_table=False,
                representation_file="",
                page_with_visual=False,
            )
        )

    return matches


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
