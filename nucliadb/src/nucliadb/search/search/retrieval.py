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
from collections.abc import Iterable

from nidx_protos.nodereader_pb2 import (
    DocumentScored,
    GraphSearchResponse,
    ParagraphResult,
    ParagraphSearchResponse,
    SearchRequest,
    SearchResponse,
    VectorSearchResponse,
)

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.ids import ParagraphId, VectorId
from nucliadb.search import logger
from nucliadb.search.requesters.utils import Method, nidx_query
from nucliadb.search.search.metrics import merge_observer, search_observer
from nucliadb.search.search.query_parser.models import UnitRetrieval
from nucliadb.search.search.query_parser.parsers.unit_retrieval import convert_retrieval_to_proto
from nucliadb.search.search.rank_fusion import IndexSource, get_rank_fusion
from nucliadb_models.retrieval import GraphScore, KeywordScore, SemanticScore
from nucliadb_models.search import SCORE_TYPE, TextPosition

# Constant score given to all graph results until we implement graph scoring
FAKE_GRAPH_SCORE = 1.0


async def nidx_search(kbid: str, pb_query: SearchRequest) -> tuple[SearchResponse, list[str]]:
    """Wrapper around nidx_query for SEARCH that merges shards results in a
    single response.

    At some point, nidx will provide this functionality and we'll be able to
    remove this.

    """
    shards_responses, queried_shards = await nidx_query(kbid, Method.SEARCH, pb_query)
    response = merge_shard_responses(shards_responses)
    return response, queried_shards


@search_observer.wrap({"type": "text_block_search"})
async def text_block_search(
    kbid: str, retrieval: UnitRetrieval
) -> tuple[list[TextBlockMatch], SearchRequest, SearchResponse, list[str]]:
    """Search for text blocks in multiple indexes and return an rank fused view.

    This search method provides a textual view of the data. For example, given a
    graph query, it will return the text blocks associated with matched
    triplets, not the triplet itself.

    """
    assert retrieval.rank_fusion is not None, "text block search requries a rank fusion algorithm"

    pb_query = convert_retrieval_to_proto(retrieval)
    shards_response, queried_shards = await nidx_search(kbid, pb_query)

    keyword_results = keyword_results_to_text_block_matches(shards_response.paragraph.results)
    semantic_results = semantic_results_to_text_block_matches(shards_response.vector.documents)
    graph_results = graph_results_to_text_block_matches(shards_response.graph)

    rank_fusion = get_rank_fusion(retrieval.rank_fusion)
    merged_text_blocks = rank_fusion.fuse(
        {
            IndexSource.KEYWORD: keyword_results,
            IndexSource.SEMANTIC: semantic_results,
            IndexSource.GRAPH: graph_results,
        }
    )

    # cut to the rank fusion window. As we ask each shard and index this window,
    # we'll normally have extra results
    text_blocks = merged_text_blocks[: retrieval.rank_fusion.window]

    return text_blocks, pb_query, shards_response, queried_shards


@merge_observer.wrap({"type": "shards_responses"})
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
        scores=[KeywordScore(score=item.score.bm25)],
        score_type=SCORE_TYPE.BM25,
        order=0,  # NOTE: this will be filled later
        text=None,  # NOTE: this will be filled later too
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
        representation_file=item.metadata.representation.file or None,
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
        scores=[SemanticScore(score=item.score)],
        score_type=SCORE_TYPE.VECTOR,
        order=0,  # NOTE: this will be filled later
        text=None,  # NOTE: this will be filled later too
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
        representation_file=item.metadata.representation.file or None,
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
                scores=[GraphScore(score=FAKE_GRAPH_SCORE)],
                score_type=SCORE_TYPE.RELATION_RELEVANCE,
                order=0,  # NOTE: this will be filled later
                text=None,  # NOTE: this will be filled later too
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
