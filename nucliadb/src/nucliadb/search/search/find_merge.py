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
from typing import Optional, cast

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.ids import ParagraphId, VectorId
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.serialize import managed_serialize
from nucliadb.search import SERVICE_NAME, logger
from nucliadb.search.search.merge import merge_relations_results
from nucliadb.search.search.results_hydrator.base import (
    text_block_to_find_paragraph,
)
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName
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
from nucliadb_utils import const
from nucliadb_utils.utilities import has_feature

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
    highlight: bool = False,
    ematches: Optional[list[str]] = None,
):
    async with max_operations:
        find_paragraph.text = await paragraphs.get_paragraph_text(
            kbid=kbid,
            paragraph_id=paragraph_id,
            highlight=highlight,
            ematches=ematches,
            matches=[],  # TODO
        )


@merge_observer.wrap({"type": "set_resource_metadada_value"})
async def set_resource_metadata_value(
    txn: Transaction,
    kbid: str,
    resource: str,
    show: list[ResourceProperties],
    field_type_filter: list[FieldTypeName],
    extracted: list[ExtractedDataTypeName],
    find_resources: dict[str, FindResource],
    max_operations: asyncio.Semaphore,
):
    if ResourceProperties.EXTRACTED in show and has_feature(
        const.Features.IGNORE_EXTRACTED_IN_SEARCH, context={"kbid": kbid}, default=False
    ):
        # Returning extracted metadata in search results is deprecated and this flag
        # will be set to True for all KBs in the future.
        show.remove(ResourceProperties.EXTRACTED)
        extracted = []

    async with max_operations:
        serialized_resource = await managed_serialize(
            txn,
            kbid,
            resource,
            show,
            field_type_filter=field_type_filter,
            extracted=extracted,
            service_name=SERVICE_NAME,
        )
        if serialized_resource is not None:
            find_resources[resource].updated_from(serialized_resource)
        else:
            logger.warning(f"Resource {resource} not found in {kbid}")
            find_resources.pop(resource, None)


@merge_observer.wrap({"type": "fetch_find_metadata"})
async def fetch_find_metadata(
    result_paragraphs: list[TextBlockMatch],
    kbid: str,
    show: list[ResourceProperties],
    field_type_filter: list[FieldTypeName],
    extracted: list[ExtractedDataTypeName],
    highlight: bool = False,
    ematches: Optional[list[str]] = None,
) -> tuple[dict[str, FindResource], list[str]]:
    find_resources: dict[str, FindResource] = {}
    best_matches: list[str] = []

    resources = set()
    operations = []
    max_operations = asyncio.Semaphore(50)
    paragraphs_to_sort: list[SortableParagraph] = []
    for text_block in result_paragraphs:
        rid = text_block.paragraph_id.rid
        find_resource = find_resources.setdefault(rid, FindResource(id=rid, fields={}))
        field_id = text_block.paragraph_id.field_id.short_without_subfield()
        find_field = find_resource.fields.setdefault(field_id, FindField(paragraphs={}))
        paragraph_id = text_block.paragraph_id.full()

        if paragraph_id in find_field.paragraphs:
            # Its a multiple match, boost the score
            if find_field.paragraphs[paragraph_id].score < text_block.score:
                # Use Vector score if there are both
                find_field.paragraphs[paragraph_id].score = text_block.score * 2
                paragraphs_to_sort.append(
                    SortableParagraph(
                        rid=rid,
                        fid=field_id,
                        pid=paragraph_id,
                        score=text_block.score,
                    )
                )
            find_field.paragraphs[paragraph_id].score_type = SCORE_TYPE.BOTH

        else:
            find_field.paragraphs[paragraph_id] = text_block_to_find_paragraph(text_block)
            paragraphs_to_sort.append(
                SortableParagraph(
                    rid=rid,
                    fid=field_id,
                    pid=paragraph_id,
                    score=text_block.score,
                )
            )
        operations.append(
            asyncio.create_task(
                set_text_value(
                    kbid=kbid,
                    paragraph_id=text_block.paragraph_id,
                    find_paragraph=find_field.paragraphs[paragraph_id],
                    highlight=highlight,
                    ematches=ematches,
                    max_operations=max_operations,
                )
            )
        )
        resources.add(rid)

    for order, paragraph in enumerate(
        sorted(paragraphs_to_sort, key=lambda par: par.score, reverse=True)
    ):
        find_resources[paragraph.rid].fields[paragraph.fid].paragraphs[paragraph.pid].order = order
        best_matches.append(paragraph.pid)

    async with get_driver().transaction(read_only=True) as txn:
        for resource in resources:
            operations.append(
                asyncio.create_task(
                    set_resource_metadata_value(
                        txn,
                        kbid=kbid,
                        resource=resource,
                        show=show,
                        field_type_filter=field_type_filter,
                        extracted=extracted,
                        find_resources=find_resources,
                        max_operations=max_operations,
                    )
                )
            )

        FIND_FETCH_OPS_DISTRIBUTION.observe(len(operations))
        if len(operations) > 0:
            done, _ = await asyncio.wait(operations)
            for task in done:
                if task.exception() is not None:  # pragma: no cover
                    logger.error("Error fetching find metadata", exc_info=task.exception())

    return find_resources, best_matches


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

    resources, best_matches = await fetch_find_metadata(
        result_paragraphs,
        kbid,
        show,
        field_type_filter,
        extracted,
        highlight,
        ematches,
    )
    api_results.resources = resources
    api_results.best_matches = best_matches
    api_results.relations = await merge_relations_results(relations, requested_relations)

    return api_results
