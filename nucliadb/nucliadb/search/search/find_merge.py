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
from typing import Any, Dict, Iterator, List, Optional, Tuple, cast

from nucliadb_protos.nodereader_pb2 import (
    DocumentScored,
    EntitiesSubgraphRequest,
    ParagraphResult,
    SearchResponse,
)

from nucliadb.ingest.serialize import serialize
from nucliadb.ingest.txn_utils import abort_transaction, get_transaction
from nucliadb.search import SERVICE_NAME
from nucliadb.search.search.cache import get_resource_cache
from nucliadb.search.search.merge import merge_relations_results
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName
from nucliadb_models.search import (
    SCORE_TYPE,
    FindField,
    FindParagraph,
    FindResource,
    KnowledgeboxFindResults,
    ResourceProperties,
    SortOptions,
    TempFindParagraph,
    TextPosition,
)
from nucliadb_telemetry import metrics

from .metrics import merge_observer
from .paragraphs import get_paragraph_text

FIND_FETCH_OPS_DISTRIBUTION = metrics.Histogram(
    "nucliadb_find_fetch_operations",
    buckets=[1, 5, 10, 20, 30, 40, 50, 60, 80, 100, 200],
)


async def set_text_value(
    kbid: str,
    result_paragraph: TempFindParagraph,
    max_operations: asyncio.Semaphore,
    highlight: bool = False,
    ematches: Optional[List[str]] = None,
):
    # TODO: Improve
    await max_operations.acquire()
    try:
        assert result_paragraph.paragraph
        assert result_paragraph.paragraph.position
        result_paragraph.paragraph.text = await get_paragraph_text(
            kbid=kbid,
            rid=result_paragraph.rid,
            field=result_paragraph.field,
            start=result_paragraph.paragraph.position.start,
            end=result_paragraph.paragraph.position.end,
            split=None,  # TODO
            highlight=highlight,
            ematches=ematches,
            matches=[],  # TODO
        )
    finally:
        max_operations.release()


async def set_resource_metadata_value(
    kbid: str,
    resource: str,
    show: List[ResourceProperties],
    field_type_filter: List[FieldTypeName],
    extracted: List[ExtractedDataTypeName],
    find_resources: Dict[str, FindResource],
    max_operations: asyncio.Semaphore,
):
    await max_operations.acquire()

    try:
        serialized_resource = await serialize(
            kbid,
            resource,
            show,
            field_type_filter=field_type_filter,
            extracted=extracted,
            service_name=SERVICE_NAME,
        )
        if serialized_resource is not None:
            find_resources[resource].updated_from(serialized_resource)
    finally:
        max_operations.release()


class Orderer:
    def __init__(self):
        self.boosted_items = []
        self.items = []

    def add(self, key: Any):
        self.items.append(key)

    def add_boosted(self, key: Any):
        self.boosted_items.append(key)

    def sorted_by_insertion(self) -> Iterator[Any]:
        for key in self.boosted_items:
            yield key
        for key in self.items:
            yield key


async def fetch_find_metadata(
    find_resources: Dict[str, FindResource],
    result_paragraphs: List[TempFindParagraph],
    kbid: str,
    show: List[ResourceProperties],
    field_type_filter: List[FieldTypeName],
    extracted: List[ExtractedDataTypeName],
    highlight: bool = False,
    ematches: Optional[List[str]] = None,
):
    resources = set()
    operations = []
    max_operations = asyncio.Semaphore(50)

    orderer = Orderer()

    for result_paragraph in result_paragraphs:
        if result_paragraph.paragraph is not None:
            find_resource = find_resources.setdefault(
                result_paragraph.rid, FindResource(id=result_paragraph.id, fields={})
            )
            find_field = find_resource.fields.setdefault(
                result_paragraph.field, FindField(paragraphs={})
            )

            if result_paragraph.paragraph.id in find_field.paragraphs:
                # Its a multiple match, push the score
                find_field.paragraphs[result_paragraph.paragraph.id].score = 25
                find_field.paragraphs[
                    result_paragraph.paragraph.id
                ].score_type = SCORE_TYPE.BOTH
                orderer.add_boosted(
                    (
                        result_paragraph.rid,
                        result_paragraph.field,
                        result_paragraph.paragraph.id,
                    )
                )
            else:
                find_field.paragraphs[
                    result_paragraph.paragraph.id
                ] = result_paragraph.paragraph
                orderer.add(
                    (
                        result_paragraph.rid,
                        result_paragraph.field,
                        result_paragraph.paragraph.id,
                    )
                )

            operations.append(
                set_text_value(
                    kbid=kbid,
                    result_paragraph=result_paragraph,
                    highlight=highlight,
                    ematches=ematches,
                    max_operations=max_operations,
                )
            )
            resources.add(result_paragraph.rid)

    for order, (rid, field_id, paragraph_id) in enumerate(
        orderer.sorted_by_insertion()
    ):
        find_resources[rid].fields[field_id].paragraphs[paragraph_id].order = order

    for resource in resources:
        operations.append(
            set_resource_metadata_value(
                kbid=kbid,
                resource=resource,
                show=show,
                field_type_filter=field_type_filter,
                extracted=extracted,
                find_resources=find_resources,
                max_operations=max_operations,
            )
        )

    FIND_FETCH_OPS_DISTRIBUTION.observe(len(operations))
    if len(operations) > 0:
        await asyncio.wait(operations)  # type: ignore


async def merge_paragraphs_vectors(
    paragraphs_shards: List[List[ParagraphResult]],
    vectors_shards: List[List[DocumentScored]],
    count: int,
    page: int,
    min_score: float,
) -> Tuple[List[TempFindParagraph], bool]:
    merged_paragrahs: List[TempFindParagraph] = []

    # We assume that paragraphs_shards and vectors_shards are already ordered
    for paragraphs_shard in paragraphs_shards:
        for paragraph in paragraphs_shard:
            merged_paragrahs.append(
                TempFindParagraph(
                    paragraph_index=paragraph,
                    field=paragraph.field,
                    rid=paragraph.uuid,
                    score=paragraph.score.bm25,
                    start=paragraph.start,
                    end=paragraph.end,
                    id=paragraph.paragraph,
                )
            )

    # merged_paragrahs.sort(key=lambda r: r.score, reverse=True)

    nextpos = 1
    for vectors_shard in vectors_shards:
        for vector in vectors_shard:
            if vector.score >= min_score:
                doc_id_split = vector.doc_id.id.split("/")
                if len(doc_id_split) == 5:
                    rid, field_type, field, index, position = doc_id_split
                    paragraph_id = f"{rid}/{field_type}/{field}/{position}"
                elif len(doc_id_split) == 6:
                    rid, field_type, field, split, index, position = doc_id_split
                    paragraph_id = f"{rid}/{field_type}/{field}/{split}/{position}"
                start, end = position.split("-")
                merged_paragrahs.insert(
                    nextpos,
                    TempFindParagraph(
                        vector_index=vector,
                        rid=rid,
                        field=f"/{field_type}/{field}",
                        score=vector.score,
                        start=int(start),
                        end=int(end),
                        id=paragraph_id,
                    ),
                )
                nextpos += 3

    # merged_paragrahs.sort(key=lambda r: r.score, reverse=True)
    init_position = count * page
    end_position = init_position + count
    next_page = len(merged_paragrahs) > end_position
    merged_paragrahs = merged_paragrahs[init_position:end_position]

    for merged_paragraph in merged_paragrahs:
        if merged_paragraph.vector_index is not None:
            merged_paragraph.paragraph = FindParagraph(
                score=vector.score,
                score_type=SCORE_TYPE.VECTOR,
                text="",
                labels=[],  # TODO: Get labels from index
                position=TextPosition(
                    page_number=merged_paragraph.vector_index.metadata.position.page_number,
                    index=merged_paragraph.vector_index.metadata.position.index,
                    start=merged_paragraph.start,
                    end=merged_paragraph.end,
                    start_seconds=[
                        x
                        for x in merged_paragraph.vector_index.metadata.position.start_seconds
                    ],
                    end_seconds=[
                        x
                        for x in merged_paragraph.vector_index.metadata.position.end_seconds
                    ],
                ),
                id=merged_paragraph.id,
            )
        if merged_paragraph.paragraph_index is not None:
            merged_paragraph.paragraph = FindParagraph(
                score=merged_paragraph.paragraph_index.score.bm25,
                score_type=SCORE_TYPE.BM25,
                text="",
                labels=[x for x in merged_paragraph.paragraph_index.labels],
                position=TextPosition(
                    page_number=merged_paragraph.paragraph_index.metadata.position.page_number,
                    index=merged_paragraph.paragraph_index.metadata.position.index,
                    start=merged_paragraph.start,
                    end=merged_paragraph.end,
                    start_seconds=[
                        x
                        for x in merged_paragraph.paragraph_index.metadata.position.start_seconds
                    ],
                    end_seconds=[
                        x
                        for x in merged_paragraph.paragraph_index.metadata.position.end_seconds
                    ],
                ),
                id=merged_paragraph.id,
            )
    return merged_paragrahs, next_page


@merge_observer.wrap({"type": "find_merge"})
async def find_merge_results(
    search_responses: List[SearchResponse],
    count: int,
    page: int,
    kbid: str,
    show: List[ResourceProperties],
    field_type_filter: List[FieldTypeName],
    extracted: List[ExtractedDataTypeName],
    sort: SortOptions,
    requested_relations: EntitiesSubgraphRequest,
    min_score: float = 0.85,
    highlight: bool = False,
) -> KnowledgeboxFindResults:
    # force getting transaction on current asyncio task
    # so all sub tasks will use the same transaction
    # this is contextvar magic that is probably not ideal
    await get_transaction()

    paragraphs: List[List[ParagraphResult]] = []
    vectors: List[List[DocumentScored]] = []
    relations = []

    # facets_counter = Counter()
    next_page = True
    ematches: List[str] = []
    real_query = ""
    total_paragraphs = 0
    for response in search_responses:
        # Iterate over answers from different logic shards

        # Merge facets
        # TODO
        # facets_counter.update(response.paragraph.facets)
        ematches.extend(response.paragraph.ematches)
        real_query = response.paragraph.query
        next_page = next_page and response.paragraph.next_page
        total_paragraphs += response.paragraph.total

        paragraphs.append(cast(List[ParagraphResult], response.paragraph.results))
        vectors.append(cast(List[DocumentScored], response.vector.documents))

        relations.append(response.relation)

    get_resource_cache(clear=True)

    result_paragraphs, merged_next_page = await merge_paragraphs_vectors(
        paragraphs, vectors, count, page, min_score
    )
    next_page = next_page or merged_next_page

    api_results = KnowledgeboxFindResults(
        resources={},
        facets={},
        query=real_query,
        total=total_paragraphs,
        page_number=page,
        page_size=count,
        next_page=next_page,
    )

    await fetch_find_metadata(
        api_results.resources,
        result_paragraphs,
        kbid,
        show,
        field_type_filter,
        extracted,
        highlight,
        ematches,
    )
    api_results.relations = await merge_relations_results(
        relations, requested_relations
    )

    await abort_transaction()
    return api_results
