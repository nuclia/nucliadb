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
import datetime
import math
from typing import Any, Optional, Set, Union

from nucliadb_protos.nodereader_pb2 import (
    DocumentResult,
    DocumentScored,
    DocumentSearchResponse,
    EntitiesSubgraphRequest,
    ParagraphResult,
    ParagraphSearchResponse,
    RelationSearchResponse,
    SearchResponse,
    SuggestResponse,
    VectorSearchResponse,
)

from nucliadb.search.search.fetch import (
    fetch_resources,
    get_labels_paragraph,
    get_labels_resource,
    get_seconds_paragraph,
)
from nucliadb_models.common import FieldTypeName
from nucliadb_models.labels import translate_system_to_alias_label
from nucliadb_models.metadata import RelationTypePbMap
from nucliadb_models.resource import ExtractedDataTypeName
from nucliadb_models.search import (
    DirectionalRelation,
    EntitySubgraph,
    KnowledgeboxSearchResults,
    KnowledgeboxSuggestResults,
    MinScore,
    Paragraph,
    Paragraphs,
    RelatedEntities,
    RelatedEntity,
    RelationDirection,
    RelationNodeTypeMap,
    Relations,
    ResourceProperties,
    ResourceResult,
    Resources,
    ResourceSearchResults,
    Sentence,
    Sentences,
    SortField,
    SortOptions,
    SortOrder,
    TextPosition,
)

from .cache import get_resource_cache, get_resource_from_cache
from .metrics import merge_observer
from .paragraphs import ExtractedTextCache, get_paragraph_text, get_text_sentence

Bm25Score = tuple[float, float]
TimestampScore = datetime.datetime
TitleScore = str
Score = Union[Bm25Score, TimestampScore, TitleScore]


def sort_results_by_score(results: Union[list[ParagraphResult], list[DocumentResult]]):
    results.sort(key=lambda x: (x.score.bm25, x.score.booster), reverse=True)


async def text_score(
    item: Union[DocumentResult, ParagraphResult],
    sort_field: SortField,
    kbid: str,
) -> Optional[Score]:
    """Returns the score for given `item` and `sort_field`. If the resource is being
    deleted, it might appear on search results but not in maindb. In this
    specific case, return None.

    """
    if sort_field == SortField.SCORE:
        return (item.score.bm25, item.score.booster)

    score: Any = None
    resource = await get_resource_from_cache(kbid, item.uuid)
    if resource is None:
        return score
    basic = await resource.get_basic()
    if basic is None:
        return score

    if sort_field == SortField.CREATED:
        score = basic.created.ToDatetime()
    elif sort_field == SortField.MODIFIED:
        score = basic.modified.ToDatetime()
    elif sort_field == SortField.TITLE:
        score = basic.title

    return score


async def merge_documents_results(
    document_responses: list[DocumentSearchResponse],
    resources: list[str],
    count: int,
    page: int,
    kbid: str,
    sort: SortOptions,
    min_score: float,
) -> Resources:
    raw_resource_list: list[tuple[DocumentResult, Score]] = []
    facets: dict[str, Any] = {}
    query = None
    total = 0
    next_page = False
    for document_response in document_responses:
        if query is None:
            query = document_response.query
        if document_response.facets:
            for key, value in document_response.facets.items():
                key = translate_system_to_alias_label(key)
                for facetresult in value.facetresults:
                    facet_label = translate_system_to_alias_label(facetresult.tag)
                    facets.setdefault(key, {}).setdefault(facet_label, 0)
                    facets[key][facet_label] += facetresult.total

        if document_response.next_page:
            next_page = True
        for result in document_response.results:
            score = await text_score(result, sort.field, kbid)
            if score is not None:
                raw_resource_list.append((result, score))
        total += document_response.total

    raw_resource_list.sort(key=lambda x: x[1], reverse=(sort.order == SortOrder.DESC))

    skip = page * count
    end = skip + count
    length = len(raw_resource_list)

    if length > end:
        next_page = True

    result_resource_list: list[ResourceResult] = []
    for result, _ in raw_resource_list[min(skip, length) : min(end, length)]:
        # /f/file

        labels = await get_labels_resource(result, kbid)
        _, field_type, field = result.field.split("/")

        result_resource_list.append(
            ResourceResult(
                score=result.score.bm25,
                rid=result.uuid,
                field=field,
                field_type=field_type,
                labels=labels,
            )
        )
        if result.uuid not in resources:
            resources.append(result.uuid)

    return Resources(
        facets=facets,
        results=result_resource_list,
        query=query,
        total=total,
        page_number=page,
        page_size=count,
        next_page=next_page,
        min_score=min_score,
    )


async def merge_suggest_paragraph_results(
    suggest_responses: list[SuggestResponse],
    kbid: str,
    highlight: bool,
):
    raw_paragraph_list: list[ParagraphResult] = []
    query = None
    ematches = None
    for suggest_response in suggest_responses:
        if query is None:
            query = suggest_response.query
        if ematches is None:
            ematches = suggest_response.ematches
        for result in suggest_response.results:
            raw_paragraph_list.append(result)

    if len(suggest_responses) > 1:
        sort_results_by_score(raw_paragraph_list)

    rcache = get_resource_cache(clear=True)
    etcache = ExtractedTextCache()
    try:
        result_paragraph_list: list[Paragraph] = []
        for result in raw_paragraph_list[:10]:
            _, field_type, field = result.field.split("/")
            text = await get_paragraph_text(
                kbid=kbid,
                rid=result.uuid,
                field=result.field,
                start=result.start,
                end=result.end,
                split=result.split,
                highlight=highlight,
                ematches=ematches,  # type: ignore
                matches=result.matches,  # type: ignore
                extracted_text_cache=etcache,
            )
            labels = await get_labels_paragraph(result, kbid)
            new_paragraph = Paragraph(
                score=result.score.bm25,
                rid=result.uuid,
                field_type=field_type,
                field=field,
                text=text,
                labels=labels,
                position=TextPosition(
                    index=result.metadata.position.index,
                    start=result.metadata.position.start,
                    end=result.metadata.position.end,
                    page_number=result.metadata.position.page_number,
                ),
            )
            if len(result.metadata.position.start_seconds) or len(
                result.metadata.position.end_seconds
            ):
                new_paragraph.start_seconds = list(
                    result.metadata.position.start_seconds
                )
                new_paragraph.end_seconds = list(result.metadata.position.end_seconds)
            else:
                # TODO: Remove once we are sure all data has been migrated!
                seconds_positions = await get_seconds_paragraph(result, kbid)
                if seconds_positions is not None:
                    new_paragraph.start_seconds = seconds_positions[0]
                    new_paragraph.end_seconds = seconds_positions[1]
            result_paragraph_list.append(new_paragraph)
        return Paragraphs(results=result_paragraph_list, query=query, min_score=0)
    finally:
        etcache.clear()
        rcache.clear()


async def merge_vectors_results(
    vector_responses: list[VectorSearchResponse],
    resources: list[str],
    kbid: str,
    count: int,
    page: int,
    min_score: Optional[float] = None,
):
    facets: dict[str, Any] = {}
    raw_vectors_list: list[DocumentScored] = []

    for vector_response in vector_responses:
        for document in vector_response.documents:
            if min_score is not None and document.score < min_score:
                continue
            if math.isnan(document.score):
                continue
            raw_vectors_list.append(document)

    if len(vector_responses) > 1:
        raw_vectors_list.sort(key=lambda x: x.score, reverse=True)

    skip = page * count
    end_element = skip + count
    length = len(raw_vectors_list)

    result_sentence_list: list[Sentence] = []
    for result in raw_vectors_list[min(skip, length) : min(end_element, length)]:
        id_count = result.doc_id.id.count("/")
        if id_count == 4:
            rid, field_type, field, index, position = result.doc_id.id.split("/")
            subfield = None
        elif id_count == 5:
            (
                rid,
                field_type,
                field,
                subfield,
                index,
                position,
            ) = result.doc_id.id.split("/")
        if result.metadata.HasField("position"):
            start_int = result.metadata.position.start
            end_int = result.metadata.position.end
            index_int = result.metadata.position.index
        else:
            # bbb pull position from key for old results that were
            # not properly filling metadata
            start, end = position.split("-")
            start_int = int(start)
            end_int = int(end)
            try:
                index_int = int(index)
            except ValueError:
                index_int = -1
        text = await get_text_sentence(
            rid, field_type, field, kbid, index_int, start_int, end_int, subfield
        )
        result_sentence_list.append(
            Sentence(
                score=result.score,
                rid=rid,
                field_type=field_type,
                field=field,
                text=text,
                index=index,
                position=TextPosition(start=start_int, end=end_int, index=index_int),
            )
        )
        if rid not in resources:
            resources.append(rid)

    return Sentences(
        results=result_sentence_list,
        facets=facets,
        page_number=page,
        page_size=count,
        min_score=round(min_score or 0, ndigits=3),
    )


async def merge_paragraph_results(
    paragraph_responses: list[ParagraphSearchResponse],
    resources: list[str],
    kbid: str,
    count: int,
    page: int,
    highlight: bool,
    sort: SortOptions,
    min_score: float,
):
    raw_paragraph_list: list[tuple[ParagraphResult, Score]] = []
    facets: dict[str, Any] = {}
    query = None
    next_page = False
    ematches: Optional[list[str]] = None
    total = 0
    for paragraph_response in paragraph_responses:
        if ematches is None:
            ematches = paragraph_response.ematches  # type: ignore
        if query is None:
            query = paragraph_response.query

        if paragraph_response.facets:
            for key, value in paragraph_response.facets.items():
                key = translate_system_to_alias_label(key)
                for facetresult in value.facetresults:
                    facet_label = translate_system_to_alias_label(facetresult.tag)
                    facets.setdefault(key, {}).setdefault(facet_label, 0)
                    facets[key][facet_label] += facetresult.total
        if paragraph_response.next_page:
            next_page = True
        for result in paragraph_response.results:
            score = await text_score(result, sort.field, kbid)
            if score is not None:
                raw_paragraph_list.append((result, score))
        total += paragraph_response.total

    raw_paragraph_list.sort(key=lambda x: x[1], reverse=(sort.order == SortOrder.DESC))

    skip = page * count
    end = skip + count
    length = len(raw_paragraph_list)

    if length > end:
        next_page = True

    result_paragraph_list: list[Paragraph] = []
    etcache = ExtractedTextCache()
    try:
        for result, _ in raw_paragraph_list[min(skip, length) : min(end, length)]:
            _, field_type, field = result.field.split("/")
            text = await get_paragraph_text(
                kbid=kbid,
                rid=result.uuid,
                field=result.field,
                start=result.start,
                end=result.end,
                split=result.split,
                highlight=highlight,
                ematches=ematches,
                matches=result.matches,  # type: ignore
                extracted_text_cache=etcache,
            )
            labels = await get_labels_paragraph(result, kbid)
            fuzzy_result = len(result.matches) > 0
            new_paragraph = Paragraph(
                score=result.score.bm25,
                rid=result.uuid,
                field_type=field_type,
                field=field,
                text=text,
                labels=labels,
                position=TextPosition(
                    index=result.metadata.position.index,
                    start=result.metadata.position.start,
                    end=result.metadata.position.end,
                    page_number=result.metadata.position.page_number,
                ),
                fuzzy_result=fuzzy_result,
            )
            if len(result.metadata.position.start_seconds) or len(
                result.metadata.position.end_seconds
            ):
                new_paragraph.start_seconds = list(
                    result.metadata.position.start_seconds
                )
                new_paragraph.end_seconds = list(result.metadata.position.end_seconds)
            else:
                # TODO: Remove once we are sure all data has been migrated!
                seconds_positions = await get_seconds_paragraph(result, kbid)
                if seconds_positions is not None:
                    new_paragraph.start_seconds = seconds_positions[0]
                    new_paragraph.end_seconds = seconds_positions[1]

            result_paragraph_list.append(new_paragraph)
            if new_paragraph.rid not in resources:
                resources.append(new_paragraph.rid)
        return Paragraphs(
            results=result_paragraph_list,
            facets=facets,
            query=query,
            total=total,
            page_number=page,
            page_size=count,
            next_page=next_page,
            min_score=min_score,
        )
    finally:
        etcache.clear()


@merge_observer.wrap({"type": "merge_relations"})
async def merge_relations_results(
    relations_responses: list[RelationSearchResponse],
    query: EntitiesSubgraphRequest,
) -> Relations:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, _merge_relations_results, relations_responses, query
    )


def _merge_relations_results(
    relations_responses: list[RelationSearchResponse],
    query: EntitiesSubgraphRequest,
) -> Relations:
    relations = Relations(entities={})

    for entry_point in query.entry_points:
        relations.entities[entry_point.value] = EntitySubgraph(related_to=[])

    for relation_response in relations_responses:
        for relation in relation_response.subgraph.relations:
            origin = relation.source
            destination = relation.to
            relation_type = RelationTypePbMap[relation.relation]
            relation_label = relation.relation_label

            if origin.value in relations.entities:
                relations.entities[origin.value].related_to.append(
                    DirectionalRelation(
                        entity=destination.value,
                        entity_type=RelationNodeTypeMap[destination.ntype],
                        relation=relation_type,
                        relation_label=relation_label,
                        direction=RelationDirection.OUT,
                    )
                )
            elif destination.value in relations.entities:
                relations.entities[destination.value].related_to.append(
                    DirectionalRelation(
                        entity=origin.value,
                        entity_type=RelationNodeTypeMap[origin.ntype],
                        relation=relation_type,
                        relation_label=relation_label,
                        direction=RelationDirection.IN,
                    )
                )

    return relations


@merge_observer.wrap({"type": "merge"})
async def merge_results(
    search_responses: list[SearchResponse],
    count: int,
    page: int,
    kbid: str,
    show: list[ResourceProperties],
    field_type_filter: list[FieldTypeName],
    extracted: list[ExtractedDataTypeName],
    sort: SortOptions,
    requested_relations: EntitiesSubgraphRequest,
    min_score: MinScore,
    highlight: bool = False,
) -> KnowledgeboxSearchResults:
    paragraphs = []
    documents = []
    vectors = []
    relations = []

    for response in search_responses:
        paragraphs.append(response.paragraph)
        documents.append(response.document)
        vectors.append(response.vector)
        relations.append(response.relation)

    api_results = KnowledgeboxSearchResults()

    rcache = get_resource_cache(clear=True)
    try:
        resources: list[str] = list()
        api_results.fulltext = await merge_documents_results(
            documents, resources, count, page, kbid, sort, min_score=min_score.bm25
        )

        api_results.paragraphs = await merge_paragraph_results(
            paragraphs,
            resources,
            kbid,
            count,
            page,
            highlight,
            sort,
            min_score=min_score.bm25,
        )

        api_results.sentences = await merge_vectors_results(
            vectors, resources, kbid, count, page, min_score=min_score.semantic
        )

        api_results.relations = await merge_relations_results(
            relations, requested_relations
        )

        api_results.resources = await fetch_resources(
            resources, kbid, show, field_type_filter, extracted
        )
        return api_results
    finally:
        rcache.clear()


async def merge_paragraphs_results(
    paragraph_responses: list[ParagraphSearchResponse],
    count: int,
    page: int,
    kbid: str,
    show: list[ResourceProperties],
    field_type_filter: list[FieldTypeName],
    extracted: list[ExtractedDataTypeName],
    highlight_split: bool,
    min_score: float,
) -> ResourceSearchResults:
    paragraphs = []
    for result in paragraph_responses:
        paragraphs.append(result)

    api_results = ResourceSearchResults()

    rcache = get_resource_cache(clear=True)
    try:
        resources: list[str] = list()
        api_results.paragraphs = await merge_paragraph_results(
            paragraphs,
            resources,
            kbid,
            count,
            page,
            highlight=highlight_split,
            sort=SortOptions(
                field=SortField.SCORE,
                order=SortOrder.DESC,
                limit=None,
            ),
            min_score=min_score,
        )
        return api_results
    finally:
        rcache.clear()


async def merge_suggest_entities_results(
    suggest_responses: list[SuggestResponse],
) -> RelatedEntities:
    unique_entities: Set[RelatedEntity] = set()
    for response in suggest_responses:
        response_entities = (
            RelatedEntity(family=e.subtype, value=e.value)
            for e in response.entity_results.nodes
        )
        unique_entities.update(response_entities)

    return RelatedEntities(entities=list(unique_entities), total=len(unique_entities))


async def merge_suggest_results(
    suggest_responses: list[SuggestResponse],
    kbid: str,
    show: list[ResourceProperties],
    field_type_filter: list[FieldTypeName],
    highlight: bool = False,
) -> KnowledgeboxSuggestResults:
    api_results = KnowledgeboxSuggestResults()

    api_results.paragraphs = await merge_suggest_paragraph_results(
        suggest_responses, kbid, highlight=highlight
    )
    api_results.entities = await merge_suggest_entities_results(suggest_responses)
    return api_results
