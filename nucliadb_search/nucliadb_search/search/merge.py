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
from typing import Any, Dict, List

from nucliadb_protos.nodereader_pb2 import (
    DocumentSearchResponse,
    ParagraphSearchResponse,
    SearchResponse,
    SuggestResponse,
    VectorSearchResponse,
)

from nucliadb_models.common import FieldTypeName
from nucliadb_models.serialize import ExtractedDataTypeName, ResourceProperties
from nucliadb_search.api.models import (
    KnowledgeboxSearchResults,
    KnowledgeboxSuggestResults,
    Paragraph,
    Paragraphs,
    ResourceResult,
    Resources,
    ResourceSearchResults,
    Sentence,
    Sentences,
)
from nucliadb_search.search.fetch import (
    fetch_resources,
    get_labels_paragraph,
    get_labels_sentence,
    get_resource_cache,
    get_text_paragraph,
    get_text_sentence,
)


async def merge_documents_results(
    documents: List[DocumentSearchResponse],
    resources: List[str],
    count: int,
    page: int,
) -> Resources:

    raw_resource_list: List[ResourceResult] = []
    facets: Dict[str, Any] = {}
    for document_response in documents:
        facets.update(document_response.facets)
        for result in document_response.results:
            # /f/file
            _, field_type, field = result.field.split("/")
            if result.score == 0:
                score = result.score_bm25
            else:
                score = result.score
            raw_resource_list.append(
                ResourceResult(
                    score=score,
                    rid=result.uuid,
                    field=field,
                    field_type=field_type,
                )
            )

    raw_resource_list.sort(key=lambda x: x.score)

    init = count * page
    last = init + count
    if last > len(raw_resource_list):
        last = len(raw_resource_list)

    resource_list = raw_resource_list[init:last]
    # Filter the resources that are matching the length

    for resource in resource_list:
        if resource.rid not in resources:
            resources.append(resource.rid)

    return Resources(facets=facets, results=resource_list)


async def merge_suggest_paragraph_results(
    suggest_responses: List[SuggestResponse],
    kbid: str,
):

    raw_paragraph_list: List[Paragraph] = []
    for suggest_response in suggest_responses:
        for result in suggest_response.results:
            _, field_type, field = result.field.split("/")
            text = await get_text_paragraph(result, kbid)
            labels = await get_labels_paragraph(result, kbid)
            raw_paragraph_list.append(
                Paragraph(
                    score=result.score,
                    rid=result.uuid,
                    field_type=field_type,
                    field=field,
                    text=text,
                    labels=labels,
                )
            )

    return Paragraphs(results=raw_paragraph_list)


async def merge_vectors_results(
    vectors: List[VectorSearchResponse],
    resources: List[str],
    kbid: str,
    count: int,
    page: int,
    max_score: float = 0.85,
):
    results: List[Sentence] = []
    facets: Dict[str, Any] = {}
    for vector in vectors:
        for document in vector.documents:
            if document.score < max_score:
                continue
            count = document.doc_id.id.count("/")
            if count == 4:
                rid, field_type, field, index, position = document.doc_id.id.split("/")
                subfield = None
            elif count == 5:
                (
                    rid,
                    field_type,
                    field,
                    subfield,
                    index,
                    position,
                ) = document.doc_id.id.split("/")
            start, end = position.split("-")
            start_int = int(start)
            end_int = int(end)
            index_int = int(index)
            text = await get_text_sentence(
                rid, field_type, field, kbid, index_int, start_int, end_int, subfield
            )
            labels = await get_labels_sentence(
                rid, field_type, field, kbid, index_int, start_int, end_int, subfield
            )
            results.append(
                Sentence(
                    score=document.score,
                    rid=rid,
                    field_type=field_type,
                    field=field,
                    text=text,
                    labels=labels,
                )
            )

    results.sort(key=lambda x: x.score)

    for paragraph in results:
        if paragraph.rid not in resources:
            resources.append(paragraph.rid)

    return Sentences(results=results, facets=facets)


async def merge_paragraph_results(
    paragraphs: List[ParagraphSearchResponse],
    resources: List[str],
    kbid: str,
    count: int,
    page: int,
):

    raw_paragraph_list: List[Paragraph] = []
    facets: Dict[str, Any] = {}
    for paragraph_response in paragraphs:
        facets.update(paragraph_response.facets)
        for result in paragraph_response.results:
            _, field_type, field = result.field.split("/")
            text = await get_text_paragraph(result, kbid)
            labels = await get_labels_paragraph(result, kbid)
            raw_paragraph_list.append(
                Paragraph(
                    score=result.score,
                    rid=result.uuid,
                    field_type=field_type,
                    field=field,
                    text=text,
                    labels=labels,
                )
            )

    raw_paragraph_list.sort(key=lambda x: x.score)

    init = count * page
    last = init + count
    if last > len(raw_paragraph_list):
        last = len(raw_paragraph_list)

    paragraph_list = raw_paragraph_list[init:last]
    # Filter the resources that are matching the length

    for paragraph in paragraph_list:
        if paragraph.rid not in resources:
            resources.append(paragraph.rid)

    return Paragraphs(results=paragraph_list, facets=facets)


async def merge_results(
    results: List[SearchResponse],
    count: int,
    page: int,
    kbid: str,
    show: List[ResourceProperties],
    field_type_filter: List[FieldTypeName],
    extracted: List[ExtractedDataTypeName],
    max_score: float = 0.85,
) -> KnowledgeboxSearchResults:
    paragraphs = []
    documents = []
    vectors = []

    for result in results:
        paragraphs.append(result.paragraph)
        documents.append(result.document)
        vectors.append(result.vector)

    api_results = KnowledgeboxSearchResults()

    get_resource_cache(clear=True)

    resources: List[str] = list()
    api_results.fulltext = await merge_documents_results(
        documents, resources, count, page
    )

    api_results.paragraphs = await merge_paragraph_results(
        paragraphs, resources, kbid, count, page
    )

    api_results.sentences = await merge_vectors_results(
        vectors, resources, kbid, count, page, max_score=max_score
    )

    api_results.resources = await fetch_resources(
        resources, kbid, show, field_type_filter, extracted
    )
    return api_results


async def merge_paragraphs_results(
    results: List[ParagraphSearchResponse],
    count: int,
    page: int,
    kbid: str,
    show: List[ResourceProperties],
    field_type_filter: List[FieldTypeName],
    extracted: List[ExtractedDataTypeName],
) -> ResourceSearchResults:
    paragraphs = []
    for result in results:
        paragraphs.append(result)

    api_results = ResourceSearchResults()

    resources: List[str] = list()
    api_results.paragraphs = await merge_paragraph_results(
        paragraphs, resources, kbid, count, page
    )
    return api_results


async def merge_suggest_results(
    results: List[SuggestResponse],
    kbid: str,
    show: List[ResourceProperties],
    field_type_filter: List[FieldTypeName],
) -> KnowledgeboxSuggestResults:

    api_results = KnowledgeboxSuggestResults()

    api_results.paragraphs = await merge_suggest_paragraph_results(results, kbid)
    return api_results
