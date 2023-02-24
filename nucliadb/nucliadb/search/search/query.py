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
import re
from datetime import datetime
from typing import List, Optional, Tuple

from nucliadb_protos.nodereader_pb2 import (
    ParagraphSearchRequest,
    SearchRequest,
    SuggestRequest,
)
from nucliadb_protos.noderesources_pb2 import Resource

from nucliadb.search import logger
from nucliadb.search.predict import PredictVectorMissing, SendToPredictError
from nucliadb.search.utilities import get_predict
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_models.search import (
    SearchOptions,
    Sort,
    SortFieldMap,
    SortOptions,
    SuggestOptions,
)

REMOVABLE_CHARS = re.compile(r"\¿|\?|\!|\¡|\,|\;|\.|\:")


async def global_query_to_pb(
    kbid: str,
    features: List[SearchOptions],
    query: str,
    filters: List[str],
    faceted: List[str],
    page_number: int,
    page_size: int,
    sort: SortOptions,
    advanced_query: Optional[str] = None,
    range_creation_start: Optional[datetime] = None,
    range_creation_end: Optional[datetime] = None,
    range_modification_start: Optional[datetime] = None,
    range_modification_end: Optional[datetime] = None,
    fields: Optional[List[str]] = None,
    sort_ord: int = Sort.ASC.value,
    reload: bool = False,
    user_vector: Optional[List[float]] = None,
    vectorset: Optional[str] = None,
    with_duplicates: bool = False,
    with_status: Optional[ResourceProcessingStatus] = None,
) -> Tuple[SearchRequest, bool]:
    fields = fields or []

    request = SearchRequest()
    request.reload = reload
    request.with_duplicates = with_duplicates

    if with_status is not None:
        request.with_status = PROCESSING_STATUS_TO_PB_MAP[with_status]

    # We need to ask for all and cut later
    request.page_number = 0
    if sort.limit is not None:
        # As the index can't sort, we have to do it when merging. To
        # have consistent results, we must limit them
        request.result_per_page = sort.limit
    else:
        request.result_per_page = page_number * page_size + page_size

    if range_creation_start is not None:
        request.timestamps.from_created.FromDatetime(range_creation_start)

    if range_creation_end is not None:
        request.timestamps.to_created.FromDatetime(range_creation_end)

    if range_modification_start is not None:
        request.timestamps.from_modified.FromDatetime(range_modification_start)

    if range_modification_end is not None:
        request.timestamps.to_modified.FromDatetime(range_modification_end)

    if SearchOptions.DOCUMENT in features or SearchOptions.PARAGRAPH in features:
        request.body = query
        if advanced_query is not None:
            request.advanced_query = advanced_query
        request.filter.tags.extend(filters)
        request.faceted.tags.extend(faceted)

        sort_field = SortFieldMap[sort.field]
        if sort_field is not None:
            request.order.sort_by = sort_field
            request.order.type = sort_ord  # type: ignore

        request.fields.extend(fields)

    request.document = SearchOptions.DOCUMENT in features
    request.paragraph = SearchOptions.PARAGRAPH in features

    incomplete = False
    if SearchOptions.VECTOR in features:
        incomplete = await _parse_vectors(
            request, kbid, query, user_vector=user_vector, vectorset=vectorset
        )

    if SearchOptions.RELATIONS in features:
        await _parse_entities(request, kbid, query)

    return request, incomplete


async def _parse_vectors(
    request: SearchRequest,
    kbid: str,
    query: str,
    user_vector: Optional[List[float]],
    vectorset: Optional[str],
) -> bool:
    incomplete = False
    if vectorset is not None:
        request.vectorset = vectorset
    if user_vector is None:
        predict = get_predict()
        try:
            predict_vector = await predict.convert_sentence_to_vector(kbid, query)
            request.vector.extend(predict_vector)
        except SendToPredictError as err:
            logger.warning(f"Errors on predict api trying to embedd query: {err}")
            incomplete = True
        except PredictVectorMissing:
            logger.warning("Predict api returned an empty vector")
            incomplete = True
    else:
        request.vector.extend(user_vector)
    return incomplete


async def _parse_entities(request: SearchRequest, kbid: str, query: str):
    predict = get_predict()
    try:
        detected_entities = await predict.detect_entities(kbid, query)
        request.relations.subgraph.entry_points.extend(detected_entities)
        request.relations.subgraph.depth = 1
    except SendToPredictError as ex:
        logger.warning(f"Errors on predict api detecting entities: {ex}")


async def suggest_query_to_pb(
    features: List[SuggestOptions],
    query: str,
    fields: List[str],
    filters: List[str],
    faceted: List[str],
    range_creation_start: Optional[datetime] = None,
    range_creation_end: Optional[datetime] = None,
    range_modification_start: Optional[datetime] = None,
    range_modification_end: Optional[datetime] = None,
) -> SuggestRequest:
    request = SuggestRequest()
    if SuggestOptions.PARAGRAPH in features:
        request.body = query
        request.filter.tags.extend(filters)
        request.fields.extend(fields)

    if range_creation_start is not None:
        request.timestamps.from_created.FromDatetime(range_creation_start)
    if range_creation_end is not None:
        request.timestamps.to_created.FromDatetime(range_creation_end)
    if range_modification_start is not None:
        request.timestamps.from_modified.FromDatetime(range_modification_start)
    if range_modification_end is not None:
        request.timestamps.to_modified.FromDatetime(range_modification_end)

    return request


async def paragraph_query_to_pb(
    features: List[SearchOptions],
    rid: str,
    query: str,
    fields: List[str],
    filters: List[str],
    faceted: List[str],
    page_number: int,
    page_size: int,
    range_creation_start: Optional[datetime] = None,
    range_creation_end: Optional[datetime] = None,
    range_modification_start: Optional[datetime] = None,
    range_modification_end: Optional[datetime] = None,
    sort: Optional[str] = None,
    sort_ord: int = Sort.ASC.value,
    reload: bool = False,
    with_duplicates: bool = False,
) -> ParagraphSearchRequest:
    request = ParagraphSearchRequest()
    request.reload = reload
    request.with_duplicates = with_duplicates

    # We need to ask for all and cut later
    request.page_number = 0
    request.result_per_page = page_number * page_size + page_size

    if range_creation_start is not None:
        request.timestamps.from_created.FromDatetime(range_creation_start)

    if range_creation_end is not None:
        request.timestamps.to_created.FromDatetime(range_creation_end)

    if range_modification_start is not None:
        request.timestamps.from_modified.FromDatetime(range_modification_start)

    if range_modification_end is not None:
        request.timestamps.to_modified.FromDatetime(range_modification_end)

    if SearchOptions.PARAGRAPH in features:
        request.uuid = rid
        request.body = query
        request.filter.tags.extend(filters)
        request.faceted.tags.extend(faceted)
        if sort:
            request.order.field = sort
            request.order.type = sort_ord  # type: ignore
        request.fields.extend(fields)

    return request


PROCESSING_STATUS_TO_PB_MAP = {
    ResourceProcessingStatus.PENDING: Resource.ResourceStatus.PENDING,
    ResourceProcessingStatus.PROCESSED: Resource.ResourceStatus.PROCESSED,
    ResourceProcessingStatus.ERROR: Resource.ResourceStatus.ERROR,
}


def pre_process_query(user_query: str) -> str:
    # NOTE: if this logic grows in the future, consider using a Strategy pattern.
    user_terms = user_query.split()
    result = []
    in_quote = False
    for term in user_terms:
        term = term.strip()
        if in_quote:
            result.append(term)
            continue

        if term.startswith('"'):
            in_quote = True
            result.append(term)
            continue

        if term.endswith('"'):
            in_quote = False

        term = REMOVABLE_CHARS.sub("", term)
        term = term.strip()
        if len(term):
            result.append(term)

    return " ".join(result)
