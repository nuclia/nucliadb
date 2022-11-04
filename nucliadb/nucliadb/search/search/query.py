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
from datetime import datetime
from typing import List, Optional

from nucliadb_protos.nodereader_pb2 import (
    ParagraphSearchRequest,
    SearchRequest,
    SuggestRequest,
)

from nucliadb.search.api.models import SearchOptions, Sort, SuggestOptions
from nucliadb.search.utilities import get_predict


async def global_query_to_pb(
    kbid: str,
    features: List[SearchOptions],
    query: str,
    filters: List[str],
    faceted: List[str],
    page_number: int,
    page_size: int,
    range_creation_start: Optional[datetime] = None,
    range_creation_end: Optional[datetime] = None,
    range_modification_start: Optional[datetime] = None,
    range_modification_end: Optional[datetime] = None,
    fields: Optional[List[str]] = None,
    sort: Optional[str] = None,
    sort_ord: int = Sort.ASC.value,
    reload: bool = False,
    vector: Optional[List[float]] = None,
    with_duplicates: bool = False,
) -> SearchRequest:
    fields = fields or []

    predict = get_predict()

    request = SearchRequest()
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

    if SearchOptions.DOCUMENT in features or SearchOptions.PARAGRAPH in features:
        request.body = query
        request.filter.tags.extend(filters)
        request.faceted.tags.extend(faceted)
        if sort:
            request.order.field = sort
            request.order.type = sort_ord  # type: ignore
        request.fields.extend(fields)

    request.document = SearchOptions.DOCUMENT in features
    request.paragraph = SearchOptions.PARAGRAPH in features

    if SearchOptions.VECTOR in features:
        if vector is None:
            request.vector.extend(await predict.convert_sentence_to_vector(kbid, query))
        else:
            request.vector.extend(vector)

    if SearchOptions.RELATIONS in features:
        pass

    return request


async def suggest_query_to_pb(
    features: List[SuggestOptions],
    query: str,
    filters: List[str],
    faceted: List[str],
    range_creation_start: Optional[datetime] = None,
    range_creation_end: Optional[datetime] = None,
    range_modification_start: Optional[datetime] = None,
    range_modification_end: Optional[datetime] = None,
    fields: Optional[List[str]] = None,
) -> SuggestRequest:

    request = SuggestRequest()
    if SuggestOptions.PARAGRAPH in features:
        request.body = query
        request.filter.tags.extend(filters)
    return request


async def paragraph_query_to_pb(
    features: List[SearchOptions],
    rid: str,
    query: str,
    filters: List[str],
    faceted: List[str],
    page_number: int,
    page_size: int,
    range_creation_start: Optional[datetime] = None,
    range_creation_end: Optional[datetime] = None,
    range_modification_start: Optional[datetime] = None,
    range_modification_end: Optional[datetime] = None,
    fields: Optional[List[str]] = None,
    sort: Optional[str] = None,
    sort_ord: int = Sort.ASC.value,
    reload: bool = False,
    with_duplicates: bool = False,
) -> ParagraphSearchRequest:
    fields = fields or []

    request = ParagraphSearchRequest()
    request.reload = reload
    request.with_duplicates = with_duplicates
    if SearchOptions.PARAGRAPH in features:
        request.uuid = rid
        request.body = query
        request.filter.tags.extend(filters)
        request.faceted.tags.extend(faceted)
        if sort:
            request.order.field = sort
            request.order.type = sort_ord  # type: ignore
        request.page_number = page_number
        request.result_per_page = page_size
        request.fields.extend(fields)

    # if SearchOptions.VECTOR in features:
    #     request.vector = await predict.convert_sentence_to_vector(query)

    if SearchOptions.RELATIONS in features:
        pass
    return request
