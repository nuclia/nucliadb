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

from nucliadb_search.api.models import SearchOptions, Sort, SuggestOptions
from nucliadb_search.utilities import get_predict


async def global_query_to_pb(
    features: List[SearchOptions],
    query: str,
    filters: List[str],
    faceted: List[str],
    sort: str,
    page_number: int,
    page_size: int,
    range_creation_start: Optional[datetime] = None,
    range_creation_end: Optional[datetime] = None,
    range_modification_start: Optional[datetime] = None,
    range_modification_end: Optional[datetime] = None,
    fields: List[str] = [],
    sort_ord: int = Sort.ASC.value,
    reload: bool = False,
) -> SearchRequest:

    predict = get_predict()

    request = SearchRequest()
    request.reload = reload
    if SearchOptions.DOCUMENT in features or SearchOptions.PARAGRAPH in features:
        request.body = query
        request.filter.tags.extend(filters)
        request.faceted.tags.extend(faceted)
        if sort:
            request.order.field = sort
            request.order.type = sort_ord  # type: ignore
        request.page_number = 0
        request.result_per_page = page_size * page_number
        request.fields.extend(fields)

    if SearchOptions.VECTOR in features:
        request.vector.extend(await predict.convert_sentence_to_vector(query))

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
    fields: List[str] = [],
) -> SuggestRequest:

    request = SuggestRequest()
    request.reload = False
    if SuggestOptions.PARAGRAPH in features:
        request.body = query
        request.filter.tags.extend(filters)
        request.faceted.tags.extend(faceted)
        request.page_number = 0
        request.result_per_page = 10
        request.fields.extend(fields)
    return request


async def paragraph_query_to_pb(
    features: List[SearchOptions],
    rid: str,
    query: str,
    filters: List[str],
    faceted: List[str],
    sort: str,
    page_number: int,
    page_size: int,
    range_creation_start: Optional[datetime] = None,
    range_creation_end: Optional[datetime] = None,
    range_modification_start: Optional[datetime] = None,
    range_modification_end: Optional[datetime] = None,
    fields: List[str] = [],
    sort_ord: int = Sort.ASC.value,
    reload: bool = False,
) -> ParagraphSearchRequest:

    # predict = get_predict()

    request = ParagraphSearchRequest()
    request.reload = reload
    if SearchOptions.PARAGRAPH in features:
        request.uuid = rid
        request.body = query
        request.filter.tags.extend(filters)
        request.faceted.tags.extend(faceted)
        if sort:
            request.order.field = sort
            request.order.type = sort_ord  # type: ignore
        request.page_number = 0
        request.result_per_page = page_size * page_number
        request.fields.extend(fields)

    # if SearchOptions.VECTOR in features:
    #     request.vector = await predict.convert_sentence_to_vector(query)

    if SearchOptions.RELATIONS in features:
        pass
    return request
