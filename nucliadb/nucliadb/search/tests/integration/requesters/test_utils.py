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

from typing import Callable

import pytest
from httpx import AsyncClient

from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search.query import global_query_to_pb
from nucliadb_models.search import SearchOptions, SortField, SortOptions, SortOrder


@pytest.mark.asyncio
@pytest.mark.xfail  # pulling start/end position for vectors results needs to be fixed
async def test_vector_result_metadata(
    search_api: Callable[..., AsyncClient], multiple_search_resource: str
) -> None:
    kbid = multiple_search_resource

    pb_query, _ = await global_query_to_pb(
        kbid,
        query="own text",
        features=[SearchOptions.VECTOR],
        filters=[],
        faceted=[],
        page_number=0,
        page_size=20,
        sort=SortOptions(
            field=SortField.SCORE,
            order=SortOrder.DESC,
            limit=None,
        ),
        min_score=-1,
    )

    results, _, _, _ = await node_query(kbid, Method.SEARCH, pb_query)
    assert len(results[0].vector.documents) > 0
    assert results[0].vector.documents[0].HasField("metadata")
