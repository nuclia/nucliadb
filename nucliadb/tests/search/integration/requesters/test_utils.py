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


import pytest
from httpx import AsyncClient

from nucliadb.search.requesters.utils import Method, nidx_query
from nucliadb.search.search.query_parser.parsers.search import parse_search
from nucliadb.search.search.query_parser.parsers.unit_retrieval import convert_retrieval_to_proto
from nucliadb_models.search import (
    MinScore,
    SearchOptions,
    SearchRequest,
    SortField,
    SortOptions,
    SortOrder,
)


@pytest.mark.xfail  # pulling start/end position for vectors results needs to be fixed
@pytest.mark.deploy_modes("cluster")
async def test_vector_result_metadata(nucliadb_search: AsyncClient, test_search_resource: str) -> None:
    kbid = test_search_resource

    parsed = await parse_search(
        kbid,
        SearchRequest(
            query="own text",
            features=[SearchOptions.SEMANTIC],
            label_filters=[],  # type: ignore
            keyword_filters=[],  # type: ignore
            faceted=[],
            top_k=20,
            min_score=MinScore(bm25=0, semantic=-1),
            sort=SortOptions(
                field=SortField.SCORE,
                order=SortOrder.DESC,
            ),
        ),
    )
    pb_query = convert_retrieval_to_proto(parsed.retrieval)

    results, _ = await nidx_query(kbid, Method.SEARCH, pb_query)
    assert len(results[0].vector.documents) > 0
    assert results[0].vector.documents[0].HasField("metadata")
