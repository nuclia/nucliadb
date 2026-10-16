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
from contextlib import asynccontextmanager
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from nidx_protos.nodereader_pb2 import SearchRequest, SearchResponse

from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.marklogic import documents
from nucliadb.search.search.backend import _search_chunks, _search_hybrid


@pytest.mark.parametrize("search", [_search_chunks, _search_hybrid])
async def test_fuzzy_chunks(marklogic_maindb_driver, search):
    async with marklogic_maindb_driver.rw_transaction() as txn:
        resource_id = uuid4().hex
        for index, text in enumerate(["My Title", "My totle"]):
            await documents.write(
                txn,
                f"/tests/fuzzy/{resource_id}/{index}.json",
                MarkLogicCollections.CHUNKS,
                {
                    "resource_id": resource_id,
                    "chunk_id": f"{resource_id}/a/title/{index}-{index + 1}",
                    "text": text,
                },
            )

        @asynccontextmanager
        async def transaction(**kwargs):
            yield txn

        driver = MagicMock()
        driver.ro_transaction = transaction
        for body, expected_fuzzy in [("totle", [False, True]), ('"totle"', [False])]:
            query = SearchRequest(body=body, paragraph=True)
            query.field_filter.resource.resource_id = resource_id
            response = SearchResponse()
            await search(driver, "test", query, response)
            assert [bool(result.matches) for result in response.paragraph.results] == expected_fuzzy
            if len(response.paragraph.results) == 2:
                assert (
                    response.paragraph.results[0].score.bm25 > response.paragraph.results[1].score.bm25
                )
