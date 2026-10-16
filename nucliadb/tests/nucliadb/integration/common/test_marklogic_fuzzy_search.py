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
