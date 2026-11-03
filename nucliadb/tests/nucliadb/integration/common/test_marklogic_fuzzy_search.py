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
from uuid import uuid4

import pytest
from nidx_protos.nodereader_pb2 import SearchRequest, SearchResponse

from nucliadb.common.datamanagers import fields as field_datamanager
from nucliadb.common.maindb.marklogic import MarkLogicDriver
from nucliadb.common.marklogic import documents
from nucliadb.search.search import backend


@pytest.mark.parametrize("semantic", [False, True])
async def test_fuzzy_chunks(marklogic_maindb_driver: MarkLogicDriver, semantic: bool):
    kbid = uuid4().hex
    await marklogic_maindb_driver.ensure_kb_database(kbid)
    resource_id = uuid4().hex
    try:
        async with marklogic_maindb_driver.rw_transaction(kbid=kbid) as txn:
            await field_datamanager.create_processed_documents(
                txn,
                field=field_datamanager.FieldProcessedContent(
                    resource_id=resource_id,
                    field_id="title",
                    field_type="a",
                    extracted_text="My Title My totle",
                    facets=[],
                ),
                chunks=[
                    field_datamanager.ChunkContent(
                        resource_id=resource_id,
                        field_id="title",
                        field_type="a",
                        chunk_id=f"{resource_id}/a/title/{index}-{index + 1}",
                        text=text,
                        facets=[],
                        data={},
                    )
                    for index, text in enumerate(["My Title", "My totle"])
                ],
                embeddings=[],
            )
            await txn.commit()

        # Like nidx, fuzzy matching is a fallback for queries without keyword matches and min score 0
        for body, min_score, expected_fuzzy in [
            ("totle", 0.0, [False]),
            ("tutle", 0.0, [True, True]),
            ('"tutle"', 0.0, []),
            ("tutle", 1.0, []),
            ("totle", 1.0, [False]),
            ("totle", 1e12, []),
        ]:
            query = SearchRequest(
                body=body, paragraph=True, result_per_page=10, min_score_bm25=min_score
            )
            query.field_filter.resource.resource_id = resource_id
            if semantic:
                query.vector.extend([1.0, 0.0, 0.0])
                query.vectorset = "my-model"
            javascript = backend._search_javascript(query, lexical=True, semantic=semantic)
            async with marklogic_maindb_driver.ro_transaction(kbid=kbid) as txn:
                result = backend._eval_object(await documents.evaluate(txn, javascript))
            response = SearchResponse()
            backend._append_chunk_results(
                query, response, backend._flatten_eval_array(result.get("chunks", []))
            )
            results = response.paragraph.results
            assert [bool(result.matches) for result in results] == expected_fuzzy, (body, min_score)
            assert all(result.score.bm25 > 0 for result in results)
            assert all(result.score.bm25 >= min_score for result in results)
            scores = [result.score.bm25 for result in results]
            assert scores == sorted(scores, reverse=True)
    finally:
        await marklogic_maindb_driver.delete_kb_database(kbid)
