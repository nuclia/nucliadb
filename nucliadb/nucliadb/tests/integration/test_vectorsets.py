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

from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.common.cluster import manager
from nucliadb.tests.knowledgeboxes.vectorsets import KbSpecs
from nucliadb_protos import nodereader_pb2


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_vectorsets(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    kb_with_vectorset: KbSpecs,
):
    kbid = kb_with_vectorset.kbid
    vectorset_id = kb_with_vectorset.vectorset_id
    default_vector_dimension = kb_with_vectorset.default_vector_dimension
    vectorset_dimension = kb_with_vectorset.vectorset_dimension

    shards = await manager.KBShardManager().get_shards_by_kbid(kbid)
    logic_shard = shards[0]
    node, shard_id = manager.choose_node(logic_shard)

    test_cases = [
        (default_vector_dimension, ""),
        (vectorset_dimension, vectorset_id),
    ]
    for dimension, vectorset in test_cases:
        query_pb = nodereader_pb2.SearchRequest(
            shard=shard_id,
            body="this is a query for my vectorset",
            vector=[1.23] * dimension,
            vectorset=vectorset,
            result_per_page=5,
        )
        results = await node.reader.Search(query_pb)  # type: ignore
        assert len(results.vector.documents) == 5

    test_cases = [
        (default_vector_dimension, vectorset_id),
        (vectorset_dimension, ""),
    ]
    for dimension, vectorset in test_cases:
        query_pb = nodereader_pb2.SearchRequest(
            shard=shard_id,
            body="this is a query for my vectorset",
            vector=[1.23] * dimension,
            vectorset=vectorset,
            result_per_page=5,
        )
        with pytest.raises(Exception) as exc:
            results = await node.reader.Search(query_pb)  # type: ignore
        assert "inconsistent dimensions" in str(exc).lower()


@pytest.mark.parametrize(
    "vectorset,expected",
    [(None, ""), ("", ""), ("myvectorset", "myvectorset")],
)
@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_vectorset_parameter(
    nucliadb_reader: AsyncClient,
    knowledgebox: str,
    vectorset,
    expected,
):
    kbid = knowledgebox

    calls: list[nodereader_pb2.SearchRequest] = []

    async def mock_node_query(
        kbid: str, method, pb_query: nodereader_pb2.SearchRequest, **kwargs
    ):
        calls.append(pb_query)
        results = [nodereader_pb2.SearchResponse()]
        incomplete_results = False
        queried_nodes = []  # type: ignore
        return (results, incomplete_results, queried_nodes)

    with (
        patch(
            "nucliadb.search.api.v1.search.node_query",
            new=AsyncMock(side_effect=mock_node_query),
        ),
        patch(
            "nucliadb.search.search.find.node_query",
            new=AsyncMock(side_effect=mock_node_query),
        ),
    ):
        resp = await nucliadb_reader.get(
            f"/kb/{kbid}/search",
            params={"query": "foo", "vectorset": vectorset},
        )
        assert resp.status_code == 200
        assert calls[-1].vectorset == expected

        resp = await nucliadb_reader.get(
            f"/kb/{kbid}/find",
            params={
                "query": "foo",
                "vectorset": vectorset,
            },
        )
        assert resp.status_code == 200
        assert calls[-1].vectorset == expected
