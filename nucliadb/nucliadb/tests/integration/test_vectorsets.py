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

import uuid

import pytest
from httpx import AsyncClient
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.common.cluster import manager
from nucliadb.tests.utils import inject_message
from nucliadb_protos import nodereader_pb2, writer_pb2

DEFAULT_VECTOR_DIMENSION = 512
VECTORSET_DIMENSION = 12


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_vectorsets(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox: str,
):
    kbid = knowledgebox
    vectorset_id = "my-vectorset"

    await create_vectorset(nucliadb_grpc, kbid, vectorset_id)
    await inject_broker_message_with_vectorset_data(nucliadb_grpc, kbid, vectorset_id)

    # TODO: change this to use search REST API when implemented
    shards = await manager.KBShardManager().get_shards_by_kbid(kbid)
    logic_shard = shards[0]
    node, shard_id = manager.choose_node(logic_shard)

    test_cases = [
        (DEFAULT_VECTOR_DIMENSION, ""),
        (VECTORSET_DIMENSION, vectorset_id),
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
        (DEFAULT_VECTOR_DIMENSION, vectorset_id),
        (VECTORSET_DIMENSION, ""),
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


async def create_vectorset(nucliadb_grpc: WriterStub, kbid: str, vectorset_id: str):
    response = await nucliadb_grpc.NewVectorSet(
        writer_pb2.NewVectorSetRequest(
            kbid=kbid,
            vectorset_id=vectorset_id,
            vector_dimension=VECTORSET_DIMENSION,
        )
    )  # type: ignore
    assert response.status == response.Status.OK


async def inject_broker_message_with_vectorset_data(
    nucliadb_grpc: WriterStub, kbid: str, vectorset_id: str
):
    from nucliadb.ingest.tests.integration.ingest.test_vectorsets import (
        create_broker_message_with_vectorset,
    )

    bm = create_broker_message_with_vectorset(
        kbid,
        rid=uuid.uuid4().hex,
        field_id=uuid.uuid4().hex,
        vectorset_id=vectorset_id,
        default_vectorset_dimension=DEFAULT_VECTOR_DIMENSION,
        vectorset_dimension=VECTORSET_DIMENSION,
    )
    await inject_message(nucliadb_grpc, bm)
