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

import pytest

from nucliadb_ingest.tests.fixtures import IngestFixture
from nucliadb_protos import knowledgebox_pb2, writer_pb2_grpc


@pytest.mark.asyncio
async def test_create_knowledgebox(grpc_servicer: IngestFixture):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)
    pb_prefix = knowledgebox_pb2.KnowledgeBoxPrefix(prefix="")

    count = 0
    async for _ in stub.ListKnowledgeBox(pb_prefix):  # type: ignore
        count += 1
    assert count == 0

    pb = knowledgebox_pb2.KnowledgeBoxNew(slug="test")
    pb.config.title = "My Title"
    result = await stub.NewKnowledgeBox(pb)  # type: ignore
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

    pb = knowledgebox_pb2.KnowledgeBoxNew(
        slug="test",
    )
    pb.config.title = "My Title 2"
    result = await stub.NewKnowledgeBox(pb)  # type: ignore
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.CONFLICT

    pb = knowledgebox_pb2.KnowledgeBoxNew(slug="test2")
    result = await stub.NewKnowledgeBox(pb)  # type: ignore
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

    pb_prefix = knowledgebox_pb2.KnowledgeBoxPrefix(prefix="test")
    slugs = []
    async for kpb in stub.ListKnowledgeBox(pb_prefix):  # type: ignore
        slugs.append(kpb.slug)

    assert "test" in slugs
    assert "test2" in slugs

    pbid = knowledgebox_pb2.KnowledgeBoxID(slug="test")
    result = await stub.DeleteKnowledgeBox(pbid)  # type: ignore
