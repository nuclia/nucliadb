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

import asyncio

import pytest
from httpx import AsyncClient

from nucliadb.ingest.tests.fixtures import IngestFixture
from nucliadb_protos import knowledgebox_pb2, writer_pb2, writer_pb2_grpc
from nucliadb_telemetry.settings import telemetry_settings
from nucliadb_telemetry.utils import get_telemetry, init_telemetry


@pytest.mark.asyncio
async def test_create_knowledgebox(
    set_telemetry_settings, grpc_servicer: IngestFixture
):
    tracer_provider = get_telemetry("GCS_SERVICE")
    assert tracer_provider is not None
    await init_telemetry(tracer_provider)

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

    await tracer_provider.force_flush()

    expected_spans = 6

    client = AsyncClient()
    for _ in range(10):
        resp = await client.get(
            f"http://localhost:{telemetry_settings.jaeger_query_port}/api/traces?service=GCS_SERVICE",
            headers={"Accept": "application/json"},
        )
        if resp.status_code != 200 or len(resp.json()["data"]) < expected_spans:
            await asyncio.sleep(2)
        else:
            break
    assert len(resp.json()["data"]) == expected_spans


@pytest.mark.asyncio
async def test_get_resource_id(grpc_servicer):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)

    pb = knowledgebox_pb2.KnowledgeBoxNew(slug="test")
    pb.config.title = "My Title"
    result: knowledgebox_pb2.NewKnowledgeBoxResponse = await stub.NewKnowledgeBox(pb)

    pb = writer_pb2.ResourceIdRequest(kbid="foo", slug="bar")
    result = await stub.GetResourceId(pb)
    assert result.uuid == ""
