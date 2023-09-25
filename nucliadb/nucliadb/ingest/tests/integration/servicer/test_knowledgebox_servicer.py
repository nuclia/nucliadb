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
from nucliadb_protos.writer_pb2 import Shards as PBShards

from nucliadb.common.maindb.local import LocalDriver
from nucliadb.ingest.tests.fixtures import IngestFixture
from nucliadb_protos import knowledgebox_pb2, utils_pb2, writer_pb2, writer_pb2_grpc
from nucliadb_telemetry.settings import telemetry_settings
from nucliadb_telemetry.utils import get_telemetry
from nucliadb_utils.keys import KB_SHARDS


@pytest.mark.asyncio
async def test_create_knowledgebox(
    set_telemetry_settings, grpc_servicer: IngestFixture, maindb_driver
):
    if isinstance(maindb_driver, LocalDriver):
        pytest.skip("There is a bug in the local driver that needs to be fixed")

    tracer_provider = get_telemetry("GCS_SERVICE")
    assert tracer_provider is not None

    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)  # type: ignore
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

    await tracer_provider.async_force_flush()

    client = AsyncClient()
    for _ in range(10):
        resp = await client.get(
            f"http://localhost:{telemetry_settings.jaeger_query_port}/api/traces?service=GCS_SERVICE",
            headers={"Accept": "application/json"},
        )
        if resp.status_code != 200:
            print(f"Error getting traces: {resp.text}")
        if resp.status_code != 200 or len(resp.json()["data"]) < 0:
            await asyncio.sleep(2)
        else:
            break

    assert len(resp.json()["data"]) > 0


async def get_kb_similarity(txn, kbid) -> utils_pb2.VectorSimilarity.ValueType:
    kb_shards_key = KB_SHARDS.format(kbid=kbid)
    kb_shards_binary = await txn.get(kb_shards_key)
    assert kb_shards_binary, "Shards object not found!"
    kb_shards = PBShards()
    kb_shards.ParseFromString(kb_shards_binary)
    return kb_shards.similarity


@pytest.mark.asyncio
async def test_create_knowledgebox_with_similarity(grpc_servicer: IngestFixture):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)  # type: ignore
    pb = knowledgebox_pb2.KnowledgeBoxNew(slug="test-dot")
    pb.config.title = "My Title"
    pb.similarity = utils_pb2.VectorSimilarity.DOT
    result = await stub.NewKnowledgeBox(pb)  # type: ignore
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

    async with grpc_servicer.servicer.driver.transaction() as txn:
        assert (
            await get_kb_similarity(txn, result.uuid) == utils_pb2.VectorSimilarity.DOT
        )


@pytest.mark.asyncio
async def test_create_knowledgebox_defaults_to_cosine_similarity(
    grpc_servicer: IngestFixture,
    txn,
):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)  # type: ignore
    pb = knowledgebox_pb2.KnowledgeBoxNew(slug="test-default")
    pb.config.title = "My Title"
    result = await stub.NewKnowledgeBox(pb)  # type: ignore
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

    async with grpc_servicer.servicer.driver.transaction() as txn:
        assert (
            await get_kb_similarity(txn, result.uuid)
            == utils_pb2.VectorSimilarity.COSINE
        )


@pytest.mark.asyncio
async def test_get_resource_id(grpc_servicer: IngestFixture) -> None:
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)  # type: ignore

    pb = knowledgebox_pb2.KnowledgeBoxNew(slug="test")
    pb.config.title = "My Title"
    result: knowledgebox_pb2.NewKnowledgeBoxResponse = await stub.NewKnowledgeBox(pb)  # type: ignore

    pbid = writer_pb2.ResourceIdRequest(kbid="foo", slug="bar")
    result = await stub.GetResourceId(pbid)  # type: ignore
    assert result.uuid == ""


@pytest.mark.asyncio
async def test_delete_knowledgebox_handles_unexisting_kb(
    grpc_servicer: IngestFixture,
) -> None:
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)  # type: ignore

    pbid = knowledgebox_pb2.KnowledgeBoxID(slug="idonotexist")
    result = await stub.DeleteKnowledgeBox(pbid)  # type: ignore
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

    pbid = knowledgebox_pb2.KnowledgeBoxID(uuid="idonotexist")
    result = await stub.DeleteKnowledgeBox(pbid)  # type: ignore
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

    pbid = knowledgebox_pb2.KnowledgeBoxID(uuid="idonotexist", slug="meneither")
    result = await stub.DeleteKnowledgeBox(pbid)  # type: ignore
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK
