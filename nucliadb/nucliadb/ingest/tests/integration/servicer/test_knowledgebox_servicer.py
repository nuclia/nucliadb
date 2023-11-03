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
from nucliadb_protos.writer_pb2 import Shards as PBShards

from nucliadb.common.maindb.local import LocalDriver
from nucliadb.ingest.tests.fixtures import IngestFixture
from nucliadb_protos import knowledgebox_pb2, utils_pb2, writer_pb2, writer_pb2_grpc
from nucliadb_utils.keys import KB_SHARDS, KB_UUID


@pytest.mark.asyncio
async def test_create_knowledgebox(grpc_servicer: IngestFixture, maindb_driver):
    if isinstance(maindb_driver, LocalDriver):
        pytest.skip("There is a bug in the local driver that needs to be fixed")

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


async def get_kb_shards(txn, kbid) -> PBShards:
    kb_shards_key = KB_SHARDS.format(kbid=kbid)
    kb_shards_binary = await txn.get(kb_shards_key)
    assert kb_shards_binary, "Shards object not found!"
    kb_shards = PBShards()
    kb_shards.ParseFromString(kb_shards_binary)
    return kb_shards


async def get_kb_config(txn, kbid) -> knowledgebox_pb2.KnowledgeBoxConfig():
    key = KB_UUID.format(kbid=kbid)
    binary = await txn.get(key)
    assert binary, "Config object not found!"
    config = knowledgebox_pb2.KnowledgeBoxConfig()
    config.ParseFromString(binary)
    return config


async def get_kb_similarity(txn, kbid) -> utils_pb2.VectorSimilarity.ValueType:
    return await get_kb_shards(txn, kbid).similarity


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "release_channel",
    [utils_pb2.ReleaseChannel.EXPERIMENTAL, utils_pb2.ReleaseChannel.STABLE],
)
async def test_create_knowledgebox_release_channel(
    grpc_servicer: IngestFixture,
    release_channel,
):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)  # type: ignore
    pb = knowledgebox_pb2.KnowledgeBoxNew(
        slug="test-default", release_channel=release_channel
    )
    pb.config.title = "My Title"
    result = await stub.NewKnowledgeBox(pb)  # type: ignore
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK
    kbid = result.uuid

    driver = grpc_servicer.servicer.driver
    async with driver.transaction() as txn:
        shards = await get_kb_shards(txn, kbid)
        config = await get_kb_config(txn, kbid)
        assert shards.release_channel == config.release_channel == release_channel
