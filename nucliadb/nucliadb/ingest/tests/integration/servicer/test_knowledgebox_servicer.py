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

from nucliadb.common import datamanagers
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.local import LocalDriver
from nucliadb.ingest.tests.fixtures import IngestFixture
from nucliadb_protos import knowledgebox_pb2, utils_pb2, writer_pb2, writer_pb2_grpc


@pytest.mark.asyncio
async def test_create_knowledgebox(grpc_servicer: IngestFixture, maindb_driver):
    if isinstance(maindb_driver, LocalDriver):
        pytest.skip("There is a bug in the local driver that needs to be fixed")

    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)  # type: ignore

    kbs = await list_all_kb_slugs(maindb_driver)
    assert len(kbs) == 0

    pb = knowledgebox_pb2.KnowledgeBoxNew(slug="test")
    pb.config.title = "My Title"
    result = await stub.NewKnowledgeBox(pb)  # type: ignore
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

    pb = knowledgebox_pb2.KnowledgeBoxNew(slug="test")
    pb.config.title = "My Title 2"
    result = await stub.NewKnowledgeBox(pb)  # type: ignore
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.CONFLICT

    pb = knowledgebox_pb2.KnowledgeBoxNew(slug="test2")
    result = await stub.NewKnowledgeBox(pb)  # type: ignore
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

    slugs = await list_all_kb_slugs(maindb_driver)
    assert "test" in slugs
    assert "test2" in slugs

    pbid = knowledgebox_pb2.KnowledgeBoxID(slug="test")
    result = await stub.DeleteKnowledgeBox(pbid)  # type: ignore


async def list_all_kb_slugs(driver: Driver) -> list[str]:
    slugs = []
    async with driver.transaction(read_only=True) as txn:
        async for _, slug in datamanagers.kb.get_kbs(txn):
            slugs.append(slug)
    return slugs


async def get_kb_config(txn, kbid) -> knowledgebox_pb2.KnowledgeBoxConfig:
    config = await datamanagers.kb.get_config(txn, kbid=kbid)
    assert config, "Config object not found!"
    return config


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
        shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
        assert shards is not None
        config = await get_kb_config(txn, kbid)
        assert shards.release_channel == config.release_channel == release_channel
