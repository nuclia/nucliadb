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
import os
import tempfile
import time
from typing import AsyncIterable, Optional

import docker  # type: ignore
import pytest
from grpc import aio, insecure_channel  # type: ignore
from grpc_health.v1 import health_pb2_grpc  # type: ignore
from grpc_health.v1.health_pb2 import HealthCheckRequest  # type: ignore
from nucliadb_protos.noderesources_pb2 import EmptyQuery, ShardCreated, ShardId
from nucliadb_protos.nodewriter_pb2 import NewShardRequest
from nucliadb_protos.nodewriter_pb2_grpc import NodeWriterStub
from nucliadb_protos.utils_pb2 import ReleaseChannel
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

from nucliadb_node.app import start_worker
from nucliadb_node.pull import Worker
from nucliadb_node.service import start_grpc
from nucliadb_node.settings import indexing_settings, settings
from nucliadb_node.writer import Writer
from nucliadb_protos import nodereader_pb2, nodereader_pb2_grpc, noderesources_pb2
from nucliadb_utils.cache.settings import settings as cache_settings

images.settings["nucliadb_node_reader"] = {
    "image": "eu.gcr.io/stashify-218417/node",
    "version": "main",
    "env": {
        "NUCLIADB_DISABLE_ANALYTICS": "True",
        "DATA_PATH": "/data",
        "READER_LISTEN_ADDRESS": "0.0.0.0:4445",
        "LAZY_LOADING": "true",
        "RUST_BACKTRACE": "full",
        "RUST_LOG": "nucliadb_*=DEBUG",  # noqa
        "DEBUG": "1",
    },
    "options": {
        "command": [
            "/usr/local/bin/node_reader",
        ],
        "platform": "linux/amd64",
        "mem_limit": "3g",
        "ports": {"4445": None},
    },
}

images.settings["nucliadb_node_writer"] = {
    "image": "eu.gcr.io/stashify-218417/node",
    "version": "main",
    "env": {
        "NUCLIADB_DISABLE_ANALYTICS": "True",
        "DATA_PATH": "/data",
        "WRITER_LISTEN_ADDRESS": "0.0.0.0:4446",
        "RUST_BACKTRACE": "full",
        "RUST_LOG": "nucliadb_*=DEBUG",  # noqa
        "DEBUG": "1",
    },
    "options": {
        "command": [
            "/usr/local/bin/node_writer",
        ],
        "platform": "linux/amd64",
        "mem_limit": "3g",
        "ports": {"4446": None},
    },
}


class nucliadbNodeReader(BaseImage):
    name = "nucliadb_node_reader"
    port = 4445

    def run(self, volume):
        self._volume = volume
        self._mount = "/data"
        return super(nucliadbNodeReader, self).run()

    def get_image_options(self):
        options = super(nucliadbNodeReader, self).get_image_options()
        options["volumes"] = {self._volume.name: {"bind": "/data"}}
        return options

    def check(self):
        channel = insecure_channel(f"{self.host}:{self.get_port()}")
        stub = health_pb2_grpc.HealthStub(channel)
        pb = HealthCheckRequest(service="nodereader.NodeReader")
        try:
            result = stub.Check(pb)
            return result.status == 1
        except Exception:  # pragma: no cover
            return False


class nucliadbNodeWriter(BaseImage):
    name = "nucliadb_node_writer"
    port = 4446

    def run(self, volume):
        self._volume = volume
        self._mount = "/data"
        return super(nucliadbNodeWriter, self).run()

    def get_image_options(self):
        options = super(nucliadbNodeWriter, self).get_image_options()
        options["volumes"] = {self._volume.name: {"bind": "/data"}}
        return options

    def check(self):
        channel = insecure_channel(f"{self.host}:{self.get_port()}")
        stub = health_pb2_grpc.HealthStub(channel)
        pb = HealthCheckRequest(service="nodewriter.NodeWriter")
        try:
            result = stub.Check(pb)
            return result.status == 1
        except Exception:  # pragma: no cover
            return False


nucliadb_node_reader = nucliadbNodeReader()
nucliadb_node_writer = nucliadbNodeWriter()


@pytest.fixture(scope="session")
def node_single():
    docker_client = docker.from_env(version=BaseImage.docker_version)
    volume_node = docker_client.volumes.create(driver="local")

    writer1_host, writer1_port = nucliadb_node_writer.run(volume_node)
    reader1_host, reader1_port = nucliadb_node_reader.run(volume_node)

    settings.writer_listen_address = f"{writer1_host}:{writer1_port}"
    settings.reader_listen_address = f"{reader1_host}:{reader1_port}"

    yield

    print(
        "Reader Container Logs: \n"
        + nucliadb_node_reader.container_obj.logs().decode("utf-8")
    )
    print(
        "Writer Container Logs: \n"
        + nucliadb_node_reader.container_obj.logs().decode("utf-8")
    )

    container_ids = [
        nucliadb_node_reader.container_obj.id,
        nucliadb_node_writer.container_obj.id,
    ]

    nucliadb_node_reader.stop()
    nucliadb_node_writer.stop()

    for container_id in container_ids:
        for i in range(5):
            try:
                docker_client.containers.get(container_id)
            except docker.errors.NotFound:
                break
            time.sleep(2)  # pragma: no cover

    volume_node.remove()


@pytest.fixture(scope="function")
async def writer_stub(node_single):
    channel = aio.insecure_channel(settings.writer_listen_address)
    stub = NodeWriterStub(channel)
    yield stub


@pytest.fixture(scope="function")
async def writer(node_single) -> AsyncIterable[Writer]:
    writer = Writer(settings.writer_listen_address)
    yield writer
    await writer.close()


class Reader:
    def __init__(self, grpc_reader_address: str):
        self.channel = aio.insecure_channel(grpc_reader_address)
        self.stub = nodereader_pb2_grpc.NodeReaderStub(self.channel)  # type: ignore

    async def get_shard(self, pb: ShardId) -> Optional[noderesources_pb2.Shard]:
        req = nodereader_pb2.GetShardRequest()
        req.shard_id.id = pb.id
        return await self.stub.GetShard(req)  # type: ignore

    async def close(self):
        await self.channel.close()


@pytest.fixture(scope="function")
async def reader(node_single) -> AsyncIterable[Reader]:
    reader = Reader(settings.reader_listen_address)
    yield reader
    await reader.close()


@pytest.fixture(scope="function")
async def grpc_server(writer):
    grpc_finalizer = await start_grpc(writer=writer)
    yield
    await grpc_finalizer()


@pytest.fixture(scope="function")
async def worker(
    node_single,
    writer_stub: NodeWriterStub,
    gcs_storage,
    natsd,
    data_path: str,
    writer,
    reader,
    grpc_server,
) -> AsyncIterable[Worker]:
    settings.force_host_id = "node1"
    settings.data_path = data_path
    indexing_settings.index_jetstream_servers = [natsd]
    cache_settings.cache_pubsub_nats_url = [natsd]

    worker = await start_worker(writer)
    yield worker

    # Cleanup all shards to be ready for following tests
    try:
        shards = await writer_stub.ListShards(EmptyQuery())  # type: ignore
        for shard in shards.ids:  # pragma: no cover
            await writer_stub.DeleteShard(shard)  # type: ignore
    except Exception:  # pragma: no cover
        pass

    await worker.finalize()


@pytest.fixture(scope="function")
async def shard(request, writer_stub: NodeWriterStub) -> AsyncIterable[str]:
    channel = (
        ReleaseChannel.EXPERIMENTAL
        if request.param == "EXPERIMENTAL"
        else ReleaseChannel.STABLE
    )
    request = NewShardRequest(kbid="test", release_channel=channel)
    shard: ShardCreated = await writer_stub.NewShard(request)  # type: ignore

    try:
        yield shard.id
    finally:
        await writer_stub.DeleteShard(ShardId(id=shard.id))  # type: ignore


@pytest.fixture(scope="function")
def data_path():
    with tempfile.TemporaryDirectory() as td:
        previous = os.environ.get("DATA_PATH")
        os.environ["DATA_PATH"] = str(td)

        yield str(td)

        if previous is None:
            os.environ.pop("DATA_PATH")
        else:  # pragma: no cover
            os.environ["DATA_PATH"] = previous
