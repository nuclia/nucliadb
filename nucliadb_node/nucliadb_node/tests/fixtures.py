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
import os
import tempfile
import time
from typing import AsyncIterable

import docker  # type: ignore
import pytest
from grpc import aio, insecure_channel  # type: ignore
from grpc_health.v1 import health_pb2_grpc  # type: ignore
from grpc_health.v1.health_pb2 import HealthCheckRequest  # type: ignore
from nucliadb_protos.noderesources_pb2 import EmptyQuery, ShardCreated, ShardId
from nucliadb_protos.nodesidecar_pb2_grpc import NodeSidecarStub
from nucliadb_protos.nodewriter_pb2 import NewShardRequest
from nucliadb_protos.nodewriter_pb2_grpc import NodeWriterStub
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

from nucliadb_node import shadow_shards
from nucliadb_node.app import App, main
from nucliadb_node.settings import settings

images.settings["nucliadb_node_reader"] = {
    "image": "eu.gcr.io/stashify-218417/node",
    "version": "main",
    "command": "bash -c 'node_reader & node_writer'",
    "env": {
        "VECTORS_DIMENSION": "768",
        "NUCLIADB_DISABLE_TELEMETRY": "True",
        "DATA_PATH": "/data",
        "READER_LISTEN_ADDRESS": "0.0.0.0:4445",
        "LAZY_LOADING": "true",
        "RUST_BACKTRACE": "full",
    },
    "options": {
        "command": [
            "/usr/local/bin/node_reader",
        ],
        "ports": {"4445": None},
    },
}

images.settings["nucliadb_node_writer"] = {
    "image": "eu.gcr.io/stashify-218417/node",
    "version": "main",
    "env": {
        "VECTORS_DIMENSION": "768",
        "NUCLIADB_DISABLE_TELEMETRY": "True",
        "DATA_PATH": "/data",
        "WRITER_LISTEN_ADDRESS": "0.0.0.0:4446",
        "CHITCHAT_PORT": "4444",
        "SEED_NODES": "",
        "RUST_BACKTRACE": "full",
        "RUST_LOG": "nucliadb_cluster=DEBUG",
    },
    "options": {
        "command": [
            "/usr/local/bin/node_writer",
        ],
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
        except:  # noqa
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
        except:  # noqa
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

    nucliadb_node_reader.stop()
    nucliadb_node_writer.stop()

    for container in (
        nucliadb_node_reader,
        nucliadb_node_writer,
    ):
        for i in range(5):
            try:
                docker_client.containers.get(container.container_obj.id)
            except docker.errors.NotFound:
                print("REMOVED")
                break
            time.sleep(2)

    volume_node.remove()


@pytest.fixture(scope="function")
async def writer_stub(node_single):
    channel = aio.insecure_channel(settings.writer_listen_address)
    stub = NodeWriterStub(channel)
    yield stub


@pytest.fixture(scope="function")
async def sidecar(
    node_single, writer_stub: NodeWriterStub, gcs_storage, natsd, data_path: str
) -> AsyncIterable[App]:
    settings.force_host_id = "node1"
    settings.data_path = data_path
    app = await main()

    yield app

    # Cleanup all shards to be ready for following tests
    shards = await writer_stub.ListShards(EmptyQuery())  # type: ignore
    for shard in shards.ids:
        deleted = await writer_stub.DeleteShard(shard)  # type: ignore
        assert shard == deleted

    for finalizer in app.finalizers:
        if asyncio.iscoroutinefunction(finalizer):
            await finalizer()
        else:
            finalizer()


@pytest.fixture(scope="function")
async def sidecar_stub(sidecar):
    channel = aio.insecure_channel(settings.sidecar_listen_address)
    stub = NodeSidecarStub(channel)
    yield stub


@pytest.fixture(scope="function")
async def shard(writer_stub: NodeWriterStub) -> AsyncIterable[str]:
    request = NewShardRequest(kbid="test")
    shard: ShardCreated = await writer_stub.NewShard(request)  # type: ignore
    yield shard.id
    sid = ShardId()
    sid.id = shard.id
    await writer_stub.DeleteShard(sid)  # type: ignore


@pytest.fixture(scope="function")
def data_path():
    with tempfile.TemporaryDirectory() as td:
        previous = os.environ.get("DATA_PATH")
        os.environ["DATA_PATH"] = str(td)
        shadow_shards.MAIN.pop("manager", None)

        yield str(td)

        if previous is None:
            os.environ.pop("DATA_PATH")
        else:
            os.environ["DATA_PATH"] = previous


@pytest.fixture(scope="function")
def shadow_folder(data_path) -> str:
    return shadow_shards.SHADOW_SHARDS_FOLDER.format(data_path=data_path)


@pytest.fixture(scope="function")
async def shadow_shard(
    sidecar_stub: NodeSidecarStub, shadow_folder: str
) -> AsyncIterable[str]:
    resp = await sidecar_stub.CreateShadowShard(EmptyQuery())  # type: ignore
    assert resp.success

    yield resp.shard.id

    await sidecar_stub.DeleteShadowShard(resp.shard)  # type: ignore
