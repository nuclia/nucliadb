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
from nucliadb_protos.nodewriter_pb2_grpc import NodeWriterStub
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

from nucliadb_node.app import main
from nucliadb_node.settings import settings
from nucliadb_node.shadow_shards import SHADOW_SHARDS_FOLDER

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


@pytest.fixture(scope="function")
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
async def sidecar(node_single, gcs_storage, natsd):
    settings.force_host_id = "node1"
    settings.data_path = "/tmp"
    app = await main()
    # Create a shard
    yield app

    # Delete a shard
    for finalizer in app.finalizers:
        if asyncio.iscoroutinefunction(finalizer):
            await finalizer()
        else:
            finalizer()


@pytest.fixture(scope="function")
async def sidecar_grpc_servicer(sidecar):
    channel = aio.insecure_channel(settings.sidecar_listen_address)
    yield channel


@pytest.fixture(scope="function")
async def shard() -> AsyncIterable[str]:
    stub = NodeWriterStub(aio.insecure_channel(settings.writer_listen_address))
    request = EmptyQuery()
    shard: ShardCreated = await stub.NewShard(request)  # type: ignore
    yield shard.id
    sid = ShardId()
    sid.id = shard.id
    await stub.DeleteShard(sid)  # type: ignore


@pytest.fixture(scope="function")
def shadow_folder():
    with tempfile.TemporaryDirectory() as td:
        previous = os.environ.get("DATA_PATH")
        os.environ["DATA_PATH"] = str(td)

        yield SHADOW_SHARDS_FOLDER.format(data_path=td)

        if previous is None:
            os.environ.pop("DATA_PATH")
        else:
            os.environ["DATA_PATH"] = previous
