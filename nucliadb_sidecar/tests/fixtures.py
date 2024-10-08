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
import time
from typing import AsyncIterable, Optional

import docker  # type: ignore
import pytest
from grpc import aio, insecure_channel
from grpc_health.v1 import health_pb2_grpc
from grpc_health.v1.health_pb2 import HealthCheckRequest
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

from nucliadb_protos import nodereader_pb2, nodereader_pb2_grpc, noderesources_pb2
from nucliadb_protos.noderesources_pb2 import EmptyQuery, ShardCreated, ShardId
from nucliadb_protos.nodewriter_pb2 import NewShardRequest
from nucliadb_protos.nodewriter_pb2_grpc import NodeWriterStub
from nucliadb_protos.utils_pb2 import ReleaseChannel
from nucliadb_sidecar import SERVICE_NAME
from nucliadb_sidecar.app import start_listeners, start_worker
from nucliadb_sidecar.pull import Worker
from nucliadb_sidecar.service import start_grpc
from nucliadb_sidecar.settings import indexing_settings, settings
from nucliadb_sidecar.writer import Writer
from nucliadb_utils.cache.settings import settings as cache_settings
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.settings import nats_consumer_settings, storage_settings
from nucliadb_utils.utilities import get_storage, start_nats_manager, stop_nats_manager

images.settings["nucliadb_sidecar_reader"] = {
    "image": "europe-west4-docker.pkg.dev/nuclia-internal/nuclia/node",
    "version": "latest",
    "env": {
        "NUCLIADB_DISABLE_ANALYTICS": "True",
        "DATA_PATH": "/data",
        "READER_LISTEN_ADDRESS": "0.0.0.0:4445",
        "RUST_BACKTRACE": "full",
        "RUST_LOG": "nucliadb_*=DEBUG",  # noqa
        "DEBUG": "true",
    },
    "options": {
        "command": [
            "/usr/local/bin/node_reader",
        ],
        "platform": "linux/amd64",
        "mem_limit": "3g",
        "ports": {"4445": ("0.0.0.0", 0)},
        "publish_all_ports": False,
    },
}

images.settings["nucliadb_sidecar_writer"] = {
    "image": "europe-west4-docker.pkg.dev/nuclia-internal/nuclia/node",
    "version": "latest",
    "env": {
        "NUCLIADB_DISABLE_ANALYTICS": "True",
        "DATA_PATH": "/data",
        "WRITER_LISTEN_ADDRESS": "0.0.0.0:4446",
        "RUST_BACKTRACE": "full",
        "RUST_LOG": "nucliadb_*=DEBUG",  # noqa
        "DEBUG": "true",
    },
    "options": {
        "command": [
            "/usr/local/bin/node_writer",
        ],
        "platform": "linux/amd64",
        "mem_limit": "3g",
        "ports": {"4446": ("0.0.0.0", 0)},
        "publish_all_ports": False,
    },
}


class nucliadbNodeReader(BaseImage):
    name = "nucliadb_sidecar_reader"
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
    name = "nucliadb_sidecar_writer"
    port = 4446

    def run(
        self,
        volume,
        file_backend: str = "unset",
        file_backend_config: Optional[dict[str, str]] = None,
    ):
        self._volume = volume
        self._mount = "/data"
        self._file_backend = file_backend
        self._file_backend_config = file_backend_config or {}
        return super(nucliadbNodeWriter, self).run()

    def get_image_options(self):
        options = super(nucliadbNodeWriter, self).get_image_options()
        options["volumes"] = {self._volume.name: {"bind": "/data"}}

        # Set the file backend and its configuration via environment variables
        options["environment"]["FILE_BACKEND"] = self._file_backend
        for key, value in self._file_backend_config.items():
            options["environment"][key] = value

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


nucliadb_sidecar_reader = nucliadbNodeReader()
nucliadb_sidecar_writer = nucliadbNodeWriter()


@pytest.fixture(scope="function")
def node_single(storage):
    docker_client = docker.from_env(version=BaseImage.docker_version)
    volume_node = docker_client.volumes.create(driver="local")

    # Extract relevant file backend settings to pass to the image
    file_backend = storage_settings.file_backend.value
    file_backend_settings = {
        k: (getattr(storage_settings, k) or "").replace("localhost", "172.17.0.1")
        for k in dir(storage_settings)
        if k.startswith(file_backend)
    }
    file_backend_settings[f"{file_backend}_INDEXING_BUCKET"] = "indexing"

    writer1_host, writer1_port = nucliadb_sidecar_writer.run(
        volume_node, file_backend=file_backend, file_backend_config=file_backend_settings
    )
    reader1_host, reader1_port = nucliadb_sidecar_reader.run(volume_node)

    settings.writer_listen_address = f"{writer1_host}:{writer1_port}"
    settings.reader_listen_address = f"{reader1_host}:{reader1_port}"

    yield

    print("Reader Container Logs: \n" + nucliadb_sidecar_reader.container_obj.logs().decode("utf-8"))
    print("Writer Container Logs: \n" + nucliadb_sidecar_reader.container_obj.logs().decode("utf-8"))

    container_ids = [
        nucliadb_sidecar_reader.container_obj.id,
        nucliadb_sidecar_writer.container_obj.id,
    ]

    nucliadb_sidecar_reader.stop()
    nucliadb_sidecar_writer.stop()

    for container_id in container_ids:
        for _ in range(5):
            try:
                docker_client.containers.get(container_id)
            except docker.errors.NotFound:
                break
            time.sleep(2)  # pragma: no cover

    volume_node.remove()


@pytest.fixture(scope="function")
async def writer_stub(node_single) -> AsyncIterable[NodeWriterStub]:
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
        self.stub = nodereader_pb2_grpc.NodeReaderStub(self.channel)

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


# As consumers are not removed and recreated changes per test won't have effect,
# so we set this value per session
@pytest.fixture(scope="session", autouse=True)
def concurrent_nats_processing():
    original = nats_consumer_settings.nats_max_ack_pending
    nats_consumer_settings.nats_max_ack_pending = 10
    yield
    nats_consumer_settings.nats_max_ack_pending = original


@pytest.fixture(scope="function")
def nats_settings(natsd):
    original_index_jetstream_servers = indexing_settings.index_jetstream_servers
    original_cache_pubsub_nats_url = cache_settings.cache_pubsub_nats_url

    indexing_settings.index_jetstream_servers = [natsd]
    cache_settings.cache_pubsub_nats_url = [natsd]

    yield

    indexing_settings.index_jetstream_servers = original_index_jetstream_servers
    cache_settings.cache_pubsub_nats_url = original_cache_pubsub_nats_url


@pytest.fixture(scope="function")
async def nats_manager(
    natsd,
    nats_settings,
) -> AsyncIterable[NatsConnectionManager]:
    nats_manager = await start_nats_manager(
        SERVICE_NAME,
        nats_servers=indexing_settings.index_jetstream_servers,
        nats_creds=None,
    )
    yield nats_manager
    await stop_nats_manager()


@pytest.fixture(scope="function")
async def listeners(writer: Writer):
    listeners = await start_listeners(writer)
    yield
    for listener in listeners:
        await listener.finalize()


@pytest.fixture(scope="function")
async def worker(
    node_single,
    storage,
    nats_manager: NatsConnectionManager,
    writer: Writer,
    writer_stub: NodeWriterStub,
    grpc_server,
    listeners,
) -> AsyncIterable[Worker]:
    settings.force_host_id = "node1"

    await get_storage(service_name=SERVICE_NAME)
    worker = await start_worker(writer, nats_manager)

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
    channel = ReleaseChannel.STABLE
    request = NewShardRequest(kbid="test", release_channel=channel)
    shard: ShardCreated = await writer_stub.NewShard(request)  # type: ignore

    try:
        yield shard.id
    finally:
        await writer_stub.DeleteShard(ShardId(id=shard.id))  # type: ignore


@pytest.fixture(scope="function")
async def bunch_of_shards(request, writer_stub: NodeWriterStub) -> AsyncIterable[list[str]]:
    channel = ReleaseChannel.STABLE
    request = NewShardRequest(kbid="test", release_channel=channel)

    try:
        shard_ids = []
        for _ in range(5):
            shard: ShardCreated = await writer_stub.NewShard(request)  # type: ignore
            shard_ids.append(shard.id)

        yield shard_ids

    finally:
        for shard_id in shard_ids:
            await writer_stub.DeleteShard(ShardId(id=shard_id))  # type: ignore
