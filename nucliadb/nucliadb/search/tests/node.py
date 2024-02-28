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
import dataclasses
import logging
import os
import time
from typing import Union

import backoff
import docker  # type: ignore
import pytest
from grpc import insecure_channel
from grpc_health.v1 import health_pb2_grpc
from grpc_health.v1.health_pb2 import HealthCheckRequest
from nucliadb_protos.nodewriter_pb2 import EmptyQuery, ShardId
from nucliadb_protos.nodewriter_pb2_grpc import NodeWriterStub
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore
from pytest_lazy_fixtures import lazy_fixture

from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb_utils.tests.conftest import get_testing_storage_backend

logger = logging.getLogger(__name__)

images.settings["nucliadb_node_reader"] = {
    "image": "europe-west4-docker.pkg.dev/nuclia-internal/nuclia/node",
    "version": "latest",
    "env": {
        "HOST_KEY_PATH": "/data/node.key",
        "DATA_PATH": "/data",
        "READER_LISTEN_ADDRESS": "0.0.0.0:4445",
        "NUCLIADB_DISABLE_ANALYTICS": "True",
        "RUST_BACKTRACE": "full",
        "DEBUG": "1",
        "RUST_LOG": "nucliadb_*=DEBUG",
    },
    "options": {
        "command": [
            "/usr/local/bin/node_reader",
        ],
        "ports": {"4445": None},
        "mem_limit": "3g",  # default is 1g, need to override
        "platform": "linux/amd64",
    },
}

images.settings["nucliadb_node_writer"] = {
    "image": "europe-west4-docker.pkg.dev/nuclia-internal/nuclia/node",
    "version": "latest",
    "env": {
        "HOST_KEY_PATH": "/data/node.key",
        "DATA_PATH": "/data",
        "WRITER_LISTEN_ADDRESS": "0.0.0.0:4446",
        "NUCLIADB_DISABLE_ANALYTICS": "True",
        "RUST_BACKTRACE": "full",
        "DEBUG": "1",
        "RUST_LOG": "nucliadb_*=DEBUG",
    },
    "options": {
        "command": [
            "/usr/local/bin/node_writer",
        ],
        "ports": {"4446": None},
        "mem_limit": "3g",  # default is 1g, need to override
        "platform": "linux/amd64",
    },
}

images.settings["nucliadb_node_sidecar"] = {
    "image": "europe-west4-docker.pkg.dev/nuclia-internal/nuclia/node_sidecar",
    "version": "latest",
    "env": {
        "INDEX_JETSTREAM_SERVERS": "[]",
        "CACHE_PUBSUB_NATS_URL": "",
        "HOST_KEY_PATH": "/data/node.key",
        "DATA_PATH": "/data",
        "SIDECAR_LISTEN_ADDRESS": "0.0.0.0:4447",
        "READER_LISTEN_ADDRESS": "0.0.0.0:4445",
        "WRITER_LISTEN_ADDRESS": "0.0.0.0:4446",
        "PYTHONUNBUFFERED": "1",
        "LOG_LEVEL": "DEBUG",
        "DEBUG": "1",
    },
    "options": {
        "command": [
            "node_sidecar",
        ],
        "ports": {"4447": None},
        "platform": "linux/amd64",
    },
}


def get_container_host(container_obj):
    return container_obj.attrs["NetworkSettings"]["IPAddress"]


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


class nucliadbNodeSidecar(BaseImage):
    name = "nucliadb_node_sidecar"
    port = 4447

    def run(self, volume):
        self._volume = volume
        self._mount = "/data"
        return super(nucliadbNodeSidecar, self).run()

    def get_image_options(self):
        options = super(nucliadbNodeSidecar, self).get_image_options()
        options["volumes"] = {self._volume.name: {"bind": "/data"}}
        return options

    def check(self):
        channel = insecure_channel(f"{self.host}:{self.get_port()}")
        stub = health_pb2_grpc.HealthStub(channel)
        pb = HealthCheckRequest(service="")
        try:
            result = stub.Check(pb)
            return result.status == 1
        except:  # noqa
            return False


nucliadb_node_1_reader = nucliadbNodeReader()
nucliadb_node_1_writer = nucliadbNodeWriter()
nucliadb_node_1_sidecar = nucliadbNodeSidecar()

nucliadb_node_2_reader = nucliadbNodeReader()
nucliadb_node_2_writer = nucliadbNodeWriter()
nucliadb_node_2_sidecar = nucliadbNodeSidecar()


@dataclasses.dataclass
class NodeS3Storage:
    server: str

    def envs(self):
        return {
            "FILE_BACKEND": "s3",
            "S3_CLIENT_ID": "",
            "S3_CLIENT_SECRET": "",
            "S3_BUCKET": "test",
            "S3_INDEXING_BUCKET": "indexing",
            "S3_DEADLETTER_BUCKET": "deadletter",
            "S3_ENDPOINT": self.server,
        }


@dataclasses.dataclass
class NodeGCSStorage:
    server: str

    def envs(self):
        return {
            "FILE_BACKEND": "gcs",
            "GCS_BUCKET": "test",
            "GCS_INDEXING_BUCKET": "indexing",
            "GCS_DEADLETTER_BUCKET": "deadletter",
            "GCS_ENDPOINT_URL": self.server,
        }


NodeStorage = Union[NodeGCSStorage, NodeS3Storage]


class _NodeRunner:
    def __init__(self, natsd, storage: NodeStorage):
        self.docker_client = docker.from_env(version=BaseImage.docker_version)
        self.natsd = natsd
        self.storage = storage
        self.data = {}  # type: ignore

    def start(self):
        docker_platform_name = self.docker_client.api.version()["Platform"][
            "Name"
        ].upper()
        if "GITHUB_ACTION" not in os.environ and (
            "DESKTOP" in docker_platform_name
            # newer versions use community
            or "DOCKER ENGINE - COMMUNITY" == docker_platform_name
        ):
            # Valid when using Docker desktop
            docker_internal_host = "host.docker.internal"
        else:
            # Valid when using github actions
            docker_internal_host = "172.17.0.1"

        self.volume_node_1 = self.docker_client.volumes.create(driver="local")
        self.volume_node_2 = self.docker_client.volumes.create(driver="local")

        writer1_host, writer1_port = nucliadb_node_1_writer.run(self.volume_node_1)
        writer2_host, writer2_port = nucliadb_node_2_writer.run(self.volume_node_2)

        reader1_host, reader1_port = nucliadb_node_1_reader.run(self.volume_node_1)
        reader2_host, reader2_port = nucliadb_node_2_reader.run(self.volume_node_2)

        natsd_server = self.natsd.replace("localhost", docker_internal_host)
        images.settings["nucliadb_node_sidecar"]["env"].update(
            {
                "INDEX_JETSTREAM_SERVERS": f'["{natsd_server}"]',
                "CACHE_PUBSUB_NATS_URL": f'["{natsd_server}"]',
                "READER_LISTEN_ADDRESS": f"{docker_internal_host}:{reader1_port}",
                "WRITER_LISTEN_ADDRESS": f"{docker_internal_host}:{writer1_port}",
            }
        )
        self.storage.server = self.storage.server.replace(
            "localhost", docker_internal_host
        )
        images.settings["nucliadb_node_sidecar"]["env"].update(self.storage.envs())

        sidecar1_host, sidecar1_port = nucliadb_node_1_sidecar.run(self.volume_node_1)

        images.settings["nucliadb_node_sidecar"]["env"][
            "READER_LISTEN_ADDRESS"
        ] = f"{docker_internal_host}:{reader2_port}"
        images.settings["nucliadb_node_sidecar"]["env"][
            "WRITER_LISTEN_ADDRESS"
        ] = f"{docker_internal_host}:{writer2_port}"

        sidecar2_host, sidecar2_port = nucliadb_node_2_sidecar.run(self.volume_node_2)

        writer1_internal_host = get_container_host(nucliadb_node_1_writer.container_obj)
        writer2_internal_host = get_container_host(nucliadb_node_2_writer.container_obj)

        self.data.update(
            {
                "writer1_internal_host": writer1_internal_host,
                "writer2_internal_host": writer2_internal_host,
                "writer1": {
                    "host": writer1_host,
                    "port": writer1_port,
                },
                "writer2": {
                    "host": writer2_host,
                    "port": writer2_port,
                },
                "reader1": {
                    "host": reader1_host,
                    "port": reader1_port,
                },
                "reader2": {
                    "host": reader2_host,
                    "port": reader2_port,
                },
                "sidecar1": {
                    "host": sidecar1_host,
                    "port": sidecar1_port,
                },
                "sidecar2": {
                    "host": sidecar2_host,
                    "port": sidecar2_port,
                },
            }
        )
        return self.data

    def stop(self):
        container_ids = [
            nucliadb_node_1_reader.container_obj.id,
            nucliadb_node_1_writer.container_obj.id,
            nucliadb_node_1_sidecar.container_obj.id,
            nucliadb_node_2_writer.container_obj.id,
            nucliadb_node_2_reader.container_obj.id,
            nucliadb_node_2_sidecar.container_obj.id,
        ]
        nucliadb_node_1_reader.stop()
        nucliadb_node_1_writer.stop()
        nucliadb_node_1_sidecar.stop()
        nucliadb_node_2_writer.stop()
        nucliadb_node_2_reader.stop()
        nucliadb_node_2_sidecar.stop()

        for container_id in container_ids:
            for _ in range(5):
                try:
                    self.docker_client.containers.get(container_id)  # type: ignore
                except docker.errors.NotFound:
                    break
                time.sleep(2)

        self.volume_node_1.remove()
        self.volume_node_2.remove()

    def setup_env(self):
        # reset on every test run in case something touches it
        cluster_settings.writer_port_map = {
            self.data["writer1_internal_host"]: self.data["writer1"]["port"],
            self.data["writer2_internal_host"]: self.data["writer2"]["port"],
        }
        cluster_settings.reader_port_map = {
            self.data["writer1_internal_host"]: self.data["reader1"]["port"],
            self.data["writer2_internal_host"]: self.data["reader2"]["port"],
        }

        cluster_settings.node_writer_port = None  # type: ignore
        cluster_settings.node_reader_port = None  # type: ignore

        cluster_settings.cluster_discovery_mode = "manual"
        cluster_settings.cluster_discovery_manual_addresses = [
            self.data["writer1_internal_host"],
            self.data["writer2_internal_host"],
        ]


@pytest.fixture(scope="session")
def gcs_node_storage(gcs):
    return NodeGCSStorage(server=gcs)


@pytest.fixture(scope="session")
def s3_node_storage(s3):
    return NodeS3Storage(server=s3)


def lazy_load_storage_backend():
    backend = get_testing_storage_backend()
    if backend == "gcs":
        return [lazy_fixture.lf("gcs_node_storage")]
    elif backend == "s3":
        return [lazy_fixture.lf("s3_node_storage")]
    else:
        print(f"Unknown storage backend {backend}, using gcs")
        return [lazy_fixture.lf("gcs_node_storage")]


@pytest.fixture(scope="session", params=lazy_load_storage_backend())
def node_storage(request):
    return request.param


@pytest.fixture(scope="session", autouse=False)
def _node(natsd: str, node_storage):
    nr = _NodeRunner(natsd, node_storage)
    try:
        cluster_info = nr.start()
    except Exception:
        nr.stop()
        raise
    nr.setup_env()
    yield cluster_info
    nr.stop()


@pytest.fixture(scope="function")
def node(_node, request):
    # clean up all shard data before each test
    channel1 = insecure_channel(
        f"{_node['writer1']['host']}:{_node['writer1']['port']}"
    )
    channel2 = insecure_channel(
        f"{_node['writer2']['host']}:{_node['writer2']['port']}"
    )
    writer1 = NodeWriterStub(channel1)
    writer2 = NodeWriterStub(channel2)

    logger.debug("cleaning up shards data")
    try:
        cleanup_node(writer1)
        cleanup_node(writer2)
    except Exception:
        logger.error(
            "Error cleaning up shards data. Maybe the node fixture could not start properly?",
            exc_info=True,
        )

        client = docker.client.from_env()
        containers_by_port = {}
        for container in client.containers.list():
            name = container.name
            command = container.attrs["Config"]["Cmd"]
            ports = container.ports
            print(f"container {name} executing {command} is using ports: {ports}")

            for internal_port in container.ports:
                for host in container.ports[internal_port]:
                    port = host["HostPort"]
                    port_containers = containers_by_port.setdefault(port, [])
                    if container not in port_containers:
                        port_containers.append(container)

        for port, containers in containers_by_port.items():
            if len(containers) > 1:
                names = ", ".join([container.name for container in containers])
                print(f"ATENTION! Containers {names} share port {port}!")
        raise
    finally:
        channel1.close()
        channel2.close()

    yield _node


@backoff.on_exception(
    backoff.expo, Exception, jitter=backoff.random_jitter, max_tries=5
)
def cleanup_node(writer: NodeWriterStub):
    for shard in writer.ListShards(EmptyQuery()).ids:
        writer.DeleteShard(ShardId(id=shard.id))
