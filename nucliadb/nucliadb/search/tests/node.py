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

import logging
import time

import docker  # type: ignore
import pytest
from grpc import insecure_channel  # type: ignore
from grpc_health.v1 import health_pb2_grpc  # type: ignore
from grpc_health.v1.health_pb2 import HealthCheckRequest  # type: ignore
from nucliadb_protos.nodewriter_pb2 import EmptyQuery, ShardId
from nucliadb_protos.nodewriter_pb2_grpc import NodeWriterStub
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb_utils.tests import free_port

logger = logging.getLogger(__name__)

images.settings["nucliadb_node_reader"] = {
    "image": "eu.gcr.io/stashify-218417/node",
    "version": "main",
    "env": {
        "HOST_KEY_PATH": "/data/node.key",
        "VECTORS_DIMENSION": "768",
        "DATA_PATH": "/data",
        "NODE_TYPE": "Io",
        "READER_LISTEN_ADDRESS": "0.0.0.0:4445",
        "NUCLIADB_DISABLE_TELEMETRY": "True",
        "LAZY_LOADING": "true",
        "RUST_BACKTRACE": "full",
        "RUST_LOG": "nucliadb_node=DEBUG,nucliadb_vectors=DEBUG,nucliadb_fields_tantivy=DEBUG,nucliadb_paragraphs_tantivy=DEBUG,nucliadb_cluster=ERROR",  # noqa
    },
    "options": {
        "command": [
            "/usr/local/bin/node_reader",
        ],
        "ports": {"4445": None},
        "mem_limit": "2g",  # default is 1g, need to override
    },
}

images.settings["nucliadb_node_writer"] = {
    "image": "eu.gcr.io/stashify-218417/node",
    "version": "main",
    "env": {
        "HOST_KEY_PATH": "/data/node.key",
        "VECTORS_DIMENSION": "768",
        "DATA_PATH": "/data",
        "NODE_TYPE": "Io",
        "WRITER_LISTEN_ADDRESS": "0.0.0.0:4446",
        "CHITCHAT_PORT": "4444",
        "NUCLIADB_DISABLE_TELEMETRY": "True",
        "SEED_NODES": "",
        "RUST_BACKTRACE": "full",
        "RUST_LOG": "nucliadb_node=DEBUG,nucliadb_vectors=DEBUG,nucliadb_fields_tantivy=DEBUG,nucliadb_paragraphs_tantivy=DEBUG,nucliadb_cluster=ERROR,chitchat=ERROR",  # noqa
    },
    "options": {
        "command": [
            "/usr/local/bin/node_writer",
        ],
        "ports": {"4446": None},
        "mem_limit": "2g",  # default is 1g, need to override
    },
}

images.settings["nucliadb_node_sidecar"] = {
    "image": "eu.gcr.io/stashify-218417/node_sidecar",
    "version": "main",
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
    },
    "options": {
        "command": [
            "node_sidecar",
        ],
        "ports": {"4447": None},
    },
}

images.settings["nucliadb_cluster_manager"] = {
    "image": "eu.gcr.io/stashify-218417/cluster_manager",
    "version": "main",
    "network": "host",
    "env": {
        "LISTEN_PORT": "4444",
        "NODE_TYPE": "Ingest",
        "SEEDS": "0.0.0.0:4444",
        "MONITOR_ADDR": "TO_REPLACE",
        "RUST_LOG": "debug",
        "RUST_BACKTRACE": "full",
    },
    "options": {
        "command": [
            "/nucliadb_cluster/cluster_manager",
        ],
    },
}


def get_chitchat_port(container_obj, port):
    network = container_obj.attrs["NetworkSettings"]
    service_port = "{0}/udp".format(port)
    for netport in network["Ports"].keys():
        if netport == "6543/tcp":
            continue

        if netport == service_port:
            return network["Ports"][service_port][0]["HostPort"]


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


class nucliadbChitchatNode(BaseImage):
    name = "nucliadb_cluster_manager"
    port = 4444

    def run(self):
        return super(nucliadbChitchatNode, self).run()

    def get_image_options(self):
        options = super(nucliadbChitchatNode, self).get_image_options()
        return options

    def check(self):
        return True


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
nucliadb_cluster_mgr = nucliadbChitchatNode()

nucliadb_node_2_reader = nucliadbNodeReader()
nucliadb_node_2_writer = nucliadbNodeWriter()
nucliadb_node_2_sidecar = nucliadbNodeSidecar()


class _NodeRunner:
    def __init__(self, natsd, gcs):
        self.docker_client = docker.from_env(version=BaseImage.docker_version)
        self.natsd = natsd
        self.gcs = gcs
        self.data = {}

    def start(self):
        docker_platform_name = self.docker_client.api.version()["Platform"][
            "Name"
        ].upper()
        if (
            "DESKTOP" in docker_platform_name
            # newer versions use community
            or "DOCKER ENGINE - COMMUNITY" == docker_platform_name
        ):
            # Valid when using Docker desktop
            docker_internal_host = "host.docker.internal"
        else:
            # Valid when using github actions
            docker_internal_host = "172.17.0.1"
        docker_internal_host = "172.17.0.1"

        self.volume_node_1 = self.docker_client.volumes.create(driver="local")
        self.volume_node_2 = self.docker_client.volumes.create(driver="local")

        cluster_settings.chitchat_binding_host = "0.0.0.0"
        cluster_settings.chitchat_binding_port = free_port()

        images.settings["nucliadb_cluster_manager"]["env"][
            "MONITOR_ADDR"
        ] = f"{docker_internal_host}:{cluster_settings.chitchat_binding_port}"
        images.settings["nucliadb_cluster_manager"]["env"]["UPDATE_INTERVAL"] = "1s"

        cluster_mgr_host, cluster_mgr_port = nucliadb_cluster_mgr.run()

        cluster_mgr_port = get_chitchat_port(nucliadb_cluster_mgr.container_obj, 4444)
        cluster_mgr_real_host = get_container_host(nucliadb_cluster_mgr.container_obj)

        images.settings["nucliadb_node_writer"]["env"][
            "SEED_NODES"
        ] = f"{cluster_mgr_real_host}:4444"
        writer1_host, writer1_port = nucliadb_node_1_writer.run(self.volume_node_1)

        writer2_host, writer2_port = nucliadb_node_2_writer.run(self.volume_node_2)
        reader1_host, reader1_port = nucliadb_node_1_reader.run(self.volume_node_1)

        reader2_host, reader2_port = nucliadb_node_2_reader.run(self.volume_node_2)

        natsd_server = self.natsd.replace("localhost", docker_internal_host)
        images.settings["nucliadb_node_sidecar"]["env"][
            "INDEX_JETSTREAM_SERVERS"
        ] = f'["{natsd_server}"]'
        images.settings["nucliadb_node_sidecar"]["env"][
            "CACHE_PUBSUB_NATS_URL"
        ] = f'["{natsd_server}"]'
        gcs_server = self.gcs.replace("localhost", docker_internal_host)
        images.settings["nucliadb_node_sidecar"]["env"]["GCS_ENDPOINT_URL"] = gcs_server
        images.settings["nucliadb_node_sidecar"]["env"]["GCS_BUCKET"] = "test"
        images.settings["nucliadb_node_sidecar"]["env"]["FILE_BACKEND"] = "gcs"
        images.settings["nucliadb_node_sidecar"]["env"][
            "GCS_INDEXING_BUCKET"
        ] = "indexing"
        images.settings["nucliadb_node_sidecar"]["env"][
            "GCS_DEADLETTER_BUCKET"
        ] = "deadletter"

        images.settings["nucliadb_node_sidecar"]["env"][
            "READER_LISTEN_ADDRESS"
        ] = f"{docker_internal_host}:{reader1_port}"
        images.settings["nucliadb_node_sidecar"]["env"][
            "WRITER_LISTEN_ADDRESS"
        ] = f"{docker_internal_host}:{writer1_port}"

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
                "chitchat": {
                    "host": cluster_mgr_host,
                    "port": cluster_mgr_port,
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
        nucliadb_node_1_reader.stop()
        nucliadb_node_1_writer.stop()
        nucliadb_node_1_sidecar.stop()
        nucliadb_node_2_writer.stop()
        nucliadb_node_2_reader.stop()
        nucliadb_node_2_sidecar.stop()
        nucliadb_cluster_mgr.stop()

        for container in (
            nucliadb_node_1_reader,
            nucliadb_node_1_writer,
            nucliadb_node_2_reader,
            nucliadb_node_2_writer,
            nucliadb_node_2_sidecar,
            nucliadb_node_2_sidecar,
            nucliadb_cluster_mgr,
        ):
            for i in range(5):
                try:
                    self.docker_client.containers.get(container.container_obj.id)  # type: ignore
                except docker.errors.NotFound:
                    break
                time.sleep(2)

        self.volume_node_1.remove()
        self.volume_node_2.remove()

    def restart(self):
        self.stop()
        return self.start()

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
        cluster_settings.sidecar_port_map = {
            self.data["writer1_internal_host"]: self.data["sidecar1"]["port"],
            self.data["writer2_internal_host"]: self.data["sidecar2"]["port"],
        }

        cluster_settings.node_writer_port = None  # type: ignore
        cluster_settings.node_reader_port = None  # type: ignore
        cluster_settings.node_sidecar_port = None  # type: ignore


@pytest.fixture(scope="session", autouse=False)
def _node_runner(natsd: str, gcs: str):
    yield _NodeRunner(natsd, gcs)


@pytest.fixture(scope="session", autouse=False)
def _node(natsd: str, gcs: str, _node_runner: _NodeRunner):
    yield _node_runner.start()

    _node_runner.stop()


@pytest.fixture(scope="function")
def node(_node, _node_runner: _NodeRunner, request):
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

    for shard in writer1.ListShards(EmptyQuery()).ids:
        writer1.DeleteShard(ShardId(id=shard.id))

    for shard in writer2.ListShards(EmptyQuery()).ids:
        writer2.DeleteShard(ShardId(id=shard.id))

    channel1.close()
    channel2.close()

    _node_runner.setup_env()

    yield _node

    _test_failed_statuses = getattr(request.node, "_test_failed_statuses", {})
    if len(_test_failed_statuses) > 0 and any(_test_failed_statuses.values()):
        logger.warning(
            "Test failed with rerun, restarting node in case it's related to a node crash"
        )
        _node_runner.restart()
