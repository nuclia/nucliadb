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

from dataclasses import dataclass
from typing import Optional

import pytest
from grpc import insecure_channel  # type: ignore
from grpc_health.v1 import health_pb2_grpc  # type: ignore
from grpc_health.v1.health_pb2 import HealthCheckRequest  # type: ignore
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

from nucliadb_client.client import NucliaDBClient

images.settings["nucliadb"] = {
    "image": "nuclia/nucliadb",
    "version": "latest",
    "env": {
        "NUCLIADB_DISABLE_TELEMETRY": "True",
        "NUCLIADB_ENV": "True",
        "DRIVER": "LOCAL",
        "LOG": "DEBUG",
        "HTTP": "8080",
        "GRPC": "8030",
        "TRAIN": "8040",
    },
    "options": {
        "ports": {"8080": None, "8030": None, "8040": None},
    },
}


class NucliaDB(BaseImage):
    name = "nucliadb"
    port = 8030
    train_port = 8040
    http_port = 8080

    # def get_port(self):
    #     return 8030

    def get_http(self):
        # return 8080
        network = self.container_obj.attrs["NetworkSettings"]
        service_port = "{0}/tcp".format(self.http_port)
        if service_port in network["Ports"]:
            return network["Ports"][service_port][0]["HostPort"]
        else:
            return None

    def get_train(self) -> Optional[int]:
        network = self.container_obj.attrs["NetworkSettings"]
        service_port = "{0}/tcp".format(self.train_port)
        if service_port in network["Ports"]:
            return network["Ports"][service_port][0]["HostPort"]
        else:
            return None

    def check(self):
        channel = insecure_channel(f"{self.host}:{self.get_port()}")
        stub = health_pb2_grpc.HealthStub(channel)
        pb = HealthCheckRequest()
        try:
            result = stub.Check(pb)
            return result.status == 1
        except:  # noqa
            return False


nucliadb_image = NucliaDB()


@dataclass
class NucliaDBFixture:
    host: str
    grpc: int
    http: int
    train: int


@pytest.fixture(scope="function")
def nucliadb():
    host, grpc_port = nucliadb_image.run()
    http_port = nucliadb_image.get_http()
    train_port = nucliadb_image.get_train()

    yield NucliaDBFixture(host=host, grpc=grpc_port, http=http_port, train=train_port)

    nucliadb_image.stop()


@pytest.fixture(scope="function")
def nucliadb_client(nucliadb: NucliaDBFixture):
    yield NucliaDBClient(
        host=nucliadb.host, grpc=nucliadb.grpc, http=nucliadb.http, train=nucliadb.train
    )


@pytest.fixture(scope="function")
def nucliadb_knowledgebox(nucliadb_client: NucliaDBClient):
    kb = nucliadb_client.create_kb(slug="testkb")
    yield kb
    assert kb.delete()
