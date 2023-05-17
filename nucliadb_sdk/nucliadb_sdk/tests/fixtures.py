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
from dataclasses import dataclass
from typing import Optional
from uuid import uuid4

import pytest
import requests
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

import nucliadb_sdk
from nucliadb_client.client import NucliaDBClient as GRPCNucliaDBClient

images.settings["nucliadb"] = {
    "image": "nuclia/nucliadb",
    "version": "latest",
    "env": {
        "DRIVER": "local",
        "NUCLIADB_DISABLE_TELEMETRY": "True",
        "dummy_predict": "True",
        "dummy_processing": "True",
        "max_receive_message_length": "40",
        "TEST_SENTENCE_ENCODER": "multilingual-2023-02-21",
        "TEST_RELATIONS": """{"tokens": [{"text": "Nuclia", "ner": "ORG"}]}""",
    },
}

NUCLIA_DOCS_dataset = (
    "https://storage.googleapis.com/config.flaps.dev/test_nucliadb/nuclia.export"
)


class NucliaDB(BaseImage):
    name = "nucliadb"
    port = 8080

    def check(self):
        try:
            response = requests.get(f"http://{self.host}:{self.get_port()}")
            return response.status_code == 200
        except Exception:
            return False


@dataclass
class NucliaFixture:
    host: str
    port: int
    grpc: int
    container: Optional[NucliaDB] = None


@pytest.fixture(scope="session")
def nucliadb():
    if os.environ.get("TEST_LOCAL_NUCLIADB"):
        yield NucliaFixture(
            host=os.environ.get("TEST_LOCAL_NUCLIADB"),
            port=8080,
            grpc=8060,
            container="local",
        )
    else:
        container = NucliaDB()
        host, port = container.run()
        network = container.container_obj.attrs["NetworkSettings"]
        if os.environ.get("TESTING", "") == "jenkins":
            grpc = 8060
        else:
            service_port = "8060/tcp"
            grpc = network["Ports"][service_port][0]["HostPort"]
        yield NucliaFixture(
            host=host, port=port, grpc=grpc, container=container.container_obj
        )
        container.stop()


@pytest.fixture(scope="session")
def sdk(nucliadb: NucliaFixture):
    sdk = nucliadb_sdk.NucliaSDK(
        region=nucliadb_sdk.Region.ON_PREM,
        url=f"http://{nucliadb.host}:{nucliadb.port}/api/",
    )
    return sdk


@pytest.fixture(scope="function")
def knowledgebox(sdk: nucliadb_sdk.NucliaSDK):
    kbslug = uuid4().hex
    kb = sdk.create_knowledge_box(slug=kbslug)

    yield kb

    sdk.delete_knowledge_box(kbid=kb.uuid)


async def init_fixture(
    nucliadb: NucliaFixture,
    dataset_slug: str,
    dataset_location: str,
):
    client = GRPCNucliaDBClient(
        host=nucliadb.host, grpc=nucliadb.grpc, http=nucliadb.port, train=0
    )
    client.init_async_grpc()
    kbid = await client.import_kb(slug=dataset_slug, location=dataset_location)
    await client.finish_async_grpc()
    return kbid


@pytest.fixture(scope="session")
def docs_dataset(nucliadb: NucliaFixture):
    kbid = asyncio.run(init_fixture(nucliadb, "docs", NUCLIA_DOCS_dataset))
    yield kbid
