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

from nucliadb_client.client import NucliaDBClient as GRPCNucliaDBClient
from nucliadb_models.resource import KnowledgeBoxObj
from nucliadb_sdk.client import Environment, NucliaDBClient
from nucliadb_sdk.knowledgebox import KnowledgeBox

images.settings["nucliadb"] = {
    "image": "nuclia/nucliadb",
    "version": "latest",
    "env": {
        "DRIVER": "local",
        "NUCLIADB_DISABLE_TELEMETRY": "True",
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


@pytest.fixture(scope="session")
def nucliadb():
    if os.environ.get("TEST_LOCAL_NUCLIADB"):
        yield os.environ.get("TEST_LOCAL_NUCLIADB")
    else:
        container = NucliaDB()
        host, port = container.run()
        public_api_url = f"http://{host}:{port}"
        yield public_api_url
        container.stop()


@pytest.fixture(scope="function")
def knowledgebox(nucliadb):
    kbslug = uuid4().hex
    api_path = f"{nucliadb}/api/v1"
    response = requests.post(
        f"{api_path}/kbs",
        json={"slug": kbslug},
        headers={"X-NUCLIADB-ROLES": "MANAGER"},
    )
    assert response.status_code == 201

    kb = KnowledgeBoxObj.parse_raw(response.content)
    kbid = kb.uuid

    url = f"{api_path}/kb/{kbid}"
    client = NucliaDBClient(
        environment=Environment.OSS,
        writer_host=url,
        reader_host=url,
        search_host=url,
        train_host=url,
    )
    yield KnowledgeBox(client)

    response = requests.delete(
        f"{api_path}/kb/{kbid}", headers={"X-NUCLIADB-ROLES": f"MANAGER"}
    )
    assert response.status_code == 200


@dataclass
class SDKFixture:
    host: str
    port: int
    grpc: int
    container: Optional[NucliaDB] = None


async def init_fixture(
    nucliadb: SDKFixture,
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
def nucliadb_imported():
    ndb = NucliaDB()
    host, port = ndb.run()
    network = ndb.container_obj.attrs["NetworkSettings"]

    if os.environ.get("TESTING", "") == "jenkins":
        grpc = 8060
    else:
        service_port = "8060/tcp"
        grpc = network["Ports"][service_port][0]["HostPort"]
    yield SDKFixture(host=host, port=port, grpc=grpc, container=ndb.container_obj)
    ndb.stop()


@pytest.fixture(scope="session")
def docs_fixture(nucliadb_imported: SDKFixture):
    kbid = asyncio.run(init_fixture(nucliadb_imported, "docs", NUCLIA_DOCS_dataset))
    client = NucliaDBClient(
        environment=Environment.OSS,
        url=f"http://{nucliadb_imported.host}:{nucliadb_imported.port}/api/v1/kb/{kbid}",
    )
    return KnowledgeBox(client)
