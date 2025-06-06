# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import asyncio
import os
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Optional
from uuid import uuid4

import docker  # type: ignore
import httpx
import pytest
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

import nucliadb_sdk

images.settings["postgresql"]["version"] = "13"
images.settings["postgresql"]["env"]["POSTGRES_PASSWORD"] = "postgres"
images.settings["postgresql"]["env"]["POSTGRES_DB"] = "postgres"

images.settings["nucliadb"] = {
    "image": "nuclia/nucliadb",
    "version": "latest",
    "env": {
        "NUCLIADB_DISABLE_ANALYTICS": "True",
        "dummy_predict": "True",
        "dummy_processing": "True",
        "max_receive_message_length": "40",
        "TEST_SENTENCE_ENCODER": "multilingual-2023-02-21",
        "TEST_RELATIONS": """{"tokens": [{"text": "Nuclia", "ner": "ORG"}]}""",
        "LOG_LEVEL": "DEBUG",
        "DEBUG": "true",
        "INDEX_SEARCHER_REFRESH_INTERVAL": "0.1",
    },
    "options": {"mem_limit": "2g"},
}

NUCLIA_DOCS_dataset = (
    "https://storage.googleapis.com/ncl-testbed-gcp-stage-1/test_nucliadb/nuclia-datasets.export"
)


MB = 1024 * 1024
CHUNK_SIZE = 5 * MB


class NucliaDB(BaseImage):
    name = "nucliadb"
    port = 8080

    def check(self):
        try:
            response = httpx.get(f"http://{self.host}:{self.get_port()}")
            return response.status_code == 200
        except Exception:
            return False


@dataclass
class NucliaFixture:
    host: str
    port: int
    grpc: int
    url: str
    container: Optional[NucliaDB] = None


def get_docker_internal_host():
    """
    This is needed for the case when we are starting a nucliadb container for testing,
    it needs to know the docker internal host to connect to pg container that is on the same network.
    """
    docker_client = docker.from_env(version=BaseImage.docker_version)
    docker_platform_name = docker_client.api.version()["Platform"]["Name"].upper()
    if "GITHUB_ACTION" in os.environ:
        # Valid when using github actions
        docker_internal_host = "172.17.0.1"
    elif docker_platform_name == "DOCKER ENGINE - COMMUNITY":
        # for linux users
        docker_internal_host = "172.17.0.1"
    elif "DOCKER DESKTOP" in docker_platform_name:
        # Valid when using Docker desktop
        docker_internal_host = "host.docker.internal"
    else:
        docker_internal_host = "172.17.0.1"
    return docker_internal_host


@pytest.fixture(scope="session")
def nucliadb(pg):
    pg_host, pg_port = pg
    # Setup the connection to pg for the maindb driver
    url = f"postgresql://postgres:postgres@{pg_host}:{pg_port}/postgres"

    images.settings["nucliadb"]["env"]["DRIVER"] = "PG"
    images.settings["nucliadb"]["env"]["DRIVER_PG_URL"] = url

    if os.environ.get("TEST_LOCAL_NUCLIADB"):
        host = os.environ.get("TEST_LOCAL_NUCLIADB")
        child = None

        # Start a local NucliaDB here
        if host == "START":
            host = "localhost"
            child = subprocess.Popen(
                os.path.join(os.path.dirname(sys.executable), "nucliadb"),
                env=images.settings["nucliadb"]["env"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            # Wait for service ready
            for _ in range(10):
                try:
                    if httpx.get(f"http://localhost:8080").status_code == 200:
                        break
                except Exception:
                    pass
                time.sleep(1)

            if httpx.get(f"http://localhost:8080").status_code != 200:
                raise Exception("No NucliaDB Running")

        yield NucliaFixture(
            host=host,
            port=8080,
            grpc=8030,
            container="local",
            url=f"http://{host}:8080/api",
        )

        if child:
            child.kill()
    else:
        # We need to replace the localhost with the internal docker host to allow container-to-container communication
        images.settings["nucliadb"]["env"]["DRIVER_PG_URL"] = images.settings["nucliadb"]["env"][
            "DRIVER_PG_URL"
        ].replace("localhost", get_docker_internal_host())
        container = NucliaDB()
        host, port = container.run()
        network = container.container_obj.attrs["NetworkSettings"]
        service_port = "8060/tcp"
        grpc = network["Ports"][service_port][0]["HostPort"]

        yield NucliaFixture(
            host=host,
            port=port,
            grpc=grpc,
            container=container.container_obj,
            url=f"http://{host}:{port}/api",
        )
        container.stop()


@pytest.fixture(scope="session")
def sdk(nucliadb: NucliaFixture):
    sdk = nucliadb_sdk.NucliaDB(region="on-prem", url=nucliadb.url)
    return sdk


@pytest.fixture(scope="function")
def sdk_async(nucliadb: NucliaFixture):
    sdk = nucliadb_sdk.NucliaDBAsync(region="on-prem", url=nucliadb.url)
    return sdk


@pytest.fixture(scope="function")
def kb(sdk: nucliadb_sdk.NucliaDB):
    kbslug = uuid4().hex
    kb = sdk.create_knowledge_box(slug=kbslug)

    yield kb

    sdk.delete_knowledge_box(kbid=kb.uuid)


async def init_fixture(
    nucliadb: NucliaFixture,
    dataset_slug: str,
    dataset_location: str,
):
    sdk = nucliadb_sdk.NucliaDB(region="on-prem", url=nucliadb.url)
    slug = uuid.uuid4().hex
    kb_obj = sdk.create_knowledge_box(slug=slug)
    kbid = kb_obj.uuid

    import_resp = httpx.get(dataset_location)
    assert import_resp.status_code == 200, (
        f"Error pulling dataset {dataset_location}:{import_resp.status_code}"
    )
    import_data = import_resp.content

    import_id = sdk.start_import(kbid=kbid, content=import_data).import_id
    assert sdk.import_status(kbid=kbid, import_id=import_id).status.value == "finished"

    return kbid


@pytest.fixture(scope="session")
def docs_dataset(nucliadb: NucliaFixture):
    kbid = asyncio.run(init_fixture(nucliadb, "docs", NUCLIA_DOCS_dataset))
    yield kbid
