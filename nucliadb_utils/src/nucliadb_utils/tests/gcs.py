# Copyright 2021 Bosutech XXI S.L.
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
import re
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import ExitStack
from typing import Any, AsyncIterator, Iterator
from unittest.mock import patch

import docker  # type: ignore[import-untyped]
import pytest
import requests
from pytest_docker_fixtures import images
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore[import-untyped]

from nucliadb_utils.settings import FileBackendConfig, StorageSettings, storage_settings
from nucliadb_utils.storages.gcs import GCSStorage
from nucliadb_utils.storages.settings import Settings as ExtendedSettings
from nucliadb_utils.storages.settings import settings as extended_storage_settings
from nucliadb_utils.tests import free_port

# IMPORTANT!
# Without this, tests running in a remote docker host won't work
DOCKER_ENV_GROUPS = re.search(r"//([^:]+)", docker.from_env().api.base_url)
DOCKER_HOST: str | None = DOCKER_ENV_GROUPS.group(1) if DOCKER_ENV_GROUPS else None

# This images has the XML API in this PR https://github.com/fsouza/fake-gcs-server/pull/1164
# which is needed because the Rust crate object_store uses it
# If this gets merged, we can switch to the official image
images.settings["gcs"] = {
    "image": "tustvold/fake-gcs-server",
    "version": "latest",
    "options": {
        "command": f"-scheme http -external-url http://{DOCKER_HOST}:{{port}} -port {{port}} -public-host {{network_gateway}}:{{port}}"
    },
}


class GCS(BaseImage):
    name = "gcs"

    def __init__(self):
        super().__init__()
        self.port = free_port()

    def get_image_options(self):
        options = super().get_image_options()
        options["ports"] = {str(self.port): str(self.port)}

        # configure external URLs to be accessible outside the container using
        # the docker network gateway IP address
        network_gateway = (
            docker.from_env().networks.get(self.default_network).attrs["IPAM"]["Config"][0]["Gateway"]
        )
        options["command"] = options["command"].format(network_gateway=network_gateway, port=self.port)

        return options

    def check(self):
        try:
            response = requests.get(f"http://{self.host}:{self.get_port()}/storage/v1/b")
            return response.status_code == 200
        except Exception:  # pragma: no cover
            return False


@pytest.fixture(scope="session")
def gcs() -> Iterator[str]:
    container = GCS()
    host, port = container.run()
    if running_in_mac_os():
        public_api_url = f"http://{host}:{port}"
    else:
        public_api_url = f"http://172.17.0.1:{port}"
    yield public_api_url
    container.stop()


def running_in_mac_os() -> bool:
    import os

    return os.uname().sysname == "Darwin"


@pytest.fixture(scope="session")
async def session_gcs_storage_settings(gcs: str) -> AsyncIterator[tuple[dict[str, Any], dict[str, Any]]]:
    settings: dict[str, Any] = {
        "file_backend": FileBackendConfig.GCS,
        "gcs_endpoint_url": gcs,
        "gcs_base64_creds": None,
        "gcs_bucket": "test_{kbid}",
        "gcs_location": "location",
        "gcs_anonymous": True,
    }
    extended_settings: dict[str, Any] = {
        "gcs_deadletter_bucket": "deadletter",
        "gcs_indexing_bucket": "indexing",
        "gcs_threads": 1,
    }

    storage = create_storage(StorageSettings(**settings), ExtendedSettings(**extended_settings))
    await storage.initialize()
    await storage.create_bucket("nidx")
    await storage.finalize()

    yield settings, extended_settings


@pytest.fixture(scope="function")
def gcs_storage_settings(
    gcs: str, session_gcs_storage_settings: tuple[dict[str, Any], dict[str, Any]]
) -> Iterator[dict[str, Any]]:
    settings, extended_settings = session_gcs_storage_settings
    with ExitStack() as stack:
        for key, value in settings.items():
            context = patch.object(storage_settings, key, value)
            stack.enter_context(context)
        for key, value in extended_settings.items():
            context = patch.object(extended_storage_settings, key, value)
            stack.enter_context(context)

        yield settings | extended_settings


def create_storage(settings, extended_settings):
    return GCSStorage(
        url=settings.gcs_endpoint_url,
        account_credentials=settings.gcs_base64_creds,
        bucket=settings.gcs_bucket,
        location=settings.gcs_location,
        project=settings.gcs_project,
        executor=ThreadPoolExecutor(extended_settings.gcs_threads),
        deadletter_bucket=extended_settings.gcs_deadletter_bucket,
        indexing_bucket=extended_settings.gcs_indexing_bucket,
        labels=settings.gcs_bucket_labels,
        anonymous=settings.gcs_anonymous,
    )


@pytest.fixture(scope="function")
async def gcs_storage(gcs: str, gcs_storage_settings: dict[str, Any]) -> AsyncIterator[GCSStorage]:
    storage = create_storage(storage_settings, extended_storage_settings)
    await storage.initialize()
    yield storage
    await storage.finalize()
