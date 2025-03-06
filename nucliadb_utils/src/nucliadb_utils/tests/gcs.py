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
import re
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import ExitStack
from typing import Any, Iterator, Optional
from unittest.mock import patch

import docker  # type: ignore  # type: ignore
import pytest
import requests
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

from nucliadb_utils.settings import FileBackendConfig, storage_settings
from nucliadb_utils.storages.gcs import GCSStorage
from nucliadb_utils.storages.settings import settings as extended_storage_settings
from nucliadb_utils.tests import free_port

# IMPORTANT!
# Without this, tests running in a remote docker host won't work
DOCKER_ENV_GROUPS = re.search(r"//([^:]+)", docker.from_env().api.base_url)
DOCKER_HOST: Optional[str] = DOCKER_ENV_GROUPS.group(1) if DOCKER_ENV_GROUPS else None

# This images has the XML API in this PR https://github.com/fsouza/fake-gcs-server/pull/1164
# which is needed because the Rust crate object_store uses it
# If this gets merged, we can switch to the official image
images.settings["gcs"] = {
    "image": "tustvold/fake-gcs-server",
    "version": "latest",
    "options": {},
}


class GCS(BaseImage):
    name = "gcs"

    def __init__(self):
        super().__init__()
        self.port = free_port()

    def get_image_options(self):
        options = super().get_image_options()
        options["ports"] = {str(self.port): str(self.port)}
        options["command"] = (
            f"-scheme http -external-url http://{DOCKER_HOST}:{self.port} -port {self.port} -public-host 172.17.0.1:{self.port}"
        )
        return options

    def check(self):
        try:
            response = requests.get(f"http://{self.host}:{self.get_port()}/storage/v1/b")
            return response.status_code == 200
        except Exception:  # pragma: no cover
            return False


@pytest.fixture(scope="session")
def gcs():
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


@pytest.fixture(scope="function")
def gcs_storage_settings(gcs) -> Iterator[dict[str, Any]]:
    settings = {
        "file_backend": FileBackendConfig.GCS,
        "gcs_endpoint_url": gcs,
        "gcs_base64_creds": None,
        "gcs_bucket": "test_{kbid}",
        "gcs_location": "location",
    }
    extended_settings = {
        "gcs_deadletter_bucket": "deadletter",
        "gcs_indexing_bucket": "indexing",
    }
    with ExitStack() as stack:
        for key, value in settings.items():
            context = patch.object(storage_settings, key, value)
            stack.enter_context(context)
        for key, value in extended_settings.items():
            context = patch.object(extended_storage_settings, key, value)
            stack.enter_context(context)

        yield settings | extended_settings


@pytest.fixture(scope="function")
async def gcs_storage(gcs, gcs_storage_settings: dict[str, Any]):
    storage = GCSStorage(
        url=storage_settings.gcs_endpoint_url,
        account_credentials=storage_settings.gcs_base64_creds,
        bucket=storage_settings.gcs_bucket,
        location=storage_settings.gcs_location,
        project=storage_settings.gcs_project,
        executor=ThreadPoolExecutor(1),
        deadletter_bucket=extended_storage_settings.gcs_deadletter_bucket,
        indexing_bucket=extended_storage_settings.gcs_indexing_bucket,
        labels=storage_settings.gcs_bucket_labels,
        anonymous=True,
    )
    await storage.initialize()
    await storage.create_bucket("nidx")
    yield storage
    await storage.finalize()
