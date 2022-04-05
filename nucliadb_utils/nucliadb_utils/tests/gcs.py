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
from typing import Optional

import docker  # type: ignore
import pytest
import requests
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

from nucliadb_utils.storages.gcs import GCSStorage
from nucliadb_utils.store import MAIN

# IMPORTANT!
# Without this, tests running in a remote docker host won't work
DOCKER_ENV_GROUPS = re.search(r"//([^:]+)", docker.from_env().api.base_url)
DOCKER_HOST: Optional[str] = DOCKER_ENV_GROUPS.group(1) if DOCKER_ENV_GROUPS else None  # type: ignore

images.settings["gcs"] = {
    "image": "fsouza/fake-gcs-server",
    "version": "v1.30.1",
    "options": {
        "command": f"-scheme http -external-url http://{DOCKER_HOST}:4443 -port 4443",
        "ports": {"4443": "4443"},
    },
}


class GCS(BaseImage):
    name = "gcs"
    port = 4443

    def check(self):
        try:
            response = requests.get(
                f"http://{self.host}:{self.get_port()}/storage/v1/b"
            )
            return response.status_code == 200
        except:
            return False


@pytest.fixture(scope="session")
def gcs():
    container = GCS()
    host, port = container.run()
    public_api_url = f"http://{host}:{port}"
    yield public_api_url
    container.stop()


@pytest.fixture(scope="function")
async def gcs_storage(gcs):
    storage = GCSStorage(
        account_credentials=None,
        bucket="test_{kbid}",
        location="location",
        project="project",
        executor=ThreadPoolExecutor(1),
        deadletter_bucket="deadletters",
        indexing_bucket="indexing",
        labels={},
        url=gcs,
    )
    MAIN["storage"] = storage
    await storage.initialize()
    yield storage
    await storage.finalize()
    if "storage" in MAIN:
        del MAIN["storage"]
