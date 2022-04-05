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
import pytest
import requests
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

from nucliadb_utils.storages.s3 import S3Storage
from nucliadb_utils.store import MAIN

images.settings["s3"] = {
    "image": "localstack/localstack",
    "version": "0.12.18",
    "env": {"SERVICES": "s3"},
    "options": {
        "ports": {"4566": None, "4571": None},
    },
}


class S3(BaseImage):
    name = "s3"
    port = 4566

    def check(self):
        try:
            response = requests.get(f"http://{self.host}:{self.get_port()}")
            return response.status_code == 404
        except:
            return False


@pytest.fixture(scope="session")
def s3():
    container = S3()
    host, port = container.run()
    public_api_url = f"http://{host}:{port}"
    yield public_api_url
    container.stop()


@pytest.fixture(scope="function")
async def s3_storage(s3):
    storage = S3Storage(
        aws_client_id="",
        aws_client_secret="",
        deadletter_bucket="deadletter",
        indexing_bucket="indexing",
        endpoint_url=s3,
        verify_ssl=False,
        use_ssl=False,
        region_name=None,
        bucket="test",
    )
    await storage.initialize()
    MAIN["storage"] = storage
    yield storage
    await storage.finalize()
    del MAIN["storage"]
