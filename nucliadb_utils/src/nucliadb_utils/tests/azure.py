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
from dataclasses import dataclass
from typing import Generator

import pytest
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

from nucliadb_utils.storages.azure import AzureStorage
from nucliadb_utils.store import MAIN
from nucliadb_utils.utilities import Utility

images.settings["azurite"] = {
    "image": "mcr.microsoft.com/azure-storage/azurite",
    "version": "3.30.0",
    "options": {
        "ports": {"10000": None},
        "command": " ".join(
            [
                # To start the blob service only -- by default is on port 10000
                "azurite-blob",
                # So we can access it from outside the container
                "--blobHost 0.0.0.0",
            ]
        ),
    },
    "env": {},
}


class Azurite(BaseImage):
    name = "azurite"
    port = 10000

    def check(self):
        try:
            from azure.storage.blob import BlobServiceClient  # type: ignore

            container_port = self.port
            host_port = self.get_port(port=container_port)
            conn_string = get_connection_string(self.host, host_port)

            client = BlobServiceClient.from_connection_string(conn_string)
            container_client = client.get_container_client("foo")
            container_client.create_container()
            container_client.delete_container()
            return True
        except Exception as ex:
            print(ex)
            return False


@dataclass
class AzuriteFixture:
    host: str
    port: int
    container: BaseImage
    connection_string: str
    account_url: str


def get_connection_string(host, port) -> str:
    """
    We're using the default Azurite credentials for testing purposes.
    """
    parts = [
        "DefaultEndpointsProtocol=http",
        "AccountName=devstoreaccount1",
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        f"BlobEndpoint=http://{host}:{port}/devstoreaccount1",
    ]
    return ";".join(parts)


@pytest.fixture(scope="session")
def azurite() -> Generator[AzuriteFixture, None, None]:
    container = Azurite()
    host, port = container.run()
    try:
        yield AzuriteFixture(
            host=host,
            port=port,
            container=container.container_obj,
            connection_string=get_connection_string(host, port),
            account_url=f"http://{host}:{port}/devstoreaccount1",
        )
    finally:
        container.stop()


@pytest.fixture(scope="function")
async def azure_storage(azurite):
    storage = AzureStorage(
        account_url=azurite.account_url,
        connection_string=azurite.connection_string,
    )
    MAIN[Utility.STORAGE] = storage
    await storage.initialize()
    try:
        yield storage
    finally:
        await storage.finalize()
        if Utility.STORAGE in MAIN:
            del MAIN[Utility.STORAGE]
