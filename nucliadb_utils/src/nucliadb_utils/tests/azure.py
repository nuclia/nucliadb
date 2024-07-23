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
from contextlib import ExitStack
from dataclasses import dataclass
from typing import Any, Generator
from unittest.mock import patch

import pytest
from pytest_docker_fixtures import images  # type: ignore  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore  # type: ignore

from nucliadb_utils.settings import FileBackendConfig, storage_settings
from nucliadb_utils.storages.azure import AzureStorage

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
def azure_storage_settings(azurite: AzuriteFixture) -> dict[str, Any]:
    settings = {
        "file_backend": FileBackendConfig.AZURE,
        "azure_account_url": azurite.account_url,
        "azure_connection_string": azurite.connection_string,
    }
    with ExitStack() as stack:
        for key, value in settings.items():
            context = patch.object(storage_settings, key, value)
            stack.enter_context(context)

        yield settings


@pytest.fixture(scope="function")
async def azure_storage(azurite, azure_storage_settings: dict[str, Any]):
    assert storage_settings.azure_account_url is not None

    storage = AzureStorage(
        account_url=storage_settings.azure_account_url,
        connection_string=storage_settings.azure_connection_string,
    )
    await storage.initialize()
    yield storage
    await storage.finalize()
