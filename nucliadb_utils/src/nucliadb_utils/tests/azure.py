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
from collections.abc import AsyncIterator, Generator, Iterator
from contextlib import ExitStack
from dataclasses import dataclass
from typing import Any, cast
from unittest.mock import patch

import pytest
from pytest_docker_fixtures import images
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore[import-untyped]

from nucliadb_utils.settings import FileBackendConfig, StorageSettings, storage_settings
from nucliadb_utils.storages.azure import AzureStorage
from nucliadb_utils.storages.settings import Settings as ExtendedSettings
from nucliadb_utils.storages.settings import settings as extended_storage_settings

images.settings = cast(dict[str, Any], images.settings)
images.settings["azurite"] = {
    "image": "mcr.microsoft.com/azure-storage/azurite",
    "version": "3.31.0",
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

    _errors = 0

    def check(self):
        try:
            from azure.storage.blob import BlobServiceClient

            container_port = self.port
            host_port = self.get_port(port=container_port)
            conn_string = get_connection_string(self.host, host_port)
            client = BlobServiceClient.from_connection_string(conn_string)
            container_client = client.get_container_client("foo")
            container_client.create_container()
            container_client.delete_container()
            return True
        except Exception as ex:
            self._errors += 1
            if self._errors > 10:
                raise
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
    assert container.container_obj
    try:
        yield AzuriteFixture(
            host=host,
            port=port,
            container=container.container_obj,
            connection_string=get_connection_string(host, port),
            account_url=f"https://devstoreaccount1.blob.core.windows.net",
        )
    finally:
        container.stop()


@pytest.fixture(scope="session")
def session_azure_storage_settings(
    azurite: AzuriteFixture,
) -> Iterator[tuple[dict[str, Any], dict[str, Any]]]:
    settings: dict[str, Any] = {
        "file_backend": FileBackendConfig.AZURE,
        "azure_account_url": azurite.account_url,
        "azure_connection_string": azurite.connection_string,
    }
    extended_settings: dict[str, Any] = {
        "azure_deadletter_bucket": "deadletter",
        "azure_indexing_bucket": "indexing",
    }

    yield settings, extended_settings


@pytest.fixture(scope="session")
async def session_azure_storage_buckets(session_azure_storage_settings):
    settings, extended_settings = session_azure_storage_settings
    storage = create_storage(StorageSettings(**settings), ExtendedSettings(**extended_settings))
    await storage.initialize()
    await storage.create_bucket("nidx")
    await storage.finalize()


@pytest.fixture(scope="function")
def azure_storage_settings(
    azurite: AzuriteFixture, session_azure_storage_settings: tuple[dict[str, Any], dict[str, Any]]
) -> Iterator[dict[str, Any]]:
    settings, extended_settings = session_azure_storage_settings
    with ExitStack() as stack:
        for key, value in settings.items():
            context = patch.object(storage_settings, key, value)
            stack.enter_context(context)
        for key, value in extended_settings.items():
            context = patch.object(extended_storage_settings, key, value)
            stack.enter_context(context)
        yield settings | extended_settings


def create_storage(settings, extended_settings):
    assert settings.azure_account_url is not None
    return AzureStorage(
        account_url=settings.azure_account_url,
        kb_account_url=settings.azure_kb_account_url or settings.azure_account_url,
        connection_string=settings.azure_connection_string,
    )


@pytest.fixture(scope="function")
async def azure_storage(
    azurite, azure_storage_settings: dict[str, Any], session_azure_storage_buckets
) -> AsyncIterator[AzureStorage]:
    storage = create_storage(storage_settings, extended_storage_settings)
    await storage.initialize()
    yield storage
    await storage.finalize()
