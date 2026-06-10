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
from collections.abc import AsyncIterator, Iterator
from contextlib import ExitStack
from typing import Any, cast
from unittest.mock import patch

import pytest
import requests
from pytest_docker_fixtures import images
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore[import-untyped]

from nucliadb_utils.settings import FileBackendConfig, storage_settings
from nucliadb_utils.storages.s3 import S3Storage
from nucliadb_utils.storages.settings import settings as extended_storage_settings

images.settings = cast(dict[str, Any], images.settings)
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
        except Exception:
            return False


def running_in_mac_os() -> bool:
    import os

    return os.uname().sysname == "Darwin"


@pytest.fixture(scope="session")
def s3() -> Iterator[str]:
    container = S3()
    host, port = container.run()
    if running_in_mac_os():
        public_api_url = f"http://{host}:{port}"
    else:
        public_api_url = f"http://172.17.0.1:{port}"
    yield public_api_url
    container.stop()


@pytest.fixture(scope="session")
def session_s3_storage_settings(s3: str) -> Iterator[tuple[dict[str, Any], dict[str, Any]]]:
    settings = {
        "file_backend": FileBackendConfig.S3,
        "s3_endpoint": s3,
        "s3_client_id": "fake",
        "s3_client_secret": "fake",
        "s3_ssl": False,
        "s3_verify_ssl": False,
        "s3_verify_ssl_certificate": None,
        "s3_region_name": None,
        "s3_bucket": "test-{kbid}",
        "s3_kms_key_id": "fake-kms-key-id",
        # "s3_bucket_tags": {
        #     "testTag": "test",
        # },
    }
    extended_settings = {
        "s3_indexing_bucket": "indexing",
        "s3_deadletter_bucket": "deadletter",
    }
    yield settings, extended_settings


@pytest.fixture(scope="function")
async def s3_storage_settings(
    s3: str, session_s3_storage_settings: tuple[dict[str, Any], dict[str, Any]]
) -> AsyncIterator[dict[str, Any]]:
    settings, extended_settings = session_s3_storage_settings
    with ExitStack() as stack:
        for key, value in settings.items():
            context = patch.object(storage_settings, key, value)
            stack.enter_context(context)
        for key, value in extended_settings.items():
            context = patch.object(extended_storage_settings, key, value)
            stack.enter_context(context)

        yield settings | extended_settings


@pytest.fixture(scope="function")
async def s3_storage(s3: str, s3_storage_settings: dict[str, Any]) -> AsyncIterator[S3Storage]:
    storage = S3Storage(
        aws_client_id=storage_settings.s3_client_id,
        aws_client_secret=storage_settings.s3_client_secret,
        deadletter_bucket=extended_storage_settings.s3_deadletter_bucket,
        indexing_bucket=extended_storage_settings.s3_indexing_bucket,
        endpoint_url=storage_settings.s3_endpoint,
        use_ssl=storage_settings.s3_ssl,
        verify_ssl=storage_settings.s3_verify_ssl_certificate or storage_settings.s3_verify_ssl,
        region_name=storage_settings.s3_region_name,
        bucket=storage_settings.s3_bucket,
        bucket_tags=storage_settings.s3_bucket_tags,
        kms_key_id=storage_settings.s3_kms_key_id,
    )
    await storage.initialize()
    await storage.create_bucket("nidx")
    yield storage
    await storage.finalize()
