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
import os
import sys
from collections.abc import Iterator
from typing import Any
from unittest.mock import Mock

import pytest
from pytest import FixtureRequest
from pytest_lazy_fixtures import lazy_fixture

from nucliadb_utils.storages.azure import AzureStorage
from nucliadb_utils.storages.gcs import GCSStorage
from nucliadb_utils.storages.local import LocalStorage
from nucliadb_utils.storages.s3 import S3Storage
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import Utility, clean_utility, set_utility


@pytest.fixture(scope="function")
def onprem_nucliadb():
    from nucliadb_utils.settings import nuclia_settings

    original = nuclia_settings.onprem
    nuclia_settings.onprem = True
    yield
    nuclia_settings.onprem = original


@pytest.fixture(scope="function")
def hosted_nucliadb():
    from nucliadb_utils.settings import nuclia_settings

    original = nuclia_settings.onprem
    nuclia_settings.onprem = False
    yield
    nuclia_settings.onprem = original


def get_testing_storage_backend() -> str:
    """
    Default to gcs for linux users and s3 for macOS users. This is because some
    tests fail on macOS with the gcs backend with a weird nidx error (to be looked into).
    """
    if sys.platform.startswith("darwin"):
        default = "s3"
    else:
        default = "gcs"
    selected = os.environ.get("TESTING_STORAGE_BACKEND", default)
    return selected


def lazy_storage_fixture(pattern: str):
    backend = get_testing_storage_backend()
    fixture_name = pattern.format(backend=backend)
    return [lazy_fixture.lf(fixture_name)]


@pytest.fixture(scope="function", params=lazy_storage_fixture("{backend}_storage"))
def storage(request: FixtureRequest) -> Iterator[Storage]:
    """
    Generic storage fixture that allows us to run the same tests for different storage backends.
    """
    storage_driver = request.param
    set_utility(Utility.STORAGE, storage_driver)

    yield storage_driver

    clean_utility(Utility.STORAGE)


@pytest.fixture(scope="function")
def storage_settings(request, storage) -> Iterator[dict[str, Any]]:
    """Useful fixture that returns the settings used in the generic `storage`
    fixture.

    This becomes useful when `storage` is overwritten programatically. Tests or
    fixtures depending on generic storage settings can use this fixutre to get
    the appropiate settings, being agnostic to the specific storage used.

    """
    if isinstance(storage, Mock):
        yield {}
    else:
        storage_backend_map: dict[type, str] = {
            AzureStorage: "azure",
            GCSStorage: "gcs",
            LocalStorage: "local",
            S3Storage: "s3",
        }

        backend = storage_backend_map.get(type(storage), None)
        if backend is None:
            raise Exception(f"Unknown storage configured ({type(storage)}), can't get settings")

        fixture_name = f"{backend}_storage_settings"
        # print("Using storage settings:", fixture_name)
        assert fixture_name in request.fixturenames
        settings: dict[str, Any] = request.getfixturevalue(fixture_name)
        yield settings


@pytest.fixture(scope="session", params=lazy_storage_fixture("session_{backend}_storage_settings"))
def session_storage_settings(request) -> Iterator[tuple[dict[str, Any], dict[str, Any]]]:
    yield request.param


@pytest.fixture(scope="session", params=lazy_storage_fixture("session_{backend}_storage_buckets"))
def session_storage_buckets(request) -> Iterator[tuple[dict[str, Any], dict[str, Any]]]:
    yield request.param
