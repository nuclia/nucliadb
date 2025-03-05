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
import os
from typing import Any, Iterator, Type
from unittest.mock import Mock

import pytest
from pytest_lazy_fixtures import lazy_fixture

from nucliadb_utils.storages.azure import AzureStorage
from nucliadb_utils.storages.gcs import GCSStorage
from nucliadb_utils.storages.local import LocalStorage
from nucliadb_utils.storages.s3 import S3Storage
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


def get_testing_storage_backend(default="gcs"):
    return os.environ.get("TESTING_STORAGE_BACKEND", default)


def lazy_storage_fixture():
    backend = get_testing_storage_backend()
    fixture_name = f"{backend}_storage"
    return [lazy_fixture.lf(fixture_name)]


@pytest.fixture(scope="function", params=lazy_storage_fixture())
async def storage(request):
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
        storage_backend_map: dict[Type, str] = {
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
