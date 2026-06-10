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
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from pytest import TempPathFactory

from nucliadb_utils.settings import FileBackendConfig, storage_settings
from nucliadb_utils.storages.local import LocalStorage
from nucliadb_utils.storages.settings import settings as extended_storage_settings


@pytest.fixture(scope="session")
def local_storage_path(tmp_path_factory: TempPathFactory) -> Iterator[Path]:
    path = tmp_path_factory.mktemp("blob")
    yield path


@pytest.fixture(scope="session")
def session_local_storage_settings(
    local_storage_path: Path,
) -> Iterator[tuple[dict[str, Any], dict[str, Any]]]:
    settings = {
        "file_backend": FileBackendConfig.LOCAL,
        "local_files": str(local_storage_path.absolute()),
    }
    extended_settings = {
        "local_testing_files": str(local_storage_path.absolute()),
    }
    yield settings, extended_settings


@pytest.fixture(scope="function")
def local_storage_settings(
    local_storage_path: Path, session_local_storage_settings: tuple[dict[str, Any], dict[str, Any]]
) -> Iterator[dict[str, Any]]:
    settings, extended_settings = session_local_storage_settings
    with ExitStack() as stack:
        for key, value in settings.items():
            context = patch.object(storage_settings, key, value)
            stack.enter_context(context)
        for key, value in extended_settings.items():
            context = patch.object(extended_storage_settings, key, value)
            stack.enter_context(context)

        yield settings | extended_settings


@pytest.fixture(scope="function")
async def local_storage(local_storage_settings: dict[str, Any]) -> AsyncIterator[LocalStorage]:
    assert storage_settings.local_files is not None
    storage = LocalStorage(
        local_testing_files=storage_settings.local_files,
        indexing_bucket=storage_settings.local_indexing_bucket,
    )
    await storage.initialize()
    yield storage
    await storage.finalize()
