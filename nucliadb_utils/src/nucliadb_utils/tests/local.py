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
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import patch

import pytest

from nucliadb_utils.settings import FileBackendConfig, storage_settings
from nucliadb_utils.storages.local import LocalStorage


@pytest.fixture(scope="function")
def local_storage_settings(tmp_path: Path) -> Iterator[dict[str, Any]]:
    settings = {
        "file_backend": FileBackendConfig.LOCAL,
        "local_files": str((tmp_path / "blob").absolute()),
    }
    with ExitStack() as stack:
        for key, value in settings.items():
            context = patch.object(storage_settings, key, value)
            stack.enter_context(context)

        yield settings


@pytest.fixture(scope="function")
async def local_storage(local_storage_settings: dict[str, Any]):
    assert storage_settings.local_files is not None

    storage = LocalStorage(
        local_testing_files=storage_settings.local_files,
        indexing_bucket=storage_settings.local_indexing_bucket,
    )
    await storage.initialize()
    yield storage
    await storage.finalize()
