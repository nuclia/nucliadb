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
import tempfile

import pytest

from nucliadb_utils.storages.local import LocalStorage
from nucliadb_utils.store import MAIN


@pytest.fixture(scope="function")
async def local_storage(gcs):
    folder = tempfile.TemporaryDirectory()
    storage = LocalStorage(local_testing_files=folder.name)

    MAIN["storage"] = storage
    await storage.initialize()
    yield storage
    await storage.finalize()
    folder.cleanup()
    if "storage" in MAIN:
        del MAIN["storage"]
