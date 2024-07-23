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
from os.path import dirname
from typing import AsyncIterator

import pytest

from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.common.maindb.driver import Driver
from nucliadb_utils.storages.settings import settings as storage_settings
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import (
    Utility,
    clean_utility,
    set_utility,
)


@pytest.fixture(scope="function")
async def shard_manager(storage: Storage, maindb_driver: Driver) -> AsyncIterator[KBShardManager]:
    sm = KBShardManager()
    set_utility(Utility.SHARD_MANAGER, sm)

    yield sm

    clean_utility(Utility.SHARD_MANAGER)


@pytest.fixture(scope="function")
async def local_files():
    storage_settings.local_testing_files = f"{dirname(__file__)}"
