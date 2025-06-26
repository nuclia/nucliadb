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

from unittest.mock import AsyncMock, patch

import pytest

from nucliadb.writer import tus
from nucliadb.writer.tus.dm import (
    FileDataManager,
    RedisFileDataManager,
    RedisFileDataManagerFactory,
)


@pytest.fixture()
def valkey():
    mock = AsyncMock()
    with patch("nucliadb.writer.tus.dm.aioredis.from_url", return_value=mock):
        yield mock


async def test_file_data_manager_factory(valkey):
    factory = RedisFileDataManagerFactory("redis://localhost:6379")
    inst = factory()

    assert isinstance(inst, RedisFileDataManager)
    assert inst.redis == valkey

    await factory.finalize()

    valkey.aclose.assert_called_once_with(close_connection_pool=True)


async def test_get_file_data_manager():
    await tus.finalize()  # make sure to clear

    with patch.object(tus.writer_settings, "dm_enabled", False):
        assert isinstance(tus.get_dm(), FileDataManager)


async def test_get_file_data_manager_redis(valkey):
    await tus.finalize()  # make sure to clear

    with patch.object(tus.writer_settings, "dm_enabled", True):
        assert isinstance(tus.get_dm(), RedisFileDataManager)
