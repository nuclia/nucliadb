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

from nucliadb.common.maindb.utils import settings, setup_driver
from nucliadb_utils.exceptions import ConfigurationError
from nucliadb_utils.store import MAIN


@pytest.fixture(autouse=True)
def reset_driver_utils():
    MAIN.pop("driver", None)
    yield
    MAIN.pop("driver", None)


@pytest.mark.asyncio
async def test_setup_driver_redis():
    mock = AsyncMock(initialized=False)
    with patch.object(settings, "driver", "redis"), patch.object(
        settings, "driver_redis_url", "driver_redis_url"
    ), patch("nucliadb.common.maindb.utils.RedisDriver", return_value=mock):
        assert await setup_driver() == mock
        mock.initialize.assert_awaited_once()


@pytest.mark.asyncio
async def test_setup_driver_tikv():
    mock = AsyncMock(initialized=False)
    with patch.object(settings, "driver", "tikv"), patch.object(
        settings, "driver_tikv_url", "driver_tikv_url"
    ), patch("nucliadb.common.maindb.utils.TiKVDriver", return_value=mock):
        assert await setup_driver() == mock
        mock.initialize.assert_awaited_once()


@pytest.mark.asyncio
async def test_setup_driver_pg():
    mock = AsyncMock(initialized=False)
    with patch.object(settings, "driver", "pg"), patch.object(
        settings, "driver_pg_url", "driver_pg_url"
    ), patch("nucliadb.common.maindb.utils.PGDriver", return_value=mock):
        assert await setup_driver() == mock
        mock.initialize.assert_awaited_once()


@pytest.mark.asyncio
async def test_setup_driver_local():
    mock = AsyncMock(initialized=False)
    with patch.object(settings, "driver", "local"), patch.object(
        settings, "driver_local_url", "driver_local_url"
    ), patch("nucliadb.common.maindb.utils.LocalDriver", return_value=mock):
        assert await setup_driver() == mock
        mock.initialize.assert_awaited_once()


@pytest.mark.asyncio
async def test_setup_driver_error():
    with patch.object(settings, "driver", "pg"), patch.object(
        settings, "driver_pg_url", None
    ), pytest.raises(ConfigurationError):
        await setup_driver()
