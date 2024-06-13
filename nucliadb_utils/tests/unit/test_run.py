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

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nucliadb_utils import run

pytestmark = pytest.mark.asyncio


async def test_run_until_exit():
    loop = MagicMock()
    with patch("nucliadb_utils.run.asyncio.get_running_loop", return_value=loop):
        finalizer = AsyncMock()
        asyncio.create_task(run.run_until_exit([finalizer], sleep=0.01))

        await asyncio.sleep(0.05)

        assert len(loop.add_signal_handler.mock_calls) == 2
        callback = loop.add_signal_handler.mock_calls[0].args[1]
        callback()  # cause exit

        await asyncio.sleep(0.05)

        finalizer.assert_called_once()


async def test_run_until_exit_handles_hard_exit():
    loop = MagicMock()
    with patch("nucliadb_utils.run.asyncio.get_running_loop", return_value=loop):
        finalizer = AsyncMock(side_effect=Exception())
        task = asyncio.create_task(run.run_until_exit([finalizer]))

        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.sleep(0.05)

        finalizer.assert_called_once()
