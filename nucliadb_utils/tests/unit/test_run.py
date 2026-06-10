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

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from nucliadb_utils import run


async def test_run_until_exit():
    loop = MagicMock()
    with patch("nucliadb_utils.run.asyncio.get_running_loop", return_value=loop):
        finalizer = AsyncMock()
        task = asyncio.create_task(run.run_until_exit([finalizer], sleep=0.01))

        await asyncio.sleep(0.05)

        assert len(loop.add_signal_handler.mock_calls) == 2
        callback = loop.add_signal_handler.mock_calls[0].args[1]
        callback()  # cause exit

        await asyncio.sleep(0.05)

        finalizer.assert_called_once()

        task.cancel()


async def test_run_until_exit_handles_hard_exit():
    loop = MagicMock()
    with patch("nucliadb_utils.run.asyncio.get_running_loop", return_value=loop):
        finalizer = AsyncMock(side_effect=Exception())
        task = asyncio.create_task(run.run_until_exit([finalizer]))

        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.sleep(0.05)

        finalizer.assert_called_once()
