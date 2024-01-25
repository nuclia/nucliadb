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
import random
import time

import pytest

from nucliadb_utils.asyncio_utils import ConcurrentRunner, run_concurrently


async def test_run_concurrently():
    async def mycoro(value):
        await asyncio.sleep(random.uniform(0.0, 0.5))
        return value

    results = await run_concurrently(mycoro(i) for i in range(10))
    assert results == list(range(10))


async def test_concurrent_runner():
    async def mycoro(value):
        await asyncio.sleep(0.5)
        return value

    # Test without max_tasks: execution time should be less than 1 second
    # as both should be executed in concurrently
    runner = ConcurrentRunner()
    runner.schedule(mycoro(1))
    runner.schedule(mycoro(2))
    start = time.perf_counter()
    results = await runner.wait()
    end = time.perf_counter()
    assert results == [1, 2]
    assert end - start < 1

    # Test with max_tasks: execution time should be at least 1 second
    runner = ConcurrentRunner(max_tasks=1)
    runner.schedule(mycoro(1))
    runner.schedule(mycoro(2))
    start = time.perf_counter()
    results = await runner.wait()
    end = time.perf_counter()
    assert results == [1, 2]
    assert end - start >= 1

    # Exception should be raised if any of the coroutines raises an exception
    with pytest.raises(ValueError):

        async def coro_with_exception():
            raise ValueError("Test")

        runner = ConcurrentRunner()
        runner.schedule(coro_with_exception())
        await runner.wait()
