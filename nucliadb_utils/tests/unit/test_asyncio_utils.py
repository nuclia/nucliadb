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
import random
import time
from typing import Coroutine

import pytest

from nucliadb_utils.asyncio_utils import ConcurrentRunner, run_concurrently


async def test_run_concurrently():
    async def mycoro(value):
        await asyncio.sleep(random.uniform(0.0, 0.5))
        return value

    tasks: list[Coroutine] = [mycoro(i) for i in range(10)]
    results = await run_concurrently(tasks)
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
