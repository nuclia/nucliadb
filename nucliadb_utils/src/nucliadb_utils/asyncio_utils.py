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
from collections.abc import Coroutine
from typing import Any, Sequence

from nucliadb_utils import logger


class ConcurrentRunner:
    """
    Runs a list of coroutines concurrently, with a maximum number of tasks running.
    Returns the results of the coroutines in the order they were scheduled.
    """

    def __init__(self, max_tasks: int | None = None):
        self._tasks: list[asyncio.Task] = []
        self.max_tasks = asyncio.Semaphore(max_tasks) if max_tasks is not None else None

    async def run_coroutine(self, coro: Coroutine):
        if self.max_tasks is None:
            return await coro
        else:
            async with self.max_tasks:
                return await coro

    def schedule(self, coro: Coroutine):
        # Use task name as a way to sort the results
        task_name = str(len(self._tasks))
        task = asyncio.create_task(self.run_coroutine(coro), name=task_name)
        self._tasks.append(task)

    async def wait(self) -> list[Any]:
        results: list[Any] = []
        done, pending = await asyncio.wait(self._tasks)
        if len(pending) > 0:
            logger.warning(f"ConcurrentRunner: {len(pending)} tasks were pending")

        sorted_done = sorted(done, key=lambda task: int(task.get_name()))
        done_task: asyncio.Task
        for done_task in sorted_done:
            if done_task.exception() is not None:
                raise done_task.exception()  # type: ignore
            results.append(done_task.result())
        return results


async def run_concurrently(tasks: Sequence[Coroutine], max_concurrent: int | None = None) -> list[Any]:
    """
    Runs a list of coroutines concurrently, with a maximum number of tasks running.
    Returns the results of the coroutines in the order they were scheduled.
    """
    if len(tasks) == 0:
        return []

    if len(tasks) == 1:
        return [await tasks[0]]

    runner = ConcurrentRunner(max_tasks=max_concurrent)
    for task in tasks:
        runner.schedule(task)
    return await runner.wait()
