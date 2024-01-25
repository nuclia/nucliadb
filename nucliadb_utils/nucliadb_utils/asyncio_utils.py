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
import asyncio
from collections.abc import Coroutine
from typing import Any, Optional

from nucliadb_utils import logger


class ConcurrentRunner:
    """
    Runs a list of coroutines concurrently, with a maximum number of tasks running.
    Returns the results of the coroutines in the order they were scheduled.
    """

    def __init__(self, max_tasks: Optional[int] = None):
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


async def run_concurrently(
    tasks: list[Coroutine], max_concurrent: Optional[int] = None
) -> list[Any]:
    """
    Runs a list of coroutines concurrently, with a maximum number of tasks running.
    Returns the results of the coroutines in the order they were scheduled.
    """
    runner = ConcurrentRunner(max_tasks=max_concurrent)
    for task in tasks:
        runner.schedule(task)
    return await runner.wait()
