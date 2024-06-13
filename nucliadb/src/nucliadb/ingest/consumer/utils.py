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
from typing import Callable, Coroutine


class DelayedTaskHandler:
    """
    This class is used to schedule a task to run after a delay. If the same
    key is scheduled multiple times, the handler is ignored.
    """

    loop: asyncio.AbstractEventLoop

    def __init__(self, delay: float = 5.0):
        self.delay = delay
        self.to_process: dict[
            str, tuple[asyncio.TimerHandle, Callable[[], Coroutine[None, None, None]]]
        ] = {}
        self.outstanding_tasks: dict[str, asyncio.Task] = {}

    async def initialize(self) -> None:
        self.loop = asyncio.get_running_loop()

    async def finalize(self) -> None:
        # finish all the rest of the messages
        for timer, handler in self.to_process.values():
            timer.cancel()
            await handler()

        for task in list(self.outstanding_tasks.values()):
            await task

    def schedule(self, key: str, handler: Callable[[], Coroutine[None, None, None]]) -> None:
        if key in self.to_process:
            # already waiting to process this key, ignore
            return

        timer = self.loop.call_later(self.delay, self.run, key, handler)
        self.to_process[key] = (timer, handler)

    def run(self, key: str, handler: Callable[[], Coroutine[None, None, None]]) -> None:
        self.to_process.pop(key)
        if key in self.outstanding_tasks:
            # already running for this kb, restart the timer
            self.schedule(key, handler)
            return

        async def outer_task() -> None:
            try:
                await handler()
            finally:
                if key in self.outstanding_tasks:
                    self.outstanding_tasks.pop(key)

        self.outstanding_tasks[key] = self.loop.create_task(outer_task())
