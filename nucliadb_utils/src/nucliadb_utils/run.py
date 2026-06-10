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
import inspect
import logging
import signal
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)


async def run_until_exit(finalizers: list[Callable[[], Awaitable[None]]], sleep: float = 1.0):
    exit_event = asyncio.Event()

    def handle_exit(*args):
        exit_event.set()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, handle_exit, signal.SIGINT, None)
    loop.add_signal_handler(signal.SIGTERM, handle_exit, signal.SIGTERM, None)

    try:
        while not exit_event.is_set():
            await asyncio.sleep(sleep)
    except (KeyboardInterrupt, SystemExit, RuntimeError, asyncio.CancelledError):
        # exiting
        ...

    for finalizer in finalizers:
        try:
            if inspect.iscoroutinefunction(finalizer):
                await finalizer()
            else:
                finalizer()
        except Exception:
            logger.exception("Error while finalizing", exc_info=True)
