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
import logging
import signal
from typing import Awaitable, Callable

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
            await finalizer()
        except Exception:
            logger.exception("Error while finalizing", exc_info=True)
