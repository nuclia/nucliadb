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
import logging
from typing import AsyncGenerator, Awaitable, Callable

from nucliadb_telemetry.errors import capture_exception

logger = logging.getLogger(__name__)


async def async_gen_lookahead(
    gen: AsyncGenerator[bytes, None],
) -> AsyncGenerator[tuple[bytes, bool], None]:
    """Async generator that yields the next chunk and whether it's the last one.
    Empty chunks are ignored.

    """
    buffered_chunk = None
    async for chunk in gen:
        if buffered_chunk is None:
            # Buffer the first chunk
            buffered_chunk = chunk
            continue

        if chunk is None or len(chunk) == 0:
            continue

        # Yield the previous chunk and buffer the current one
        yield buffered_chunk, False
        buffered_chunk = chunk

    # Yield the last chunk if there is one
    if buffered_chunk is not None:
        yield buffered_chunk, True


class MessageProgressUpdater:
    """
    Context manager to send progress updates to NATS.

    This should allow lower ack_wait time settings without causing
    messages to be redelivered.
    """

    _task: asyncio.Task

    def __init__(self, seqid: str, cb: Callable[[], Awaitable[bool]], timeout: float):
        self.seqid = seqid
        self.cb = cb
        self.timeout = timeout

    def start(self):
        task_name = f"MessageProgressUpdater: {id(self)} (seqid={self.seqid})"
        self._task = asyncio.create_task(self._progress(), name=task_name)

    async def end(self):
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:  # pragma: no cover
            logger.info("MessageProgressUpdater cancelled")
            pass
        except Exception as exc:  # pragma: no cover
            capture_exception(exc)
            logger.exception("Error in MessageProgressUpdater")
            pass

    async def __aenter__(self):
        self.start()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.end()

    async def _progress(self):
        while True:
            try:
                await asyncio.sleep(self.timeout)
                done = await self.cb()
                if done:  # all done, do not mark with in_progress
                    return
            except (RuntimeError, asyncio.CancelledError):
                return
            except Exception:  # pragma: no cover
                logger.exception("Error sending task progress to NATS")
