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

from nats.aio.client import Msg

logger = logging.getLogger(__name__)


class MessageProgressUpdater:
    """
    Context manager to send progress updates to NATS.

    This should allow lower ack_wait time settings without causing
    messages to be redelivered.
    """

    _task: asyncio.Task

    def __init__(self, msg: Msg, timeout: float):
        self.msg = msg
        self.timeout = timeout

    async def __aenter__(self):
        task_name = f"MessageProgressUpdater: {id(self)}"
        self._task = asyncio.create_task(self._progress(), name=task_name)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:  # pragma: no cover
            pass
        except Exception:  # pragma: no cover
            pass

    async def _progress(self):
        while True:
            try:
                await asyncio.sleep(self.timeout)
                if self.msg._ackd:  # all done, do not mark with in_progress
                    return
                await self.msg.in_progress()
            except (RuntimeError, asyncio.CancelledError):
                return
            except Exception:  # pragma: no cover
                logger.exception("Error sending task progress to NATS")
