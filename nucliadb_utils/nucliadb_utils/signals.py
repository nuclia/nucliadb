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
from collections.abc import Awaitable
from enum import Enum
from inspect import iscoroutinefunction
from typing import Any, Callable, Type

from nucliadb_telemetry.errors import capture_exception
from nucliadb_utils import logger


class ListenerPriority(Enum):
    DONT_CARE = 0
    CRITICAL = 1


class Signal:
    def __init__(self, payload_model: Type):
        self.payload_model_type = payload_model
        self.callbacks: dict[str, tuple[Callable[..., Awaitable], int]] = {}

    def add_listener(
        self,
        listener_id: str,
        cb: Callable[..., Awaitable],
        priority: ListenerPriority = ListenerPriority.DONT_CARE,
    ):
        if listener_id in self.callbacks:
            raise ValueError(f"Already registered a listener with id: {listener_id}")

        if not iscoroutinefunction(cb):
            raise NotImplementedError("Only async listeners are allowed")

        self.callbacks[listener_id] = (cb, priority.value)

    def remove_listener(self, listener_id: str):
        self.callbacks.pop(listener_id, None)

    async def dispatch(self, payload: Any):
        """Send signal to all registered callbacks by they priority order."""
        assert (
            type(payload) == self.payload_model_type
        ), "Can't dispatch a signal with an invalid model"

        awaitables = [
            cb(payload=payload)
            for cb, _ in sorted(
                self.callbacks.values(), key=lambda t: t[1], reverse=True
            )
        ]

        results = await asyncio.gather(*awaitables, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                event_id = capture_exception(result)
                logger.error(
                    f"Error on listener dispatch. Check sentry for more details. Event id: {event_id}",
                )
