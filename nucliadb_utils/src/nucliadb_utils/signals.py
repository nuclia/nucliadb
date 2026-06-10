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
from collections.abc import Awaitable, Callable
from enum import Enum
from inspect import iscoroutinefunction
from typing import Any

from nucliadb_telemetry.errors import capture_exception
from nucliadb_utils import logger


class ListenerPriority(Enum):
    DONT_CARE = 0
    CRITICAL = 1


class Signal:
    def __init__(self, payload_model: type):
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
        assert isinstance(payload, self.payload_model_type), (
            "Can't dispatch a signal with an invalid model"
        )

        awaitables = [
            cb(payload=payload)
            for cb, _ in sorted(self.callbacks.values(), key=lambda t: t[1], reverse=True)
        ]

        results = await asyncio.gather(*awaitables, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                event_id = capture_exception(result)
                logger.error(
                    f"Error on listener dispatch. Check sentry for more details. Event id: {event_id}",
                )
