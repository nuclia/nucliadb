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
import time
import traceback
from typing import Dict, Optional, Type

try:
    from prometheus_client import Counter, Histogram  # type: ignore
except ImportError:
    Counter = Histogram = None  # type: ignore

ERROR_NONE = "none"
ERROR_GENERAL_EXCEPTION = "exception"


class watch:
    start: float

    def __init__(
        self,
        *,
        counter: Optional[Counter] = None,
        histogram: Optional[Histogram] = None,
        error_mappings: Dict[str, Type[Exception]] = None,
        labels: Optional[Dict[str, str]] = None,
    ):
        self.counter = counter
        self.histogram = histogram
        self.labels = labels or {}
        self.error_mappings = error_mappings or {}

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[Exception]],
        exc_value: Optional[Exception],
        exc_traceback: Optional[traceback.StackSummary],
    ):
        if Counter is None:
            return

        error = ERROR_NONE
        if self.histogram is not None:
            finished = time.time()
            if len(self.labels) > 0:
                self.histogram.labels(**self.labels).observe(finished - self.start)
            else:
                self.histogram.observe(finished - self.start)

        if self.counter is not None:
            if exc_value is None:
                error = ERROR_NONE
            else:
                for error_type, mapped_exc_type in self.error_mappings.items():
                    if isinstance(exc_value, mapped_exc_type):
                        error = error_type
                        break
                else:
                    error = ERROR_GENERAL_EXCEPTION
            self.counter.labels(error=error, **self.labels).inc()


class watch_lock:
    def __init__(
        self,
        histogram: Histogram,
        lock: asyncio.Lock,
        labels: Optional[Dict[str, str]] = None,
    ):
        self.histogram = histogram
        self.lock = lock
        self.labels = labels or {}

    async def __aenter__(self) -> None:
        start = time.time()
        await self.lock.acquire()
        if self.histogram is not None:
            finished = time.time()
            if len(self.labels) > 0:
                self.histogram.labels(**self.labels).observe(finished - start)
            else:
                self.histogram.observe(finished - start)

    async def __aexit__(self, exc_type, exc, tb):
        self.lock.release()
