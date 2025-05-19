# Copyright 2025 Bosutech XXI S.L.
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
#

import asyncio
import time
from typing import Optional

from opentelemetry.context import Context  # type: ignore
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor, TracerProvider


class AsyncMultiSpanProcessor(SpanProcessor):
    """Implementation of class:`SpanProcessor` that forwards all received
    events to a list of span processors sequentially.

    The underlying span processors are called in sequential order as they were
    added.
    """

    def __init__(self):
        # use a tuple to avoid race conditions when adding a new span and
        # iterating through it on "on_start" and "on_end".
        self._span_processors = ()
        self._lock = asyncio.Lock()

    async def async_add_span_processor(self, span_processor: SpanProcessor) -> None:
        """Adds a SpanProcessor to the list handled by this instance."""
        async with self._lock:
            self._span_processors += (span_processor,)

    def on_start(
        self,
        span: Span,
        parent_context: Optional[Context] = None,
    ) -> None:
        for sp in self._span_processors:
            sp.on_start(span, parent_context=parent_context)

    def on_end(self, span: ReadableSpan) -> None:
        for sp in self._span_processors:
            sp.on_end(span)

    def shutdown(self) -> None:
        """Sequentially shuts down all underlying span processors."""
        for sp in self._span_processors:
            sp.shutdown()

    async def async_force_flush(self, timeout_millis: int = 30000) -> bool:
        """Sequentially calls async_force_flush on all underlying
        :class:`SpanProcessor`

        Args:
            timeout_millis: The maximum amount of time over all span processors
                to wait for spans to be exported. In case the first n span
                processors exceeded the timeout followup span processors will be
                skipped.

        Returns:
            True if all span processors flushed their spans within the
            given timeout, False otherwise.
        """
        deadline_ns = time.perf_counter_ns() + timeout_millis * 1000000
        for sp in self._span_processors:
            current_time_ns = time.perf_counter_ns()
            if current_time_ns >= deadline_ns:
                return False

            if not await sp.async_force_flush((deadline_ns - current_time_ns) // 1000000):
                return False

        return True


class AsyncTracerProvider(TracerProvider):
    initialized: bool = False
    _active_span_processor: AsyncMultiSpanProcessor  # type: ignore

    async def async_add_span_processor(self, span_processor: SpanProcessor) -> None:
        await self._active_span_processor.async_add_span_processor(span_processor)

    async def async_force_flush(self, timeout_millis: int = 30000) -> bool:
        return await self._active_span_processor.async_force_flush(timeout_millis)
