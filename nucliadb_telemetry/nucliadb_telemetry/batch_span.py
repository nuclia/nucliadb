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
from typing import List, Optional

from opentelemetry.context import (  # type: ignore
    _SUPPRESS_INSTRUMENTATION_KEY,
    Context,
    attach,
    detach,
    set_value,
)
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor  # type: ignore
from opentelemetry.sdk.trace.export import SpanExporter  # type: ignore
from opentelemetry.util._time import _time_ns  # type: ignore

from nucliadb_telemetry import logger


class _FlushRequest:
    """Represents a request for the BatchSpanProcessor to flush spans."""

    __slots__ = ["event", "num_spans"]

    def __init__(self):
        self.event = asyncio.Event()
        self.num_spans = 0


class BatchSpanProcessor(SpanProcessor):
    """Batch span processor implementation.
    `BatchSpanProcessor` is an implementation of `SpanProcessor` that
    batches ended spans and pushes them to the configured `SpanExporter`.
    `BatchSpanProcessor` is configurable with the following environment
    variables which correspond to constructor parameters:
    - :envvar:`OTEL_BSP_SCHEDULE_DELAY`
    - :envvar:`OTEL_BSP_MAX_QUEUE_SIZE`
    - :envvar:`OTEL_BSP_MAX_EXPORT_BATCH_SIZE`
    - :envvar:`OTEL_BSP_EXPORT_TIMEOUT`
    """

    def __init__(
        self,
        span_exporter: SpanExporter,
        max_queue_size: int = 2048,
        schedule_delay_millis: int = 5000,
        max_export_batch_size: int = 512,
        export_timeout_millis: int = 30000,
    ):

        if max_queue_size <= 0:
            raise ValueError("max_queue_size must be a positive integer.")

        if schedule_delay_millis <= 0:
            raise ValueError("schedule_delay_millis must be positive.")

        if max_export_batch_size <= 0:
            raise ValueError("max_export_batch_size must be a positive integer.")

        if max_export_batch_size > max_queue_size:
            raise ValueError(
                "max_export_batch_size must be less than or equal to max_queue_size."
            )

        self.span_exporter = span_exporter
        self.queue = asyncio.Queue(maxsize=max_queue_size)  # type: asyncio.Queue[Span]
        self.worker_task: asyncio.Task = asyncio.create_task(
            self.worker(), name="OtelBatchSpanProcessor"
        )
        self.condition = asyncio.Condition()
        self._flush_request = None  # type: Optional[_FlushRequest]
        self.schedule_delay_millis = schedule_delay_millis
        self.max_export_batch_size = max_export_batch_size
        self.max_queue_size = max_queue_size
        self.export_timeout_millis = export_timeout_millis
        self.done = False
        # flag that indicates that spans are being dropped
        self._spans_dropped = False
        # precallocated list to send spans to exporter
        self.spans_list = [
            None
        ] * self.max_export_batch_size  # type: List[Optional[Span]]

    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:
        pass

    def on_end(self, span: ReadableSpan) -> None:
        if self.done:
            logger.warning("Already shutdown, dropping span.")
            return
        if not span.context.trace_flags.sampled:
            return
        if self.queue.full():
            if not self._spans_dropped:
                logger.warning("Queue is full, likely spans will be dropped.")
                self._spans_dropped = True

        try:
            self.queue.put_nowait(span)
        except:
            logger.error(f"Queue size : {self.queue.qsize()}")

        if self.queue.qsize() >= self.max_export_batch_size:
            asyncio.create_task(self.notify())

    async def notify(self):
        async with self.condition:
            self.condition.notify()

    async def notify_all(self):
        async with self.condition:
            self.condition.notify_all()

    async def worker(self):
        try:
            logger.info("Batch telemetry event loop started")
            await self._worker()
        except Exception as e:
            logger.exception(e)
            raise e

    async def _worker(self):
        timeout = self.schedule_delay_millis / 1e3
        flush_request = None  # type: Optional[_FlushRequest]
        while not self.done:
            logger.debug("Waiting condition")
            async with self.condition:
                if self.done:
                    # done flag may have changed, avoid waiting
                    break
                logger.debug(f"{self.queue.qsize()} spans on queue")
                flush_request = self._get_and_unset_flush_request()
                if (
                    self.queue.qsize() < self.max_export_batch_size
                    and flush_request is None
                ):
                    try:
                        await asyncio.wait_for(self.condition.wait(), timeout)
                    except asyncio.TimeoutError:
                        pass
                    flush_request = self._get_and_unset_flush_request()
                    if not self.queue:
                        # spurious notification, let's wait again, reset timeout
                        timeout = self.schedule_delay_millis / 1e3
                        self._notify_flush_request_finished(flush_request)
                        flush_request = None
                        continue
                    if self.done:
                        # missing spans will be sent when calling flush
                        break

            # subtract the duration of this export call to the next timeout
            start = _time_ns()
            try:
                await asyncio.wait_for(self._export(flush_request), timeout)
            except asyncio.TimeoutError:
                logger.exception("Took to much time to export, network problem ahead")
            end = _time_ns()
            duration = (end - start) / 1e9
            timeout = self.schedule_delay_millis / 1e3 - duration

            self._notify_flush_request_finished(flush_request)
            flush_request = None

        # there might have been a new flush request while export was running
        # and before the done flag switched to true
        async with self.condition:
            shutdown_flush_request = self._get_and_unset_flush_request()

        # be sure that all spans are sent
        await self._drain_queue()
        self._notify_flush_request_finished(flush_request)
        self._notify_flush_request_finished(shutdown_flush_request)

    def _get_and_unset_flush_request(
        self,
    ) -> Optional[_FlushRequest]:
        """Returns the current flush request and makes it invisible to the
        worker thread for subsequent calls.
        """
        flush_request = self._flush_request
        self._flush_request = None
        if flush_request is not None:
            flush_request.num_spans = self.queue.qsize()
        return flush_request

    @staticmethod
    def _notify_flush_request_finished(
        flush_request: Optional[_FlushRequest],
    ):
        """Notifies the flush initiator(s) waiting on the given request/event
        that the flush operation was finished.
        """
        if flush_request is not None:
            flush_request.event.set()

    def _get_or_create_flush_request(self) -> _FlushRequest:
        """Either returns the current active flush event or creates a new one.
        The flush event will be visible and read by the worker thread before an
        export operation starts. Callers of a flush operation may wait on the
        returned event to be notified when the flush/export operation was
        finished.
        This method is not thread-safe, i.e. callers need to take care about
        synchronization/locking.
        """
        if self._flush_request is None:
            self._flush_request = _FlushRequest()
        return self._flush_request

    async def _export(self, flush_request: Optional[_FlushRequest]):
        """Exports spans considering the given flush_request.
        In case of a given flush_requests spans are exported in batches until
        the number of exported spans reached or exceeded the number of spans in
        the flush request.
        In no flush_request was given at most max_export_batch_size spans are
        exported.
        """
        if not flush_request:
            await self._export_batch()
            return

        num_spans = flush_request.num_spans
        while self.queue.qsize():
            num_exported = await self._export_batch()
            num_spans -= num_exported

            if num_spans <= 0:
                break

    async def _export_batch(self) -> int:
        """Exports at most max_export_batch_size spans and returns the number of
        exported spans.
        """
        idx = 0
        # currently only a single thread acts as consumer, so queue.pop() will
        # not raise an exception
        while idx < self.max_export_batch_size and self.queue.qsize():
            self.spans_list[idx] = self.queue.get_nowait()
            idx += 1
        token = attach(set_value(_SUPPRESS_INSTRUMENTATION_KEY, True))
        try:
            # Ignore type b/c the Optional[None]+slicing is too "clever"
            # for mypy
            await self.span_exporter.export(self.spans_list[:idx])  # type: ignore
        except (asyncio.CancelledError):
            logger.exception("Task was canceled while exporting Span batch")
        except Exception:  # pylint: disable=broad-except
            logger.exception("Exception while exporting Span batch)")
        detach(token)

        # clean up list
        for index in range(idx):
            self.spans_list[index] = None
        return idx

    async def _drain_queue(self):
        """Export all elements until queue is empty.
        Can only be called from the worker thread context because it invokes
        `export` that is not thread safe.
        """
        while self.queue.qsize():
            await self._export_batch()

    async def force_flush(self, timeout_millis: int = None) -> bool:

        if timeout_millis is None:
            timeout_millis = self.export_timeout_millis

        if self.done:
            logger.warning("Already shutdown, ignoring call to force_flush().")
            return True

        async with self.condition:
            flush_request = self._get_or_create_flush_request()
            # signal the worker task to flush and wait for it to finish
            self.condition.notify_all()

        if flush_request.num_spans == 0:
            return True

        # wait for token to be processed
        ret = await asyncio.wait_for(flush_request.event.wait(), timeout_millis)
        if not ret:
            logger.warning("Timeout was exceeded in force_flush().")
            return False
        return ret

    def shutdown(self) -> None:
        # signal the worker thread to finish and then wait for it
        self.done = True
        self.span_exporter.shutdown()
        try:
            self.worker_task.cancel()
        except RuntimeError:
            pass
