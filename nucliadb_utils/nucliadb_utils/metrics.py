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
