import asyncio
import time
from functools import wraps
from typing import TYPE_CHECKING, Optional, Type

import prometheus_client
from prometheus_client import Counter, Gauge, Histogram

if TYPE_CHECKING:  # pragma: no cover
    from traceback import StackSummary
else:
    StackSummary = None

OK = "ok"
ERROR = "error"


class Observer:
    def __init__(
        self,
        name: str,
        *,
        error_mappings: Optional[dict[str, Type[Exception]]] = None,
        labels: Optional[dict[str, str]] = None,
        buckets: Optional[list[float]] = None,
    ):
        self.error_mappings = error_mappings or {}
        self.labels = labels or {}

        assert (
            # managed by us, do not allow user to specify
            labels is None
            or "status" not in labels
        )

        self.counter = prometheus_client.Counter(
            name + "_count",
            f"Number of times {name} was called.",
            labelnames=list(self.labels.keys()) + ["status"],
        )
        hist_kwargs = {}
        if buckets is not None:
            hist_kwargs["buckets"] = buckets
        self.histogram = prometheus_client.Histogram(
            name + "_duration_seconds",
            f"Histogram for {name} duration.",
            labelnames=list(self.labels.keys()),
            **hist_kwargs,
        )

    def wrap(self, labels: Optional[dict[str, str]] = None):
        def decorator(func):
            if asyncio.iscoroutinefunction(func):

                @wraps(func)
                async def inner(*args, **kwargs):
                    with ObserverRecorder(self, labels or {}):
                        return await func(*args, **kwargs)

            else:

                @wraps(func)
                def inner(*args, **kwargs):
                    with ObserverRecorder(self, labels or {}):
                        return func(*args, **kwargs)

            return inner

        return decorator

    def __call__(self, labels: Optional[dict[str, str]] = None):
        return ObserverRecorder(self, labels or {})


class ObserverRecorder:
    def __init__(self, observer: Observer, label_overrides: dict[str, str]):
        self.observer = observer
        if len(label_overrides) > 0:
            self.labels = observer.labels.copy()
            self.labels.update(label_overrides)
        else:
            self.labels = observer.labels

    def __enter__(self):
        self.start = time.time()

    def __exit__(
        self,
        exc_type: Optional[Type[Exception]],
        exc_value: Optional[Exception],
        traceback: Optional[StackSummary],
    ):
        finished = time.time()
        status = OK

        if len(self.labels) > 0:
            self.observer.histogram.labels(**self.labels).observe(finished - self.start)
        else:
            self.observer.histogram.observe(finished - self.start)

        if exc_type is not None:
            status = ERROR
            for error_label, error_type in self.observer.error_mappings.items():
                if issubclass(exc_type, error_type):
                    status = error_label
                    break

        self.observer.counter.labels(status=status, **self.labels).inc()


__all__ = (
    "Observer",
    "ObserverRecorder",
    "OK",
    "ERROR",
    "Counter",
    "Histogram",
    "Gauge",
)
