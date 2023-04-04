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
from functools import wraps
from typing import TYPE_CHECKING, Optional, Type, Union

import prometheus_client

if TYPE_CHECKING:  # pragma: no cover
    from traceback import StackSummary
else:
    StackSummary = None

OK = "ok"
ERROR = "error"
INF = float("inf")


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
            f"{name}_count",
            f"Number of times {name} was called.",
            labelnames=tuple(self.labels.keys()) + ("status",),
        )
        hist_kwargs = {}
        if buckets is not None:
            hist_kwargs["buckets"] = buckets
        self.histogram = prometheus_client.Histogram(
            f"{name}_duration_seconds",
            f"Histogram for {name} duration.",
            labelnames=tuple(self.labels.keys()),
            **hist_kwargs,  # type: ignore
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

    def set_status(self, status: str):
        self.labels["status"] = status

    def start(self):
        self.start = time.monotonic()

    def end(self):
        finished = time.monotonic()
        status = self.labels.pop("status", OK)

        if len(self.labels) > 0:
            self.observer.histogram.labels(**self.labels).observe(finished - self.start)
        else:
            self.observer.histogram.observe(finished - self.start)

        self.observer.counter.labels(status=status, **self.labels).inc()

    def __enter__(self):
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[Exception]],
        exc_value: Optional[Exception],
        traceback: Optional[StackSummary],
    ):
        if exc_type is not None:
            status = ERROR
            for error_label, error_type in self.observer.error_mappings.items():
                if issubclass(exc_type, error_type):
                    status = error_label
                    break
            self.set_status(status)

        self.end()


class Gauge:
    def __init__(self, name: str, *, labelnames: Optional[list[str]] = None):
        self.labelnames = labelnames or []
        self.gauge = prometheus_client.Gauge(
            name, f"Gauge for {name}.", labelnames=self.labelnames
        )

    def set(self, value: Union[float, int], labels: Optional[dict[str, str]] = None):
        if labels is not None:
            self.gauge.labels(**labels).set(value)
        else:
            self.gauge.set(value)

    def remove(self, labels: dict[str, str]):
        self.gauge.remove(*[labels[k] for k in self.labelnames])


class Counter:
    def __init__(self, name: str, *, labelnames: Optional[list[str]] = None):
        self.labelnames = labelnames or []
        self.counter = prometheus_client.Counter(
            name, f"Counter for {name}.", labelnames=self.labelnames
        )

    def inc(self, labels: Optional[dict[str, str]] = None):
        if labels is not None:
            self.counter.labels(**labels).inc()
        else:
            self.counter.inc()


class Histogram:
    def __init__(
        self,
        name: str,
        *,
        labelnames: Optional[list[str]] = None,
        buckets: Optional[list[float]] = None,
    ):
        self.labelnames = labelnames or []
        kwargs = {}
        if buckets is not None:
            kwargs["buckets"] = buckets
        self.histo = prometheus_client.Histogram(
            name, f"Counter for {name}.", labelnames=self.labelnames, **kwargs  # type: ignore
        )

    def observe(self, value: float, labels: Optional[dict[str, str]] = None):
        if labels is not None:
            self.histo.labels(**labels).observe(value)
        else:
            self.histo.observe(value)


__all__ = (
    "Observer",
    "ObserverRecorder",
    "OK",
    "ERROR",
    "Counter",
    "Gauge",
    "Histogram",
)
