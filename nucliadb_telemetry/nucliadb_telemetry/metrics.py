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

    def __enter__(self):
        self.start = time.time()
        return self

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
