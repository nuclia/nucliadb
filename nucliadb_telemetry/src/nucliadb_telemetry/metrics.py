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
import os
import time
from functools import wraps
from inspect import isasyncgenfunction, isgeneratorfunction
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
)

import prometheus_client

if TYPE_CHECKING:  # pragma: no cover
    from traceback import StackSummary
else:
    StackSummary = None

OK = "ok"
ERROR = "error"
INF = float("inf")

_STATUS_METRIC = "status"
_VERSION_METRIC = "version"
_VERSION_ENV_VAR_NAME = "VERSION"


F = TypeVar("F", bound=Callable[..., Any])


class Observer:
    def __init__(
        self,
        name: str,
        *,
        error_mappings: Optional[Dict[str, Union[Type[Exception], Type[BaseException]]]] = None,
        labels: Optional[Dict[str, str]] = None,
        buckets: Optional[List[float]] = None,
    ):
        self.error_mappings = error_mappings or {}
        self.labels = labels or {}

        assert (
            # managed by us, do not allow user to specify
            labels is None or (_STATUS_METRIC not in labels and _VERSION_METRIC not in labels)
        )

        if _VERSION_ENV_VAR_NAME in os.environ:
            self.labels[_VERSION_METRIC] = os.environ[_VERSION_ENV_VAR_NAME]

        self.counter = prometheus_client.Counter(
            f"{name}_count",
            f"Number of times {name} was called.",
            labelnames=tuple(self.labels.keys()) + (_STATUS_METRIC,),
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

    def wrap(self, labels: Optional[Dict[str, str]] = None) -> Callable[[F], F]:
        def decorator(func):
            if asyncio.iscoroutinefunction(func):

                @wraps(func)
                async def inner(*args, **kwargs):
                    with ObserverRecorder(self, labels or {}):
                        return await func(*args, **kwargs)

            elif isasyncgenfunction(func):

                @wraps(func)
                async def inner(*args, **kwargs):
                    with ObserverRecorder(self, labels or {}):
                        async for item in func(*args, **kwargs):
                            yield item

            elif isgeneratorfunction(func):

                @wraps(func)
                def inner(*args, **kwargs):
                    with ObserverRecorder(self, labels or {}):
                        for item in func(*args, **kwargs):
                            yield item

            else:

                @wraps(func)
                def inner(*args, **kwargs):
                    with ObserverRecorder(self, labels or {}):
                        return func(*args, **kwargs)

            return inner

        return decorator

    def __call__(self, labels: Optional[Dict[str, str]] = None):
        return ObserverRecorder(self, labels or {})


class ObserverRecorder:
    def __init__(self, observer: Observer, label_overrides: Dict[str, str]):
        self.observer = observer
        if len(label_overrides) > 0:
            self.labels = observer.labels.copy()
            self.labels.update(label_overrides)
        else:
            self.labels = observer.labels

    def set_status(self, status: str):
        self.labels[_STATUS_METRIC] = status

    def start(self):
        self.start = time.monotonic()

    def end(self):
        finished = time.monotonic()
        status = self.labels.pop(_STATUS_METRIC, OK)

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
        exc_type: Optional[Union[Type[Exception], Type[BaseException]]],
        exc_value: Optional[Union[Exception, BaseException]],
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
    def __init__(self, name: str, *, labels: Optional[Dict[str, str]] = None):
        self.labels = labels or {}
        if _VERSION_ENV_VAR_NAME in os.environ:
            self.labels[_VERSION_METRIC] = os.environ[_VERSION_ENV_VAR_NAME]

        self.gauge = prometheus_client.Gauge(
            name, f"Gauge for {name}.", labelnames=tuple(self.labels.keys())
        )

    def set(self, value: Union[float, int], labels: Optional[Dict[str, str]] = None):
        merged_labels = self.labels.copy()
        merged_labels.update(labels or {})

        if len(merged_labels) > 0:
            self.gauge.labels(**merged_labels).set(value)
        else:
            self.gauge.set(value)

    def inc(self, value: Union[float, int], labels: Optional[Dict[str, str]] = None):
        merged_labels = self.labels.copy()
        merged_labels.update(labels or {})

        if len(merged_labels) > 0:
            self.gauge.labels(**merged_labels).inc(value)
        else:
            self.gauge.inc(value)

    def dec(self, value: Union[float, int], labels: Optional[Dict[str, str]] = None):
        merged_labels = self.labels.copy()
        merged_labels.update(labels or {})

        if len(merged_labels) > 0:
            self.gauge.labels(**merged_labels).dec(value)
        else:
            self.gauge.dec(value)

    def remove(self, labels: Dict[str, str]):
        self.gauge.remove(*[labels[k] for k in self.labels.keys()])


class Counter:
    def __init__(self, name: str, *, labels: Optional[Dict[str, str]] = None):
        self.labels = labels or {}
        if _VERSION_ENV_VAR_NAME in os.environ:
            self.labels[_VERSION_METRIC] = os.environ[_VERSION_ENV_VAR_NAME]

        self.counter = prometheus_client.Counter(
            name, f"Counter for {name}.", labelnames=tuple(self.labels.keys())
        )

    def inc(self, labels: Optional[Dict[str, str]] = None, value: Union[float, int] = 1):
        merged_labels = self.labels.copy()
        merged_labels.update(labels or {})

        if len(merged_labels) > 0:
            self.counter.labels(**merged_labels).inc(value)
        else:
            self.counter.inc(value)


class Histogram:
    def __init__(
        self,
        name: str,
        *,
        labels: Optional[Dict[str, str]] = None,
        buckets: Optional[List[float]] = None,
    ):
        self.labels = labels or {}
        if _VERSION_ENV_VAR_NAME in os.environ:
            self.labels[_VERSION_METRIC] = os.environ[_VERSION_ENV_VAR_NAME]

        kwargs = {}
        if buckets is not None:
            kwargs["buckets"] = buckets
        self.histo = prometheus_client.Histogram(
            name,
            f"Counter for {name}.",
            labelnames=tuple(self.labels.keys()),
            **kwargs,  # type: ignore
        )

    def observe(self, value: float, labels: Optional[Dict[str, str]] = None):
        merged_labels = self.labels.copy()
        merged_labels.update(labels or {})
        if len(merged_labels) > 0:
            self.histo.labels(**merged_labels).observe(value)
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
