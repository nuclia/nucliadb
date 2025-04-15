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
#

import asyncio
import contextlib
from abc import ABC, abstractmethod
from contextvars import ContextVar
from dataclasses import dataclass
from functools import cached_property
from typing import Generic, Optional, TypeVar

from lru import LRU

from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb_protos.utils_pb2 import ExtractedText
from nucliadb_telemetry.metrics import Counter, Gauge

# specific metrics per cache type
cached_resources = Gauge("nucliadb_cached_resources")
cached_extracted_texts = Gauge("nucliadb_cached_extracted_texts")
resource_cache_ops = Counter("nucliadb_resource_cache_ops", labels={"type": ""})
extracted_text_cache_ops = Counter("nucliadb_extracted_text_cache_ops", labels={"type": ""})


T = TypeVar("T")


@dataclass
class CacheMetrics:
    _cache_size: Gauge
    ops: Counter


class Cache(Generic[T], ABC):
    """Low-level bounded cache implementation with access to per-key async locks
    in case cache users want to lock concurrent access.

    This cache is measured using a mandatory metric all subclasses must define.

    """

    def __init__(self, cache_size: int) -> None:
        self.cache: LRU[str, T] = LRU(cache_size, callback=self._evicted_callback)
        self.locks: dict[str, asyncio.Lock] = {}

    def _evicted_callback(self, key: str, value: T):
        self.locks.pop(key, None)
        self.metrics.ops.inc({"type": "evict"})

    def get(self, key: str) -> Optional[T]:
        return self.cache.get(key)

    # Get a lock for a specific key. Locks will be evicted at the same time as
    # key-value pairs
    def get_lock(self, key: str) -> asyncio.Lock:
        return self.locks.setdefault(key, asyncio.Lock())

    def set(self, key: str, value: T):
        len_before = len(self.cache)

        self.cache[key] = value

        len_after = len(self.cache)
        if len_after - len_before > 0:
            self.metrics._cache_size.inc(len_after - len_before)

    def __contains__(self, key: str) -> bool:
        return self.cache.__contains__(key)

    def clear(self):
        self.metrics._cache_size.dec(len(self.cache))
        self.cache.clear()
        self.locks.clear()

    @abstractmethod
    @cached_property
    def metrics(self) -> CacheMetrics: ...


class ResourceCache(Cache[ResourceORM]):
    metrics = CacheMetrics(
        _cache_size=cached_resources,
        ops=resource_cache_ops,
    )


class ExtractedTextCache(Cache[ExtractedText]):
    """
    Used to cache extracted text from a resource in memory during the process
    of search results hydration.

    This is needed to avoid fetching the same extracted text multiple times,
    as matching text blocks are processed in parallel and the extracted text is
    fetched for each field where the text block is found.
    """

    metrics = CacheMetrics(
        _cache_size=cached_extracted_texts,
        ops=extracted_text_cache_ops,
    )


# Global caches (per asyncio task)

rcache: ContextVar[Optional[ResourceCache]] = ContextVar("rcache", default=None)
etcache: ContextVar[Optional[ExtractedTextCache]] = ContextVar("etcache", default=None)


# Cache management


def get_resource_cache() -> Optional[ResourceCache]:
    return rcache.get()


def get_extracted_text_cache() -> Optional[ExtractedTextCache]:
    return etcache.get()


@contextlib.contextmanager
def _use_cache(klass: type[Cache], context_var: ContextVar, /, **kwargs):
    """Context manager that manages a context var cache. It's responsible of
    cache creation and cleanup.

    Note the configured cache is specific to the current asyncio task (and all
    its subtasks). If you spawn subtasks that should share a cache, make sure
    the parent task is the one using this decorator, otherwise, each subtask
    will use its own independent cache instance

    Do not use the cache object outside the scope of this context manager!
    Otherwise, metrics and cleanup could get wrong.

    """
    cache = klass(**kwargs)
    token = context_var.set(cache)
    try:
        yield cache
    finally:
        context_var.reset(token)
        cache.clear()


@contextlib.contextmanager
def resource_cache(size: int):
    with _use_cache(ResourceCache, rcache, cache_size=size) as cache:
        yield cache


@contextlib.contextmanager
def extracted_text_cache(size: int):
    with _use_cache(ExtractedTextCache, etcache, cache_size=size) as cache:
        yield cache
