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

from contextvars import ContextVar
from typing import Generic, Optional, TypeVar

from lru import LRU

from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb_telemetry.metrics import Gauge

cached_resources = Gauge("nucliadb_global_cached_resources")


T = TypeVar("T")


class Cache(Generic[T]):
    def __init__(self, cache_size: int) -> None:
        self.cache: LRU[str, T] = LRU(cache_size)

    def get(self, key: str) -> Optional[T]:
        return self.cache.get(key)

    def set(self, key: str, value: T):
        len_before = len(self.cache)

        self.cache[key] = value

        len_after = len(self.cache)
        if len_after - len_before > 0:
            cached_resources.inc(len_after - len_before)

    def contains(self, key: str) -> bool:
        return key in self.cache

    def __del__(self):
        cached_resources.dec(len(self.cache))
        self.cache.clear()


class ResourceCache(Cache[ResourceORM]):
    # This cache size is an arbitrary number, once we have a metric in place and
    # we analyze memory consumption, we can adjust it with more knoweldge
    def __init__(self, cache_size: int = 1024) -> None:
        super().__init__(cache_size)


rcache: ContextVar[Optional[ResourceCache]] = ContextVar("rcache", default=None)


# Get or create a resource cache specific to the current asyncio task
def get_or_create_resource_cache(clear: bool = False) -> ResourceCache:
    cache: Optional[ResourceCache] = rcache.get()
    if cache is None or clear:
        cache = ResourceCache()
        rcache.set(cache)
    return cache


def get_resource_cache() -> Optional[ResourceCache]:
    return rcache.get()


def set_resource_cache() -> None:
    cache = ResourceCache()
    rcache.set(cache)


# Delete resource cache and all its content
def delete_resource_cache() -> None:
    cache = rcache.get()
    if cache is not None:
        rcache.set(None)
        del cache
