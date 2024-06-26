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
import abc
import asyncio
import logging
from typing import Any, Optional

from cachetools import LRUCache

from nucliadb.common.cache.invalidations import AbstractCacheInvalidations
from nucliadb_telemetry import metrics

logger = logging.getLogger(__name__)


CACHE_LAYER_OPS = metrics.Counter("nucliadb_cache_layer_ops", labels={"op": "", "type": ""})
CACHE_LAYER_SIZE = metrics.Gauge("nucliadb_cache_layer_size")


class CacheLayer(abc.ABC):
    class keys:
        KB_BASE_KEY = "{kbid}/"
        KB_EXISTS = "{kbid}/exists"

    @abc.abstractmethod
    async def initialize(self): ...

    @abc.abstractmethod
    async def finalize(self): ...

    @abc.abstractmethod
    def get(self, key: str) -> Any: ...

    @abc.abstractmethod
    def set(self, key: str, value: Any): ...

    @abc.abstractmethod
    def delete(self, key: str): ...

    @abc.abstractmethod
    def delete_prefix(self, prefix: str): ...

    @abc.abstractmethod
    async def invalidate(self, key: str): ...

    @abc.abstractmethod
    async def invalidate_prefix(self, prefix: str): ...


class InMemoryCache(CacheLayer):
    """
    Implements an in-memory cache layer with support for invalidations.
    """

    def __init__(self, max_size: int = 2048, invalidations: Optional[AbstractCacheInvalidations] = None):
        self._cache: LRUCache = LRUCache(maxsize=max_size)
        self._invalidations = invalidations
        self._invalidations_task = None

    async def initialize(self):
        if self._invalidations is not None:
            await self._invalidations.initialize()
            self._invalidations_task = asyncio.create_task(self._process_invalidations())

    async def finalize(self):
        if self._invalidations_task is not None:
            self._invalidations_task.cancel()
            await self._invalidations.finalize()
        self.clear()

    async def _process_invalidations(self):
        while True:
            value = await self._invalidations.invalidations_queue.get()
            if value["type"] == "invalidate_key":
                CACHE_LAYER_OPS.inc({"op": "invalidate", "type": "processed"})
                self.delete(value["key"])
            elif value["type"] == "invalidate_prefix":
                CACHE_LAYER_OPS.inc({"op": "invalidate_prefix", "type": "processed"})
                self.delete_prefix(value["prefix"])
            else:  # pragma: no cover
                logger.warning(f"Unknown invalidation type: {value}")
            self._invalidations.invalidations_queue.task_done()

    def get(self, key: str) -> Any:
        value = self._cache.get(key)
        if value is not None:
            CACHE_LAYER_OPS.inc({"op": "get", "type": "hit"})
        else:
            CACHE_LAYER_OPS.inc({"op": "get", "type": "miss"})
        CACHE_LAYER_SIZE.set(self._cache.currsize)
        return value

    def set(self, key: str, value: Any):
        CACHE_LAYER_OPS.inc({"op": "set", "type": ""})
        CACHE_LAYER_SIZE.set(self._cache.currsize)
        self._cache[key] = value

    def delete(self, key: str):
        value = self._cache.pop(key, None)
        if value is not None:
            CACHE_LAYER_OPS.inc({"op": "delete", "type": "hit"})
        else:
            CACHE_LAYER_OPS.inc({"op": "delete", "type": "miss"})
        CACHE_LAYER_SIZE.set(self._cache.currsize)

    def delete_prefix(self, prefix: str):
        CACHE_LAYER_OPS.inc({"op": "delete_prefix", "type": ""})
        for key in list(self._cache.keys()):
            if key.startswith(prefix):
                self._cache.pop(key, None)
        CACHE_LAYER_SIZE.set(self._cache.currsize)

    def clear(self):
        self._cache.clear()

    async def invalidate(self, key: str):
        CACHE_LAYER_OPS.inc({"op": "invalidate", "type": "sent"})
        self.delete(key)
        if self._invalidations is None:
            return
        await self._invalidations.invalidate(key)

    async def invalidate_prefix(self, prefix: str):
        CACHE_LAYER_OPS.inc({"op": "invalidate_prefix", "type": "sent"})
        self.delete_prefix(prefix)
        if self._invalidations is None:
            return
        await self._invalidations.invalidate_prefix(prefix)


class NoopCache(CacheLayer):
    """
    A cache layer that does nothing. This is useful to avoid having to check for
    None when using a cache layer.
    """

    async def initialize(self):
        pass

    async def finalize(self):
        pass

    def get(self, key: str) -> Any:
        return None

    def set(self, key: str, value: Any):
        pass

    def delete(self, key: str):
        pass

    def delete_prefix(self, prefix: str):
        pass

    async def invalidate(self, key: str):
        pass

    async def invalidate_prefix(self, prefix: str):
        pass
