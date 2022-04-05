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
import uuid
from sys import getsizeof
from typing import Any, Dict, List, Optional

import orjson

from nucliadb_utils import logger
from nucliadb_utils.cache import memcache
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.cache.settings import settings

try:
    from nucliadb_utils.cache.lru import LRU
except ImportError:
    from lru import LRU  # type: ignore

_default_size = 1024
_basic_types = (bytes, str, int, float)


class Cache:
    _memory_cache: LRU
    pubsub: Optional[PubSubDriver] = None
    ident: str
    initialized: bool = False

    def __init__(self, pubsub: PubSubDriver = None):
        self.ident = uuid.uuid4().hex
        self.pubsub = pubsub

    async def initialize(self):
        if self.initialized:
            return
        self._memory_cache = memcache.get_memory_cache()
        # We need to make sure that we have also PubSub
        from nucliadb_utils.utilities import get_pubsub

        if self.pubsub is None:
            self.pubsub = await get_pubsub()
        if self.pubsub.async_callback:
            await self.pubsub.subscribe(
                self.async_invalidate, settings.cache_pubsub_channel
            )
        else:
            await self.pubsub.subscribe(self.invalidate, settings.cache_pubsub_channel)
        self.initialized = True

    async def finalize(self):
        if self.initialized is False:
            logger.error("finalizing a not initialized cache utility")
        if self.pubsub is not None:
            try:
                await self.pubsub.unsubscribe(settings.cache_pubsub_channel)
            except (asyncio.CancelledError, RuntimeError):
                # task cancelled, let it die
                return
        self.initialized = False

    # Get a object from cache
    async def get(self, key):
        if key in self._memory_cache:
            logger.debug("Retrieved {} from memory cache".format(key))
            return self._memory_cache[key]

    def get_size(self, value):
        if isinstance(value, list) and len(value) > 0:
            # if its a list, guesss from first gey the length, and
            # estimate it from the total lenghts on the list..
            return getsizeof(value[0]) * len(value)
        if isinstance(value, _basic_types):
            return getsizeof(value)
        return _default_size

    # Set a object from cache
    async def set(self, key: str, value: Any, invalidate: bool = False):
        size = self.get_size(value)
        self._memory_cache.set(key, value, size)
        if invalidate:
            await self.send_invalidation([key])

    async def mset(self, keys: List[str], values: List[Any], invalidate: bool = False):
        for key, value in zip(keys, values):
            size = self.get_size(value)
            self._memory_cache.set(key, value, size)
        if invalidate:
            await self.send_invalidation(keys)

    # Delete a set of objects from cache
    async def mdelete(self, keys: List[str], invalidate: bool = False):
        for key in keys:
            if key in self._memory_cache:
                del self._memory_cache[key]
        if invalidate:
            await self.send_invalidation(keys)

    # Delete a set of objects from cache
    async def delete(self, key: str, invalidate: bool = False):
        if key in self._memory_cache:
            del self._memory_cache[key]
        if invalidate:
            await self.send_invalidation([key])

    # Clean all cache
    async def clear(self, invalidate: bool = False):
        self._memory_cache.clear()
        if invalidate:
            await self.send_invalidation(purge=True)

    async def async_invalidate(self, data: Dict[str, Any]):
        self.invalidate(data)

    # Called by the subscription to invalidations
    def invalidate(self, data: Dict[str, Any]):
        if self.pubsub is None:
            raise AttributeError("Pubsub not configured")
        payload = self.pubsub.parse(data)
        if "origin" in payload and payload["origin"] == self.ident:
            # Skip my messages
            return
        if "purge" in payload and payload["purge"]:
            self._memory_cache.clear()
        if "keys" in payload:
            for key in payload["keys"]:
                if key in self._memory_cache:
                    del self._memory_cache[key]

    async def send_invalidation(
        self, keys_to_invalidate: List[str] = [], purge: bool = False
    ):
        data = orjson.dumps(
            {"keys": keys_to_invalidate, "origin": self.ident, "purge": purge}
        )
        if self.pubsub:
            await self.pubsub.publish(settings.cache_pubsub_channel, data)

    async def get_stats(self):
        result = {
            "in-memory": {
                "size": len(self._memory_cache),
                "stats": self._memory_cache.get_stats(),
            }
        }
        return result
