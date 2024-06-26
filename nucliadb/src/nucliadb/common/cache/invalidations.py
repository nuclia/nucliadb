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

import abc
import asyncio
import json
import logging

from redis import asyncio as aioredis

logger = logging.getLogger(__name__)


class AbstractCacheInvalidations(abc.ABC):
    invalidations_queue: asyncio.Queue

    @abc.abstractmethod
    async def initialize(self): ...

    @abc.abstractmethod
    async def finalize(self): ...

    @abc.abstractmethod
    async def invalidate(self, key: str): ...

    @abc.abstractmethod
    async def invalidate_prefix(self, prefix: str): ...


class CacheInvalidations(AbstractCacheInvalidations):
    """
    For testing purposes, a cache invalidations implementation that just puts
    the invalidation messages in a queue.
    This is not meant to be used in production.
    """

    def __init__(self):
        self.invalidations_queue = asyncio.Queue(maxsize=0)

    async def initialize(self):
        pass

    async def finalize(self):
        pass

    async def invalidate(self, key: str):
        await self.invalidations_queue.put({"type": "invalidate_key", "key": key})

    async def invalidate_prefix(self, prefix: str):
        await self.invalidations_queue.put({"type": "invalidate_prefix", "prefix": prefix})


class RedisCacheInvalidations(AbstractCacheInvalidations):
    """
    Cache invalidations using Redis pubsub.
    """

    def __init__(self, redis_url: str):
        # Unbounded queue to avoid losing messages.
        # This queue is used to communicate with the cache layer about invalidations.
        self.invalidations_queue = asyncio.Queue(maxsize=0)
        self.redis = aioredis.from_url(redis_url)
        self.pubsub = self.redis.pubsub()
        self._channel = "nucliadb_cache_invalidation"
        self._invalidations_task = None
        self._pubsub_task = None

    async def initialize(self):
        if self._invalidations_task is not None:
            # Already initialized
            return
        self._invalidations_task = asyncio.create_task(self._listen_for_invalidation())

    async def finalize(self):
        if self._invalidations_task is None:
            # Already finalized
            return
        self._invalidations_task.cancel()
        self._invalidations_task = None
        await self.pubsub.unsubscribe(self._channel)
        await self.pubsub.aclose()
        if self._pubsub_task is not None:
            self._pubsub_task.cancel()
        self._pubsub_task = None
        await self.redis.aclose()

    async def _listen_for_invalidation(self):
        async def invalidation_listener(message):
            """
            Read invalidation messages from the pubsub channel and
            put them in the queue so that the cache layer can process them.
            """
            if message["type"] == "message":
                try:
                    data = json.loads(message["data"])
                    if data["type"] == "invalidate_key":
                        await self.invalidations_queue.put(
                            {"type": "invalidate_key", "key": data["key"]}
                        )
                    elif data["type"] == "invalidate_prefix":
                        await self.invalidations_queue.put(
                            {"type": "invalidate_prefix", "prefix": data["prefix"]}
                        )
                    else:  # pragma: no cover
                        logger.warning(f"Invalid redis pubsub message: {data}")
                except (json.JSONDecodeError, KeyError):
                    logger.warning(f"Invalid redis pubsub message: {message['data']}")

        await self.pubsub.subscribe(**{self._channel: invalidation_listener})
        self._pubsub_task = asyncio.create_task(self.pubsub.run())

    async def invalidate(self, key: str):
        data = json.dumps({"type": "invalidate_key", "key": key})
        await self.redis.publish(self._channel, data)

    async def invalidate_prefix(self, prefix: str):
        data = json.dumps({"type": "invalidate_prefix", "prefix": prefix})
        await self.redis.publish(self._channel, data)
