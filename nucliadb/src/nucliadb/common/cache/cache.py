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
import logging
from typing import Any, Optional

from cachetools import LRUCache

from nucliadb.common.cache.invalidations import CacheInvalidations

logger = logging.getLogger(__name__)


class CacheLayer:
    """
    Implements an in-memory cache layer with support for invalidations.
    """

    def __init__(self, max_size: int = 2048, invalidations: Optional[CacheInvalidations] = None):
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
            try:
                value = self._invalidations.queue.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.1)
                continue
            if value["type"] == "invalidate_key":
                self.delete(value["key"])
            elif value["type"] == "invalidate_prefix":
                self.delete_prefix(value["prefix"])
            else:  # pragma: no cover
                logger.warning(f"Unknown invalidation type: {value}")
            self._invalidations.queue.task_done()

    def get(self, key: str) -> Any:
        return self._cache.get(key)

    def set(self, key: str, value: Any):
        self._cache[key] = value

    def delete(self, key: str):
        self._cache.pop(key, None)

    def delete_prefix(self, prefix: str):
        for key in list(self._cache.keys()):
            if key.startswith(prefix):
                self._cache.pop(key, None)

    def clear(self):
        self._cache.clear()

    async def invalidate(self, key: str):
        self.delete(key)
        if self._invalidations is None:
            return
        await self._invalidations.invalidate(key)

    async def invalidate_prefix(self, prefix: str):
        self.delete_prefix(prefix)
        if self._invalidations is None:
            return
        await self._invalidations.invalidate_prefix(prefix)
