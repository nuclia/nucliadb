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
import logging
from typing import Optional

from nucliadb.common.cache.cache import CacheLayer, InMemoryCache, NoopCache
from nucliadb.common.cache.invalidations import RedisCacheInvalidations
from nucliadb.common.cache.settings import settings as cache_settings

logger = logging.getLogger(__name__)

_singleton: Optional[CacheLayer] = None


async def setup_cache() -> CacheLayer:
    global _singleton
    if _singleton is not None:
        logger.warning("Cache already initialized, returning existing instance")
        return _singleton
    if cache_settings.cache_redis_url is None:
        cache = NoopCache()
    else:
        invalidations = RedisCacheInvalidations(cache_settings.cache_redis_url)
        cache = InMemoryCache(cache_settings.cache_max_size, invalidations=invalidations)  # type: ignore
    await cache.initialize()
    _singleton = cache
    return cache


def get_cache() -> CacheLayer:
    global _singleton
    if _singleton is None:
        logger.warning("Cache not initialized, returning NoopCache")
        return NoopCache()
    return _singleton


async def teardown_cache(cache: CacheLayer) -> None:
    await cache.finalize()
