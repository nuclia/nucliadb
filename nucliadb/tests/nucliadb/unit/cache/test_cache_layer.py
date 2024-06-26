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

import pytest


@pytest.mark.asyncio
async def test_cache_layer_without_invalidations():
    from nucliadb.common.cache.cache import InMemoryCache

    cache = InMemoryCache(max_size=10)
    await cache.initialize()
    try:
        assert cache.get("foo") is None
        cache.set("foo", "bar")
        cache.set("foo/bar", "ipsum")
        cache.set("foo/ba", "lorem")
        assert cache.get("foo") == "bar"
        cache.delete("foo")
        assert cache.get("foo") is None
        assert cache.get("foo/bar") == "ipsum"
        cache.delete("foo/bar")
        cache.delete_prefix("foo/")
        assert cache.get("foo/bar") is None
        assert cache.get("foo/ba") is None

        # Check that when the cache is full, the least recently used key is evicted
        for i in range(10):
            cache.set(f"key{i}", f"value{i}")
        cache.set("key10", "value10")
        assert cache.get("key0") is None
        cache.set("something", "else")

        # Check that clearning the cache removes all keys
        cache.clear()
        assert cache.get("something") is None
    finally:
        await cache.finalize()


@pytest.mark.asyncio
async def test_cache_layer_with_invalidations():
    from nucliadb.common.cache.cache import InMemoryCache
    from nucliadb.common.cache.invalidations import CacheInvalidations

    invalidations = CacheInvalidations()

    cache1 = InMemoryCache(invalidations=invalidations)
    await cache1.initialize()

    cache2 = InMemoryCache(invalidations=invalidations)
    await cache2.initialize()

    try:
        # Initially, both caches should not have the key
        assert cache1.get("foo") is None
        assert cache2.get("foo") is None

        # Cache 1 sets a key, cache 2 should not have it
        cache1.set("foo", "bar")
        assert cache2.get("foo") is None

        # Invalidating via cache 2 should remove the key from cache 1
        cache1.set("foo/bar", "ipsum")
        cache1.set("foo/ba", "lorem")
        await cache2.invalidate("foo")
        await asyncio.sleep(0.1)
        assert cache1.get("foo") is None
        await cache2.invalidate_prefix("foo/")
        await asyncio.sleep(0)
        assert cache1.get("foo/bar") is None
        assert cache1.get("foo/ba") is None
    finally:
        await cache1.finalize()
        await cache2.finalize()
