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
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Optional
from unittest.mock import Mock

from nucliadb.common.cache import Cache, CacheMetrics, _use_cache


class DummyCache(Cache[Mock]):
    metrics = CacheMetrics(
        _cache_size=Mock(),
        ops=Mock(),
    )

    def __len__(self):
        return self.cache.__len__()


dummy: ContextVar[Optional[DummyCache]] = ContextVar("dummy", default=None)


@contextmanager
def dummy_cache(cache_size: int):
    with _use_cache(DummyCache, dummy, cache_size=cache_size) as cache:
        yield cache


def test_cache_decorator():
    with dummy_cache(2) as cache:
        cache.set("b", 2)
        cache.set("a", 1)
        assert len(cache) == 2

        cache.set("c", 3)
        assert len(cache) == 2, "cache should have evicted an element"

        assert "b" not in cache
        assert cache.get("b") is None
        assert "a" in cache
        assert cache.get("a") == 1
        assert "c" in cache
        assert cache.get("c") == 3

    assert len(cache) == 0, "decorator should cleanup the cache"


def test_cache_cleanup_with_errors():
    try:
        with dummy_cache(2) as cache:
            cache.set("b", 2)
            raise Exception
    except Exception:
        pass

    assert len(cache) == 0, "decorator should cleanup the cache on error"
