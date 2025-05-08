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
import contextlib
import logging
import threading
from datetime import datetime, timezone
from typing import Optional

from cachetools import TTLCache

from nucliadb.common.back_pressure.utils import BackPressureData, BackPressureException
from nucliadb_telemetry import metrics

logger = logging.getLogger(__name__)


RATE_LIMITED_REQUESTS_COUNTER = metrics.Counter(
    "nucliadb_rate_limited_requests", labels={"type": "", "cached": ""}
)


class BackPressureCache:
    """
    Global cache for storing already computed try again in times.
    It allows us to avoid making the same calculations multiple
    times if back pressure has been applied.
    """

    def __init__(self):
        self._cache = TTLCache(maxsize=1024, ttl=5 * 60)
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[BackPressureData]:
        with self._lock:
            data = self._cache.get(key, None)
            if data is None:
                return None
            if datetime.now(timezone.utc) >= data.try_after:
                # The key has expired, so remove it from the cache
                self._cache.pop(key, None)
                return None
            return data

    def set(self, key: str, data: BackPressureData):
        with self._lock:
            self._cache[key] = data


_cache = BackPressureCache()


@contextlib.contextmanager
def cached_back_pressure(cache_key: str):
    """
    Context manager that handles the caching of the try again in time so that
    we don't recompute try again times if we have already applied back pressure.
    """
    data: Optional[BackPressureData] = _cache.get(cache_key)
    if data is not None:
        back_pressure_type = data.type
        RATE_LIMITED_REQUESTS_COUNTER.inc({"type": back_pressure_type, "cached": "true"})
        raise BackPressureException(data)
    try:
        yield
    except BackPressureException as exc:
        back_pressure_type = exc.data.type
        RATE_LIMITED_REQUESTS_COUNTER.inc({"type": back_pressure_type, "cached": "false"})
        _cache.set(cache_key, exc.data)
        raise exc
