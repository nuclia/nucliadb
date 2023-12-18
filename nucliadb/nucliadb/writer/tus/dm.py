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
import time
from typing import Any, Optional

import orjson
from redis import asyncio as aioredis
from starlette.requests import Request

from nucliadb.writer import logger

from .exceptions import HTTPPreconditionFailed


class NoRedisConfigured(Exception):
    pass


DATA: dict[str, Any] = {}


class FileDataManager:
    _data: Optional[dict[str, Any]] = None
    _loaded = False
    key = None
    _ttl = 60 * 50 * 5  # 5 minutes should be plenty of time between activity

    async def load(self, key):
        # preload data
        self.key = key
        if self._data is None:
            data = DATA.get(self.key)
            if not data:
                self._data = {}
            else:
                self._data = orjson.loads(data)
                self._loaded = True

    def protect(self, request: Request):
        if self._data and "last_activity" in self._data:
            # check for another active upload, fail if we're screwing with
            # someone else
            last_activity: Optional[int] = self._data.get("last_activity")
            if last_activity and (time.time() - last_activity) < self._ttl:
                if (
                    request.headers
                    and request.headers.get("tus-override-upload", "0") != "1"
                ):
                    raise HTTPPreconditionFailed(
                        detail="There is already an active tusupload that conflicts with this one."
                    )

    async def start(self, request):
        self.protect(request)
        self._data.clear()

    async def save(self):
        if self.key is None:
            raise Exception("Not initialized")
        self._data["last_activity"] = time.time()
        value = orjson.dumps(self._data)
        DATA[self.key] = value

    async def update(self, **kwargs):
        self._data.update(kwargs)

    async def finish(self, values=None):
        await self._delete_key()

    async def _delete_key(self):
        if self.key is None:
            raise Exception("Not initialized")
        # and clear the cache key
        if self.key in DATA:
            del DATA[self.key]

    @property
    def metadata(self):
        return self._data.get("metadata", {})

    @property
    def content_type(self):
        return self.metadata.get("content_type")

    @property
    def size(self) -> int:
        default = 0
        if self._data is None:
            return default
        size = self._data.get("size")
        if size is None:
            return default
        return int(size)

    @property
    def offset(self):
        return self._data.get("offset", 0)

    @property
    def filename(self):
        return self.metadata.get("filename", 0)

    def get(self, name, default=None):
        if self._data is None:
            return default
        return self._data.get(name, default)


class RedisFileDataManagerFactory:
    """
    Allow sharing a single redis pool between multiple data managers.
    """

    def __init__(self, redis_url: str):
        self.redis = aioredis.from_url(redis_url)

    def __call__(self):
        return RedisFileDataManager(self.redis)

    async def finalize(self):
        try:
            await self.redis.close(close_connection_pool=True)
        except Exception:
            logger.warning("Error closing redis connection", exc_info=True)
            pass


class RedisFileDataManager(FileDataManager):
    def __init__(self, redis: aioredis.Redis):
        self.redis = redis

    async def load(self, key):
        # preload data
        self.key = key
        if self._data is None:
            data = await self.redis.get(self.key)
            if not data:
                self._data = {}
            else:
                self._data = orjson.loads(data)
                self._loaded = True

    async def save(self):
        if self.key is None:
            raise Exception("Not initialized")
        self._data["last_activity"] = time.time()
        value = orjson.dumps(self._data)
        await self.redis.set(self.key, value, ex=self._ttl)

    async def _delete_key(self):
        if self.key is None:
            raise Exception("Not initialized")
        # and clear the cache key
        await self.redis.delete(self.key)
