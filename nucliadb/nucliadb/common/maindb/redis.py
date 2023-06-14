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
from typing import Any, Dict, List, Optional

from nucliadb.common.maindb.driver import (
    DEFAULT_BATCH_SCAN_LIMIT,
    DEFAULT_SCAN_LIMIT,
    Driver,
    Transaction,
)

try:
    from redis import asyncio as aioredis

    REDIS = True
except ImportError:  # pragma: no cover
    REDIS = False


class RedisTransaction(Transaction):
    modified_keys: Dict[str, bytes]
    visited_keys: Dict[str, bytes]
    deleted_keys: List[str]

    def __init__(self, redis: Any, driver: Driver):
        self.redis = redis
        self.driver = driver
        self.modified_keys = {}
        self.visited_keys = {}
        self.deleted_keys = []
        self.open = True

    def clean(self):
        self.modified_keys.clear()
        self.visited_keys.clear()
        self.deleted_keys.clear()

    async def abort(self):
        self.clean()
        self.open = False

    async def commit(self):
        if len(self.modified_keys) == 0 and len(self.deleted_keys) == 0:
            self.clean()
            return

        not_to_check = []
        async with self.redis.pipeline(transaction=True) as pipe:
            count = 0
            for key, value in self.modified_keys.items():
                pipe = pipe.set(key.encode(), value)
                count += 1
            for key in self.deleted_keys:
                pipe = pipe.delete(key.encode())
                not_to_check.append(count)
                count += 1
            oks = await pipe.execute()

        for index, ok in enumerate(oks):
            # We do no check deleted if its already deleted
            if index not in not_to_check:
                assert ok
        self.clean()
        self.open = False

    async def batch_get(self, keys: List[str]):
        results = []
        for key in keys:
            if key in self.deleted_keys:
                raise KeyError(f"Not found {key}")

            if key in self.modified_keys:
                results.append(self.modified_keys[key])
                keys.remove(key)

            if key in self.visited_keys:
                results.append(self.visited_keys[key])
                keys.remove(key)

        if len(keys) > 0:
            bytes_keys: List[bytes] = [x.encode() for x in keys]
            objs = await self.redis.mget(bytes_keys)
            results.extend(objs)
        return results

    async def get(self, key: str) -> Optional[bytes]:
        if key in self.deleted_keys:
            raise KeyError(f"Not found {key}")

        if key in self.modified_keys:
            return self.modified_keys[key]

        if key in self.visited_keys:
            return self.visited_keys[key]

        else:
            obj = await self.redis.get(key.encode())
            self.visited_keys[key] = obj
            return obj

    async def set(self, key: str, value: bytes):
        if key in self.deleted_keys:
            self.deleted_keys.remove(key)

        if key in self.visited_keys:
            del self.visited_keys[key]

        self.modified_keys[key] = value

    async def delete(self, key: str):
        if key not in self.deleted_keys:
            self.deleted_keys.append(key)

        if key in self.visited_keys:
            del self.visited_keys[key]

        if key in self.modified_keys:
            del self.modified_keys[key]

    async def keys(
        self, match: str, count: int = DEFAULT_SCAN_LIMIT, include_start: bool = True
    ):
        prev_key = None

        get_all_keys = count == -1
        _count = DEFAULT_BATCH_SCAN_LIMIT if get_all_keys else count

        async with self.redis.client() as conn:
            async for key in conn.scan_iter(match=match.encode() + b"*", count=_count):
                str_key = key.decode()
                if str_key in self.deleted_keys:
                    continue
                for new_key in self.modified_keys.keys():
                    if (
                        match in new_key
                        and prev_key is not None
                        and prev_key < new_key
                        and new_key < str_key
                    ):
                        yield new_key

                yield str_key
                prev_key = str_key
        if prev_key is None:
            for new_key in self.modified_keys.keys():
                if match in new_key:
                    yield new_key


class RedisDriver(Driver):
    redis = None
    url = None

    def __init__(self, url: str):
        if REDIS is False:
            raise ImportError("Redis is not installed")
        self.url = url

    async def initialize(self):
        if self.initialized is False and self.redis is None:
            self.redis = aioredis.from_url(self.url)
        self.initialized = True

    async def finalize(self):
        if self.initialized is True:
            await self.redis.close()
            self.initialized = False

    async def begin(self) -> RedisTransaction:
        return RedisTransaction(self.redis, driver=self)

    async def keys(
        self, match: str, count: int = DEFAULT_SCAN_LIMIT, include_start: bool = True
    ):
        if self.redis is None:
            raise AttributeError()
        async with self.redis.client() as conn:
            async for key in conn.scan_iter(match=match.encode() + b"*", count=count):
                yield key.decode()
