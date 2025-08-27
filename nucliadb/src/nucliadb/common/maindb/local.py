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
import glob
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from nucliadb.common.maindb.driver import (
    DEFAULT_BATCH_SCAN_LIMIT,
    DEFAULT_SCAN_LIMIT,
    Driver,
    Transaction,
)

try:
    import aiofiles

    FILES = True
except ImportError:  # pragma: no cover
    FILES = False


class LocalTransaction(Transaction):
    modified_keys: dict[str, bytes]
    visited_keys: dict[str, bytes]
    deleted_keys: list[str]

    def __init__(self, url: str, driver: Driver):
        self.url = url
        self.open = True
        self.driver = driver
        self.modified_keys = {}
        self.visited_keys = {}
        self.deleted_keys = []

    def clean(self):
        self.modified_keys.clear()
        self.visited_keys.clear()
        self.deleted_keys.clear()

    async def abort(self):
        self.clean()
        self.open = False

    def compute_path(self, key: str):
        return f"{self.url}{key.rstrip('/')}/__data__"

    async def save(self, key: str, value: bytes):
        key = self.compute_path(key)
        folder = os.path.dirname(key)
        if not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)
        async with aiofiles.open(key, "wb+") as resp:
            await resp.write(value)

    async def remove(self, key: str):
        try:
            path = self.compute_path(key)
            os.remove(path)
        except FileNotFoundError:
            # Deleting a key that does not exist
            pass

    async def read(self, key: str) -> Optional[bytes]:
        try:
            async with aiofiles.open(self.compute_path(key), "rb") as resp:
                return await resp.read()
        except FileNotFoundError:
            return None
        except IsADirectoryError:
            return None
        except NotADirectoryError:
            return None

    async def commit(self):
        if len(self.modified_keys) == 0 and len(self.deleted_keys) == 0:
            self.clean()
            return

        not_to_check = []
        count = 0
        for key, value in self.modified_keys.items():
            await self.save(key, value)
            count += 1
        for key in self.deleted_keys:
            await self.remove(key)
            not_to_check.append(count)
            count += 1
        self.clean()
        self.open = False

    async def batch_get(self, keys: list[str], for_update: bool = False) -> list[Optional[bytes]]:
        results: list[Optional[bytes]] = []
        for key in keys:
            obj = await self.get(key)
            if obj:
                results.append(obj)
            else:
                results.append(None)

        for idx, key in enumerate(keys):
            if key in self.deleted_keys:
                results[idx] = None
            if key in self.modified_keys:
                results[idx] = self.modified_keys[key]
            if key in self.visited_keys:
                results[idx] = self.visited_keys[key]

        return results

    async def get(self, key: str, for_update: bool = False) -> Optional[bytes]:
        if key in self.deleted_keys:
            raise KeyError(f"Not found {key}")

        if key in self.modified_keys:
            return self.modified_keys[key]

        if key in self.visited_keys:
            return self.visited_keys[key]

        else:
            obj = await self.read(key)
            if obj is not None:
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

    async def delete_by_prefix(self, prefix: str) -> None:
        keys = []
        for key in self.modified_keys.keys():
            if key.startswith(prefix):
                keys.append(key)
        for key in keys:
            await self.delete(key)

    async def keys(self, match: str, count: int = DEFAULT_SCAN_LIMIT, include_start: bool = True):
        prev_key = None

        get_all_keys = count == -1
        total_count = DEFAULT_BATCH_SCAN_LIMIT if get_all_keys else count
        real_count = 0
        path = self.compute_path(match).replace("/__data__", "") + "/"
        for str_key in glob.glob(path + "**", recursive=True):
            real_key = str_key.replace("/__data__", "")
            if real_key in self.deleted_keys:
                continue
            if os.path.isdir(str_key) or not os.path.exists(str_key):
                continue
            for new_key in self.modified_keys.keys():
                if (
                    match in new_key
                    and prev_key is not None
                    and prev_key < new_key
                    and new_key < real_key
                ):
                    yield new_key.replace(self.url, "")

            yield real_key.replace(self.url, "")
            if real_count >= total_count:
                break
            real_count += 1
            prev_key = real_key
        if prev_key is None:
            for new_key in self.modified_keys.keys():
                if match in new_key:
                    yield new_key.replace(self.url, "")

    async def count(self, match: str) -> int:
        value = 0
        async for _ in self.keys(match):
            value += 1
        return value


class LocalDriver(Driver):
    url: str

    def __init__(self, url: str):
        self.url = os.path.abspath(url.rstrip("/"))

    async def initialize(self):
        if self.initialized is False and os.path.exists(self.url) is False:
            os.makedirs(self.url, exist_ok=True)
        self.initialized = True

    async def finalize(self):
        pass

    @asynccontextmanager
    async def _transaction(self, *, read_only: bool) -> AsyncGenerator[Transaction, None]:
        if self.url is None:
            raise AttributeError("Invalid url")
        txn = LocalTransaction(self.url, self)
        try:
            yield txn
        finally:
            if txn.open:
                await txn.abort()
