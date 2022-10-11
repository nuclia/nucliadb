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
from typing import Dict, List, Optional

from nucliadb.ingest.maindb.driver import (
    DEFAULT_BATCH_SCAN_LIMIT,
    DEFAULT_SCAN_LIMIT,
    TXNID,
    Driver,
    Transaction,
)
from nucliadb.ingest.maindb.exceptions import NoWorkerCommit

try:
    import aiofiles

    FILES = True
except ImportError:
    FILES = False


class LocalTransaction(Transaction):
    modified_keys: Dict[str, bytes]
    visited_keys: Dict[str, bytes]
    deleted_keys: List[str]

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
        return f"{self.url}{key}"

    async def save(self, key: str, value: bytes):
        key = self.compute_path(key)
        folder = os.path.dirname(key)
        if not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)
        async with aiofiles.open(key, "wb+") as resp:
            await resp.write(value)

    async def remove(self, key: str):
        key = self.compute_path(key)
        os.remove(key)

    async def read(self, key: str) -> Optional[bytes]:
        try:
            async with aiofiles.open(self.compute_path(key), "rb") as resp:
                return await resp.read()
        except FileNotFoundError:
            return None
        except IsADirectoryError:
            return None

    async def commit(
        self,
        worker: Optional[str] = None,
        tid: Optional[int] = None,
        resource: bool = True,
    ):
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
        if resource:
            if worker is None or tid is None:
                raise NoWorkerCommit()
            key = TXNID.format(worker=worker)
            await self.save(key, f"{tid}".encode())
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
            for key in keys:
                obj = await self.get(key)
                if obj:
                    results.append(obj)
        return results

    async def get(self, key: str) -> Optional[bytes]:
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

    async def keys(
        self, match: str, count: int = DEFAULT_SCAN_LIMIT, include_start: bool = True
    ):
        prev_key = None

        get_all_keys = count == -1
        total_count = DEFAULT_BATCH_SCAN_LIMIT if get_all_keys else count
        real_count = 0
        path = self.compute_path(match) + "/"
        for str_key in glob.glob(path + "**", recursive=True):
            if str_key in self.deleted_keys:
                continue
            if os.path.isdir(str_key) or not os.path.exists(str_key):
                continue
            for new_key in self.modified_keys.keys():
                if (
                    match in new_key
                    and prev_key is not None
                    and prev_key < new_key
                    and new_key < str_key
                ):
                    yield new_key.replace(self.url, "")

            yield str_key.replace(self.url, "")
            if real_count >= total_count:
                break
            real_count += 1
            prev_key = str_key
        if prev_key is None:
            for new_key in self.modified_keys.keys():
                if match in new_key:
                    yield new_key.replace(self.url, "")


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

    async def begin(self) -> LocalTransaction:
        if self.url is None:
            raise AttributeError("Invalid url")
        return LocalTransaction(self.url, self)

    async def keys(
        self, match: str, count: int = DEFAULT_SCAN_LIMIT, include_start: bool = True
    ):
        path = f"{self.url}/{match}"
        actual_count = 0
        for str_key in glob.glob(path + "**", recursive=True):
            if os.path.isdir(str_key) or not os.path.exists(str_key):
                continue

            yield str_key.replace(self.url, "")
            if actual_count >= count:
                break
            actual_count += 1
