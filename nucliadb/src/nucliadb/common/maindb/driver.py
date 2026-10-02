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
from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import ClassVar

DEFAULT_SCAN_LIMIT = -1
DEFAULT_BATCH_SCAN_LIMIT = 500


class Transaction:
    driver: Driver
    open: bool
    kbid: str | None = None

    async def abort(self):
        raise NotImplementedError()

    async def commit(self):
        raise NotImplementedError()

    async def batch_get(self, keys: list[str], for_update: bool = False) -> list[bytes | None]:
        raise NotImplementedError()

    async def get(self, key: str, for_update: bool = False) -> bytes | None:
        raise NotImplementedError()

    async def set(self, key: str, value: bytes):
        raise NotImplementedError()

    async def insert(self, key: str, value: bytes):
        return await self.set(key, value)

    async def delete(self, key: str):
        raise NotImplementedError()

    async def delete_by_prefix(self, prefix: str) -> None:
        raise NotImplementedError()

    def keys(
        self,
        match: str,
        count: int = DEFAULT_SCAN_LIMIT,
        include_start: bool = True,
    ) -> AsyncGenerator[str]:
        raise NotImplementedError()

    async def count(self, match: str) -> int:
        raise NotImplementedError()


class Driver:
    initialized = False
    _abort_tasks: ClassVar[list[asyncio.Task]] = []

    async def initialize(self):
        raise NotImplementedError()

    async def finalize(self):
        while len(self._abort_tasks) > 0:
            task = self._abort_tasks.pop()
            if not task.done():
                try:
                    await task
                except Exception:
                    pass

    @asynccontextmanager
    async def _transaction(
        self, *, read_only: bool, kbid: str | None = None
    ) -> AsyncGenerator[Transaction]:
        yield Transaction()

    @staticmethod
    def _validate_transaction_scope(kbid: str | None, system: bool | None) -> None:
        if system is None:
            return
        if system and kbid is not None:
            raise ValueError("Pass either kbid or system=True, not both")
        if not system and kbid is None:
            raise ValueError("Pass either kbid or system=True")

    @asynccontextmanager
    async def ro_transaction(
        self, *, kbid: str | None = None, system: bool | None = None
    ) -> AsyncGenerator[Transaction]:
        """Open a KB-scoped transaction, or a system transaction when kbid is omitted."""
        self._validate_transaction_scope(kbid, system)
        async with self._transaction(read_only=True, kbid=kbid) as txn:
            yield txn

    @asynccontextmanager
    async def rw_transaction(
        self, *, kbid: str | None = None, system: bool | None = None
    ) -> AsyncGenerator[Transaction]:
        """Open a KB-scoped transaction, or a system transaction when kbid is omitted."""
        self._validate_transaction_scope(kbid, system)
        async with self._transaction(read_only=False, kbid=kbid) as txn:
            yield txn
