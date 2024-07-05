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
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

DEFAULT_SCAN_LIMIT = 10
DEFAULT_BATCH_SCAN_LIMIT = 500


class Transaction:
    driver: Driver
    open: bool

    async def abort(self):
        raise NotImplementedError()

    async def commit(self):
        raise NotImplementedError()

    async def batch_get(self, keys: list[str], for_update: bool = False) -> list[Optional[bytes]]:
        raise NotImplementedError()

    async def get(self, key: str, for_update: bool = False) -> Optional[bytes]:
        raise NotImplementedError()

    async def set(self, key: str, value: bytes):
        raise NotImplementedError()

    async def delete(self, key: str):
        raise NotImplementedError()

    def keys(
        self, match: str, count: int = DEFAULT_SCAN_LIMIT, include_start: bool = True
    ) -> AsyncGenerator[str, None]:
        raise NotImplementedError()

    async def count(self, match: str) -> int:
        raise NotImplementedError()


class Driver:
    initialized = False
    _abort_tasks: list[asyncio.Task] = []

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

    async def begin(self, read_only: bool = False) -> Transaction:
        raise NotImplementedError()

    @asynccontextmanager
    async def transaction(
        self, wait_for_abort: bool = True, read_only: bool = False
    ) -> AsyncGenerator[Transaction, None]:
        """
        Use to make sure transaction is always aborted.

        :param wait_for_abort: If True, wait for abort to finish before returning.
                               If False, abort is done in background (unless there
                               is an error)
        """
        txn: Optional[Transaction] = None
        error: bool = False
        try:
            txn = await self.begin(read_only=read_only)
            yield txn
        except Exception:
            error = True
            raise
        finally:
            if txn is not None and txn.open:
                if error or wait_for_abort:
                    await txn.abort()
                else:
                    self._async_abort(txn)

    def _async_abort(self, txn: Transaction):
        task = asyncio.create_task(txn.abort())
        task.add_done_callback(lambda task: self._abort_tasks.remove(task))
        self._abort_tasks.append(task)
