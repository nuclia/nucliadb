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
from typing import AsyncGenerator, List, Optional

DEFAULT_SCAN_LIMIT = 10
DEFAULT_BATCH_SCAN_LIMIT = 100


class Transaction:
    driver: Driver
    open: bool

    async def abort(self):
        raise NotImplementedError()

    async def commit(self):
        raise NotImplementedError()

    async def batch_get(self, keys: List[str]):
        raise NotImplementedError()

    async def get(self, key: str) -> Optional[bytes]:
        raise NotImplementedError()

    async def set(self, key: str, value: bytes):
        raise NotImplementedError()

    async def delete(self, key: str):
        raise NotImplementedError()

    def keys(
        self, match: str, count: int = DEFAULT_SCAN_LIMIT, include_start: bool = True
    ) -> AsyncGenerator[str, None]:
        raise NotImplementedError()


class Driver:
    initialized = False

    async def initialize(self):
        raise NotImplementedError()

    async def finalize(self):
        raise NotImplementedError()

    async def begin(self) -> Transaction:
        raise NotImplementedError()

    async def keys(
        self, match: str, count: int = DEFAULT_SCAN_LIMIT, include_start: bool = True
    ) -> AsyncGenerator[str, None]:
        raise NotImplementedError()
        # mypy is funny in some cases and wants a yield to detect the type properly
        yield

    @asynccontextmanager
    async def transaction(
        self, wait_for_abort: bool = True
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
            txn = await self.begin()
            yield txn
        except Exception:
            error = True
            raise
        finally:
            if txn is None or not txn.open:
                # no need to abort
                return
            if wait_for_abort or error:
                await txn.abort()
            else:
                asyncio.create_task(txn.abort())
