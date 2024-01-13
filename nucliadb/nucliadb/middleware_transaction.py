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

import asyncio
from contextvars import ContextVar

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from nucliadb import logger
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.utils import get_driver

txn_getter = ContextVar("txn_getter", default=None)


class ReadOnlyTransactionMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        getter = LazyTransactionGetter()
        txn_getter.set(getter)  # type: ignore
        try:
            return await call_next(request)
        finally:
            await getter.maybe_abort()
            txn_getter.set(None)


class LazyTransactionGetter:
    def __init__(self):
        self._transaction = None
        self._lock = asyncio.Lock()

    async def get_transaction(self, read_only: bool = True) -> Transaction:
        if self._transaction is None:
            txn = await self._safe_get_transaction(read_only=read_only)
            self._transaction = txn
        else:
            logger.debug("Using existing transaction")
        return self._transaction

    async def _safe_get_transaction(self, read_only: bool = True) -> Transaction:
        async with self._lock:
            if self._transaction is not None:
                logger.debug("Using existing transaction")
                # Check again in case it was set while waiting for the lock
                return self._transaction
            logger.debug("Creating transaction")
            driver = get_driver()
            txn = await driver.begin(read_only=read_only)
            return txn

    async def maybe_abort(self):
        if self._transaction is not None:
            logger.debug("Aborting transaction")
            txn = self._transaction
            asyncio.current_task().add_done_callback(  # type: ignore
                # Automatically abort the transaction when the task is done
                # This can be dangereous when:
                # - There is a sub task that uses the transaction and outlives the parent task
                #    - ^^ DON'T DO THIS. Manage the transaction yourself
                lambda task: asyncio.create_task(txn.abort())  # type: ignore
            )
            self._transaction = None
        else:
            logger.debug("No transaction to abort")


async def get_transaction(read_only: bool = True) -> Transaction:
    getter = txn_getter.get()
    if getter is None:
        raise RuntimeError(
            "Transaction getter not set. Did you forget to add the ReadOnlyTransactionMiddleware?"
        )
    return await getter.get_transaction(read_only=read_only)
