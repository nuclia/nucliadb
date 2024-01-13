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
from typing import Optional

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from nucliadb import logger
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.utils import get_driver

txn_getter: ContextVar[Optional["LazyTransactionGetter"]] = ContextVar(
    "txn_getter", default=None
)


class ReadOnlyTransactionMiddleware(BaseHTTPMiddleware):
    """
    The middleware sets a LazyTransactionGetter in a context variable at the start
    of the request so that any subtask can get a unique request transaction.
    The transaction is automatically aborted when the request is finished.
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        lazy_getter = LazyTransactionGetter()
        txn_getter.set(lazy_getter)
        try:
            return await call_next(request)
        finally:
            await lazy_getter.maybe_abort()
            txn_getter.set(None)


class LazyTransactionGetter:
    def __init__(self):
        self._transaction = None
        self._lock = asyncio.Lock()

    async def get_read_only_transaction(self) -> Transaction:
        if self._transaction is not None:
            logger.debug("Using existing transaction")
            return self._transaction

        async with self._lock:
            # Check again in case it was set while waiting for the lock
            if self._transaction is not None:
                logger.debug("Using existing transaction")
                return self._transaction

            logger.debug("Creating transaction")
            self._transaction = await self._get_transaction()
            return self._transaction

    async def _get_transaction(self) -> Transaction:
        driver = get_driver()
        txn = await driver.begin(read_only=True)
        return txn

    async def maybe_abort(self):
        if self._transaction is None:
            logger.debug("No transaction to abort")
            return
        logger.debug("Aborting transaction")
        await self._transaction.abort()
        self._transaction = None


async def get_read_only_transaction() -> Transaction:
    lazy_getter: Optional[LazyTransactionGetter] = txn_getter.get()
    if lazy_getter is None:
        raise RuntimeError(
            "Transaction getter not set. Did you forget to add the ReadOnlyTransactionMiddleware?"
        )
    return await lazy_getter.get_read_only_transaction()
