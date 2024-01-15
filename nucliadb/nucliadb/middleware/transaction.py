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

from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.utils import get_driver

txn_manager: ContextVar[Optional["ReadOnlyTransactionManager"]] = ContextVar(
    "txn_manager", default=None
)


class ReadOnlyTransactionMiddleware(BaseHTTPMiddleware):
    """
    This middleware provides a unique read-only transaction for each request. The transaction is
    created lazily, so if it's not used, it's not created. The middleware also ensures that the
    transaction is aborted at the end of the request.

    This is useful, for instance, on search endpoints where we want to minimize the number
    of transactions that are created.

    Usage:
        - Add this middleware to the FastAPI app:

            app = FastAPI()
            app.add_middleware(ReadOnlyTransactionMiddleware)

        - Where needed, get the transaction:

            txn = await get_read_only_transaction()
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        mgr = ReadOnlyTransactionManager()
        txn_manager.set(mgr)
        try:
            return await call_next(request)
        finally:
            await mgr.maybe_abort()
            txn_manager.set(None)


class ReadOnlyTransactionManager:
    def __init__(self):
        self._transaction: Optional[Transaction] = None
        self._lock = asyncio.Lock()
        self.aborted: bool = False

    async def get_transaction(self) -> Transaction:
        if self.aborted:
            raise RuntimeError("Transaction was aborted")

        if self._transaction is not None:
            return self._transaction

        async with self._lock:
            # Check again in case it was set while waiting for the lock
            if self._transaction is not None:
                return self._transaction

            self._transaction = await self._get_transaction()
            return self._transaction

    async def _get_transaction(self) -> Transaction:
        driver = get_driver()
        txn = await driver.begin(read_only=True)
        return txn

    async def maybe_abort(self):
        if self.aborted or self._transaction is None:
            return

        await self._transaction.abort()
        self._transaction = None
        self._lock = None
        self.aborted = True


async def get_read_only_transaction() -> Transaction:
    """
    Returns the read-only transaction for the current request
    """
    manager: Optional[ReadOnlyTransactionManager] = txn_manager.get()
    if manager is None:
        raise RuntimeError(
            "Context var is not set. Did you forget to add the ReadOnlyTransactionMiddleware to the app?"
        )
    return await manager.get_transaction()
