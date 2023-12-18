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
import uuid
from contextvars import ContextVar
from typing import Optional

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from nucliadb import logger
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.utils import get_driver

request_id: ContextVar[Optional[str]] = ContextVar("request_id", default=None)
transactions: dict[str, Transaction] = {}


class ReadOnlyTransactionMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        request_id.set(str(uuid.uuid4()))
        try:
            return await call_next(request)
        finally:
            maybe_schedule_abort_transaction()
            request_id.set(None)


async def setup_request_readonly_transaction():
    rid = request_id.get()
    if rid is None:
        raise RuntimeError("Request id not set")

    txn = transactions.get(rid)
    if txn is not None:
        logger.warning(f"There is already a transaction for request {rid}")
        return

    logger.debug("Begin read only transaction")
    driver = get_driver()
    txn = await driver.begin(read_only=True)
    transactions[rid] = txn


def get_request_readonly_transaction():
    rid = request_id.get()
    if rid not in transactions:
        raise RuntimeError("Transaction not set or already aborted")
    return transactions[rid]


def maybe_schedule_abort_transaction():
    rid = request_id.get()
    txn = transactions.pop(rid, None)
    if txn is None:
        return
    logger.debug("Abort transaction scheduled")
    asyncio.current_task().add_done_callback(  # type: ignore
        # Automatically abort the transaction when the task is done
        # This can be dangereous when:
        # - There is a sub task that uses the transaction and outlives the parent task
        #    - ^^ DON'T DO THIS. Manage the transaction yourself
        lambda task: asyncio.create_task(txn.abort())  # type: ignore
    )
