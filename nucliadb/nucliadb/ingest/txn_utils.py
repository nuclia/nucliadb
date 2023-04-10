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
import asyncio
from contextvars import ContextVar
from typing import Optional

from nucliadb.ingest.maindb.driver import Transaction
from nucliadb.ingest.utils import get_driver

txn: ContextVar[Optional[Transaction]] = ContextVar("txn", default=None)
txn_lock: ContextVar[Optional[asyncio.Lock]] = ContextVar("txn_lock", default=None)


def _get_transaction_lock() -> asyncio.Lock:
    lock = txn_lock.get()
    if lock is None:
        lock = asyncio.Lock()
        txn_lock.set(lock)
    return lock


async def get_transaction() -> Transaction:
    transaction: Optional[Transaction] = txn.get()
    if transaction is None:
        async with _get_transaction_lock():
            check_txn_again = (
                txn.get()
            )  # need to pull txn again in case something else got it
            if check_txn_again is not None:
                return check_txn_again

            driver = await get_driver()
            transaction = await driver.begin()
            txn.set(transaction)

            asyncio.current_task().add_done_callback(  # type: ignore
                # Automatically abort the transaction when the task is done
                # This can be dangereous when:
                # - There is a sub task that uses the transaction and outlives the parent task
                #    - ^^ DON'T DO THIS. Manage the transaction yourself
                lambda task: asyncio.create_task(transaction.abort())  # type: ignore
            )

    return transaction


async def abort_transaction() -> None:
    transaction: Optional[Transaction] = txn.get()
    if transaction is not None:
        async with _get_transaction_lock():
            await transaction.abort()
