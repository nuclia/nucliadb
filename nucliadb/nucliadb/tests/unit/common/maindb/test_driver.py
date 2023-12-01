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
from unittest import mock

import pytest

from nucliadb.common.maindb.driver import Driver


class TransactionTest:
    def __init__(self):
        self.abort = mock.AsyncMock()
        self.open = True

    async def commit(self, **kw):
        self.open = False


@pytest.fixture(scope="function")
def txn():
    return TransactionTest()


@pytest.fixture(scope="function")
def driver(txn) -> Driver:  # type: ignore
    driver = Driver()
    with mock.patch.object(driver, "begin", new=mock.AsyncMock(return_value=txn)):
        yield driver


@pytest.mark.asyncio
async def test_driver_async_abort(driver, txn):
    async with driver.transaction(wait_for_abort=False):
        pass

    assert len(driver._abort_tasks) == 1
    await asyncio.sleep(0.1)

    txn.abort.assert_called_once()
    assert len(driver._abort_tasks) == 0


@pytest.mark.asyncio
async def test_driver_finalize_aborts_transactions(driver, txn):
    async with driver.transaction(wait_for_abort=False):
        pass

    assert len(driver._abort_tasks) == 1

    await driver.finalize()

    txn.abort.assert_called_once()

    assert len(driver._abort_tasks) == 0


@pytest.mark.asyncio
async def test_transaction_handles_txn_begin_errors(driver):
    driver.begin.side_effect = ValueError()
    testmock = mock.AsyncMock()
    with pytest.raises(ValueError):
        async with driver.transaction():
            testmock()
    testmock.assert_not_called()


@pytest.mark.asyncio
async def test_transaction_does_not_abotr_if_commited(driver):
    async with driver.transaction() as txn:
        await txn.commit()
    txn.abort.assert_not_awaited()


@pytest.mark.asyncio
async def test_transaction_aborts_if_txn_open(driver):
    async with driver.transaction() as txn:
        pass
    txn.abort.assert_awaited_once()


@pytest.mark.asyncio
async def test_transaction_aborts_on_errors(driver):
    with pytest.raises(ValueError):
        async with driver.transaction() as txn:
            raise ValueError()
    txn.abort.assert_awaited_once()


@pytest.mark.asyncio
async def test_transaction_wait_for_abort(driver):
    async with driver.transaction(wait_for_abort=False) as txn:
        pass
    txn.abort.assert_not_awaited()
    txn.abort.assert_called_once()
    txn.abort.reset_mock()

    # Should wait for abort if there are errors
    with pytest.raises(ValueError):
        async with driver.transaction(wait_for_abort=False) as txn:
            raise ValueError()
    txn.abort.assert_awaited_once()
    txn.abort.assert_called_once()
    txn.abort.reset_mock()

    async with driver.transaction(wait_for_abort=True) as txn:
        pass
    txn.abort.assert_awaited_once()
    txn.abort.assert_called_once()
    txn.abort.reset_mock()
