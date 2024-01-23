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
from unittest import mock

import pytest
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.testclient import TestClient

from nucliadb.middleware.transaction import (
    ReadOnlyTransactionManager,
    ReadOnlyTransactionMiddleware,
    get_read_only_transaction,
)


class TestCaseReadOnlyTransactionMiddleware:
    @pytest.fixture(scope="class")
    def app(self):
        app_ = Starlette()
        app_.add_middleware(ReadOnlyTransactionMiddleware)

        @app_.route("/foo/")
        async def foo(request):
            txn = await get_read_only_transaction()
            value = await txn.get("foo")
            return PlainTextResponse(value)

        @app_.route("/bar/")
        async def bar(request):
            return PlainTextResponse("Bar")

        return app_

    @pytest.fixture
    def txn(self):
        txn = mock.Mock()
        txn.get = mock.AsyncMock(return_value="Foo")
        txn.abort = mock.AsyncMock()
        return txn

    @pytest.fixture
    def driver(self, txn):
        driver = mock.Mock()
        driver.begin = mock.AsyncMock(return_value=txn)
        with mock.patch(
            "nucliadb.middleware.transaction.get_driver", return_value=driver
        ):
            yield

    @pytest.fixture
    def client(self, driver, app):
        return TestClient(app)

    def test_txn_is_aborted_when_used(self, client, txn):
        response = client.get("/foo/")
        assert response.text == "Foo"

        txn.get.assert_called_once_with("foo")
        txn.abort.assert_called_once()

    def test_txn_is_not_aborted_when_not_used(self, client, txn):
        _ = client.get("/bar/")
        assert txn.abort.call_count == 0


async def test_get_read_only_transaction_runtime_error_if_not_configured():
    with pytest.raises(RuntimeError):
        await get_read_only_transaction()


async def test_transaction_manager_raises_on_aborted():
    mgr = ReadOnlyTransactionManager()
    mgr.aborted = True
    with pytest.raises(RuntimeError):
        await mgr.get_transaction()


async def test_txn_manager_creates_transaction_only_once():
    mgr = ReadOnlyTransactionManager()
    txn = mock.Mock()
    get_txn = mock.AsyncMock(return_value=txn)
    mgr._get_transaction = get_txn
    assert await mgr.get_transaction() is txn
    assert await mgr.get_transaction() is txn
    mgr._get_transaction.assert_called_once()


async def test_txn_manager_maybe_abort():
    mgr = ReadOnlyTransactionManager()
    mgr.aborted = True
    await mgr.maybe_abort()
    mgr.aborted = False

    mgr._transaction = None
    await mgr.maybe_abort()

    txn = mock.Mock()
    txn.abort = mock.AsyncMock()
    mgr._transaction = txn
    await mgr.maybe_abort()
    txn.abort.assert_called_once()
