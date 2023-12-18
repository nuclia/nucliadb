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
from unittest.mock import AsyncMock, Mock

import pytest
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.testclient import TestClient

from nucliadb.middleware.transaction import (
    ReadOnlyTransactionMiddleware,
    get_request_readonly_transaction,
    setup_request_readonly_transaction,
)


class TestCaseProcessTimeHeaderMiddleware:
    @pytest.fixture(scope="class")
    def txn(self):
        txn = Mock()
        txn.get = AsyncMock(return_value="bar")
        txn.abort = AsyncMock()
        yield txn

    @pytest.fixture(scope="class")
    def driver(self, txn):
        driver = Mock()
        driver.begin = AsyncMock(return_value=txn)
        with mock.patch(
            "nucliadb.middleware.transaction.get_driver", return_value=driver
        ):
            yield driver

    @pytest.fixture(scope="class")
    def app(self, txn):
        app_ = Starlette()
        app_.add_middleware(ReadOnlyTransactionMiddleware)

        @app_.route("/foo/")
        async def foo(request):
            await setup_request_readonly_transaction()
            txn = get_request_readonly_transaction()
            val = await txn.get("foo")
            assert val == "bar"
            return PlainTextResponse("Foo")

        return app_

    @pytest.fixture
    def client(self, app):
        return TestClient(app)

    def test_middleware(self, client, driver, txn):
        response = client.get("/foo/")
        assert response.status_code == 200

        driver.begin.assert_called_once_with(read_only=True)
        txn.get.assert_called_once_with("foo")
        txn.abort.assert_called_once()


def test_get_request_readonly_transaction_raises_error_if_not_setup():
    with pytest.raises(RuntimeError):
        get_request_readonly_transaction()


async def test_setup_request_readonly_transaction_raises_error_if_not_in_context():
    with pytest.raises(RuntimeError):
        await setup_request_readonly_transaction()
