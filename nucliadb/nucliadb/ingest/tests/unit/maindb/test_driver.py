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

from nucliadb.ingest.maindb.driver import Driver


@pytest.fixture(scope="function")
def driver() -> Driver:  # type: ignore
    driver = Driver()
    with mock.patch.object(
        driver, "begin", new=mock.AsyncMock(return_value=mock.AsyncMock())
    ):
        yield driver


@pytest.mark.asyncio
async def test_transaction_aborts(driver):
    async with driver.transaction() as txn:
        pass
    txn.abort.assert_awaited_once()


@pytest.mark.asyncio
async def test_transaction_aborts_on_errors(driver):
    with pytest.raises(ValueError):
        async with driver.transaction() as txn:
            raise ValueError()
    txn.abort.assert_awaited_once()
