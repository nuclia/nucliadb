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

from unittest.mock import AsyncMock, patch

import pytest

from nucliadb.common.cluster.exceptions import NodeError, ShardNotFound
from nucliadb.ingest import purge

pytestmark = pytest.mark.asyncio


class DataIterator:
    def __init__(self, data):
        self.data = data

    def __call__(self, *args, **kwargs):
        return self

    async def __aiter__(self):
        for item in self.data:
            yield item


@pytest.fixture
def keys():
    yield []


@pytest.fixture
def driver(keys):
    mock = AsyncMock()
    mock.keys = DataIterator(keys)
    yield mock


@pytest.fixture
def storage():
    mock = AsyncMock()
    mock.delete_kb.return_value = True, False
    yield mock


@pytest.fixture(autouse=True)
def kb():
    mock = AsyncMock()
    with patch("nucliadb.ingest.purge.KnowledgeBox", mock):
        yield mock


async def test_purge(kb, keys, driver):
    keys.append("/pathto/kbid")

    await purge.purge_kb(driver)

    kb.purge.assert_called_once_with(driver, "kbid")
    driver.begin.return_value.commit.assert_called_once()


async def test_purge_handle_errors(kb, keys, driver):
    keys.append("/failed")
    keys.append("/pathto/failed")
    keys.append("/pathto/failed")
    keys.append("/pathto/failed")
    keys.append("/pathto/failed")

    kb.purge.side_effect = [ShardNotFound(), NodeError(), Exception(), None]
    driver.begin.return_value.delete.side_effect = Exception()

    await purge.purge_kb(driver)

    driver.begin.return_value.commit.assert_not_called()
    driver.begin.return_value.abort.assert_called_once()


async def test_purge_kb_storage(keys, driver, storage):
    keys.append("/pathto/kbid")

    await purge.purge_kb_storage(driver, storage)

    driver.begin.return_value.commit.assert_called_once()


async def test_purge_kb_storage_handle_errors(keys, driver, storage):
    keys.append("/failed")
    keys.append("/pathto/failed")

    driver.begin.return_value.delete.side_effect = Exception()

    await purge.purge_kb_storage(driver, storage)

    driver.begin.return_value.commit.assert_not_called()


async def test_main(driver, storage):
    with patch("nucliadb.ingest.purge.purge_kb", AsyncMock()) as purge_kb, patch(
        "nucliadb.ingest.purge.purge_kb_storage", AsyncMock()
    ) as purge_kb_storage, patch(
        "nucliadb.ingest.purge.get_storage", return_value=storage
    ), patch(
        "nucliadb.ingest.purge.setup_driver", return_value=driver
    ):
        await purge.main()

        purge_kb.assert_called_once_with(driver)
        purge_kb_storage.assert_called_once_with(driver, storage)
