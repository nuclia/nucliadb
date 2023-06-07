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

from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from nucliadb.ingest.orm.processor import Processor


@pytest.fixture()
def txn():
    yield AsyncMock()


@pytest.fixture()
def driver(txn):
    mock = MagicMock()
    mock.transaction.return_value.__aenter__.return_value = txn
    yield mock


@pytest.fixture()
def processor(driver):
    yield Processor(driver, None)


@pytest.fixture()
def shard():
    yield AsyncMock()


@pytest.fixture()
def resource():
    yield MagicMock()


async def test_commit_slug(processor: Processor, txn, resource):
    another_txn = Mock()
    resource.txn = another_txn
    resource.set_slug = AsyncMock()

    await processor.commit_slug(resource)

    resource.set_slug.assert_awaited_once()
    txn.commit.assert_awaited_once()
    assert resource.txn is another_txn
