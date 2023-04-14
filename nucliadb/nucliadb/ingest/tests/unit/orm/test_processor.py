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

from unittest.mock import AsyncMock, MagicMock, patch

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


async def test_mark_resource_error(processor: Processor, txn, shard, resource):
    with patch("nucliadb.ingest.orm.processor.set_basic") as set_basic:
        await processor._mark_resource_error(
            resource, partition="partition", seqid=1, shard=shard, kbid="kbid"
        )

    txn.commit.assert_called_once()
    set_basic.assert_called_once_with(
        txn, resource.kb.kbid, resource.uuid, resource.basic
    )

    shard.add_resource.assert_called_once_with(
        resource.indexer.brain, 1, partition="partition", kb="kbid"
    )


async def test_mark_resource_error_handle_error(
    processor: Processor, shard, resource, txn
):
    with patch("nucliadb.ingest.orm.processor.set_basic") as set_basic:
        set_basic.side_effect = Exception("test")
        await processor._mark_resource_error(
            resource, partition="partition", seqid=1, shard=shard, kbid="kbid"
        )

    txn.commit.assert_not_called()


async def test_mark_resource_error_skip_no_shard(
    processor: Processor, resource, driver
):
    await processor._mark_resource_error(
        resource, partition="partition", seqid=1, shard=None, kbid="kbid"
    )

    driver.transaction.assert_not_called()


async def test_mark_resource_error_skip_no_resource(
    processor: Processor, shard, driver
):
    await processor._mark_resource_error(
        None, partition="partition", seqid=1, shard=shard, kbid="kbid"
    )

    driver.transaction.assert_not_called()
