from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nucliadb.ingest.orm.processor import Processor


@pytest.fixture()
def driver():
    mock = MagicMock()
    mock.begin = AsyncMock()
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


async def test_mark_resource_error(processor: Processor, driver, shard, resource):
    with patch("nucliadb.ingest.orm.processor.set_basic") as set_basic:
        await processor._mark_resource_error(
            resource, partition="partition", seqid=1, shard=shard, kbid="kbid"
        )

    txn = driver.begin.return_value
    txn.commit.assert_called_once()
    set_basic.assert_called_once_with(
        txn, resource.kb.kbid, resource.uuid, resource.basic
    )

    shard.add_resource.assert_called_once_with(
        resource.indexer.brain, 1, partition="partition", kb="kbid"
    )


async def test_mark_resource_error_handle_error(
    processor: Processor, shard, resource, driver
):
    with patch("nucliadb.ingest.orm.processor.set_basic") as set_basic:
        set_basic.side_effect = Exception("test")
        await processor._mark_resource_error(
            resource, partition="partition", seqid=1, shard=shard, kbid="kbid"
        )

    txn = driver.begin.return_value
    txn.commit.assert_not_called()
    txn.abort.assert_called_once()


async def test_mark_resource_error_skip_no_shard(
    processor: Processor, resource, driver
):
    await processor._mark_resource_error(
        resource, partition="partition", seqid=1, shard=None, kbid="kbid"
    )

    driver.begin.assert_not_called()


async def test_mark_resource_error_skip_no_resource(
    processor: Processor, shard, driver
):
    await processor._mark_resource_error(
        None, partition="partition", seqid=1, shard=shard, kbid="kbid"
    )

    driver.begin.assert_not_called()
