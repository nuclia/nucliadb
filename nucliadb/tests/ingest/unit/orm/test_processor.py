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

from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.ingest.orm.exceptions import ResourceNotIndexable
from nucliadb.ingest.orm.processor import Processor, validate_indexable_resource
from nucliadb_protos import noderesources_pb2


@pytest.fixture()
def txn():
    yield AsyncMock()


@pytest.fixture()
def driver(txn):
    mock = MagicMock()
    mock.transaction.return_value.__aenter__.return_value = txn
    yield mock


@pytest.fixture()
def sm():
    mock = AsyncMock()
    mock.add_resource = AsyncMock()
    with patch("nucliadb.ingest.orm.processor.get_shard_manager", return_value=mock):
        yield mock


@pytest.fixture()
def processor(driver, sm):
    yield Processor(driver, None)


@pytest.fixture()
def resource():
    mock = MagicMock()
    mock.set_basic = AsyncMock()
    yield mock


@pytest.fixture()
def kb():
    mock = MagicMock(kbid="kbid")
    mock.get_resource_shard_id = AsyncMock()
    mock.get_resource_shard = AsyncMock()
    yield mock


async def test_commit_slug(processor: Processor, txn, resource):
    another_txn = Mock()
    resource.txn = another_txn
    resource.set_slug = AsyncMock()

    await processor.commit_slug(resource)

    resource.set_slug.assert_awaited_once()
    txn.commit.assert_awaited_once()
    assert resource.txn is another_txn


async def test_mark_resource_error(processor: Processor, txn, resource, kb, sm):
    await processor._mark_resource_error(kb, resource, partition="partition", seqid=1)
    txn.commit.assert_called_once()
    resource.set_basic.assert_awaited_once()
    sm.add_resource.assert_awaited_once_with(
        kb.get_resource_shard.return_value,
        resource.indexer.brain,
        1,
        partition="partition",
        kb="kbid",
    )


async def test_mark_resource_error_handle_error(
    processor: Processor, kb, resource, txn
):
    resource.set_basic.side_effect = Exception("test")
    await processor._mark_resource_error(kb, resource, partition="partition", seqid=1)
    txn.commit.assert_not_called()


async def test_mark_resource_error_skip_no_shard(
    processor: Processor, resource, driver, kb, txn
):
    kb.get_resource_shard.return_value = None
    await processor._mark_resource_error(kb, resource, partition="partition", seqid=1)
    txn.commit.assert_not_called()


async def test_mark_resource_error_skip_no_resource(
    processor: Processor, kb, driver, txn
):
    await processor._mark_resource_error(kb, None, partition="partition", seqid=1)
    txn.commit.assert_not_called()


def test_validate_indexable_resource():
    resource = noderesources_pb2.Resource()
    resource.paragraphs["test"].paragraphs["test"].sentences["test"].vector.append(1.0)
    validate_indexable_resource(resource)


def test_validate_indexable_resource_throws_error_for_max():
    resource = noderesources_pb2.Resource()
    for i in range(cluster_settings.max_resource_paragraphs + 1):
        resource.paragraphs["test"].paragraphs[f"test{i}"].sentences[
            "test"
        ].vector.append(1.0)
    with pytest.raises(ResourceNotIndexable):
        validate_indexable_resource(resource)
