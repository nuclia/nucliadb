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
from nidx_protos import noderesources_pb2

from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.ingest.orm.exceptions import ResourceNotIndexable
from nucliadb.ingest.orm.processor import Processor
from nucliadb.ingest.orm.processor import processor as processor_module
from nucliadb.ingest.orm.processor.processor import validate_indexable_resource
from nucliadb_protos import knowledgebox_pb2, writer_pb2


@pytest.fixture()
def txn():
    yield AsyncMock()


@pytest.fixture()
def driver(txn):
    mock = MagicMock()
    mock.ro_transaction.return_value.__aenter__.return_value = txn
    mock.rw_transaction.return_value.__aenter__.return_value = txn
    yield mock


@pytest.fixture()
def processor(driver):
    yield Processor(driver, None)  # ty:ignore[invalid-argument-type]


@pytest.fixture()
def resource():
    mock = MagicMock()
    mock.set_data = AsyncMock()
    yield mock


@pytest.fixture()
def kb():
    mock = MagicMock(kbid="kbid")
    mock.get_shard = AsyncMock()
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


async def test_mark_resource_error(processor: Processor, driver, txn, resource, kb):
    await processor._mark_resource_error(kb, resource)
    driver.rw_transaction.assert_called_once_with(kbid=kb.kbid)
    assert kb.txn is txn
    assert resource.txn is txn
    txn.commit.assert_called_once()
    resource.set_data.assert_awaited_once()


async def test_mark_resource_error_handle_error(processor: Processor, kb, resource, txn):
    resource.set_data.side_effect = Exception("test")
    await processor._mark_resource_error(kb, resource)
    txn.commit.assert_not_called()


async def test_mark_resource_error_skip_no_resource(processor: Processor, kb, driver, txn):
    await processor._mark_resource_error(kb, None)
    driver.rw_transaction.assert_not_called()
    txn.commit.assert_not_called()


@pytest.mark.parametrize("use_slug", [False, True])
@pytest.mark.parametrize("exists", [False, True])
async def test_get_kb_obj_registry_uses_system_transaction(processor, driver, mocker, use_slug, exists):
    kb_txn = MagicMock()
    system_txn = driver.ro_transaction.return_value.__aenter__.return_value
    identifier = knowledgebox_pb2.KnowledgeBoxID(uuid="" if use_slug else "kbid", slug="slug")
    get_kbid = mocker.patch.object(processor_module.datamanagers.kb, "get_kbid", return_value="kbid")
    exists_mock = mocker.patch.object(processor_module.datamanagers.kb, "exists", return_value=exists)
    storage = mocker.patch.object(processor_module, "get_storage")
    kb_class = mocker.patch.object(processor_module, "KnowledgeBox")

    kb_obj = await processor.get_kb_obj(kb_txn, identifier)

    driver.ro_transaction.assert_called_once_with(system=True)
    exists_mock.assert_awaited_once_with(system_txn, kbid="kbid")
    if use_slug:
        get_kbid.assert_awaited_once_with(system_txn, slug="slug")
    else:
        get_kbid.assert_not_awaited()
    if exists:
        kb_class.assert_called_once_with(kb_txn, storage.return_value, "kbid")
        assert kb_obj is kb_class.return_value
    else:
        storage.assert_not_awaited()
        kb_class.assert_not_called()
        assert kb_obj is None


async def test_get_kb_obj_missing_slug_only_reads_system_registry(processor, driver, mocker):
    system_txn = driver.ro_transaction.return_value.__aenter__.return_value
    get_kbid = mocker.patch.object(processor_module.datamanagers.kb, "get_kbid", return_value=None)
    exists = mocker.patch.object(processor_module.datamanagers.kb, "exists")
    kb_class = mocker.patch.object(processor_module, "KnowledgeBox")

    assert (
        await processor.get_kb_obj(MagicMock(), knowledgebox_pb2.KnowledgeBoxID(slug="missing")) is None
    )

    driver.ro_transaction.assert_called_once_with(system=True)
    get_kbid.assert_awaited_once_with(system_txn, slug="missing")
    exists.assert_not_awaited()
    kb_class.assert_not_called()


@pytest.mark.parametrize("operation", ["delete_resource", "txn"])
async def test_skipped_message_sequence_write_uses_system_transaction(processor, mocker, operation):
    message = writer_pb2.BrokerMessage(kbid="kbid", slug="missing")
    mocker.patch.object(processor_module.datamanagers.atomic.resources, "get_rid", return_value=None)
    mocker.patch.object(processor_module.datamanagers.atomic.kb, "exists", return_value=False)
    system_txn = AsyncMock()
    write_context = mocker.patch.object(
        processor_module.datamanagers,
        "with_rw_transaction",
        return_value=AsyncMock(__aenter__=AsyncMock(return_value=system_txn)),
    )
    set_sequence = mocker.patch.object(processor_module.sequence_manager, "set_last_seqid")

    await getattr(processor, operation)(message, 42, "1")

    write_context.assert_called_once_with(system=True)
    set_sequence.assert_awaited_once_with(system_txn, "1", 42)
    system_txn.commit.assert_awaited_once()
    processor.driver.rw_transaction.assert_not_called()


def test_validate_indexable_resource():
    resource = noderesources_pb2.Resource()
    resource.paragraphs["test"].paragraphs["test"].sentences["test"].vector.append(1.0)
    validate_indexable_resource(resource)


def test_validate_indexable_resource_throws_error_for_max():
    resource = noderesources_pb2.Resource()
    for i in range(cluster_settings.max_resource_paragraphs + 1):
        resource.paragraphs["test"].paragraphs[f"test{i}"].sentences["test"].vector.append(1.0)
    with pytest.raises(ResourceNotIndexable):
        validate_indexable_resource(resource)
