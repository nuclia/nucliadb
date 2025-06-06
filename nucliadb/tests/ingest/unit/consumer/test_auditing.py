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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nidx_protos import nodereader_pb2

from nucliadb.ingest.consumer import auditing
from nucliadb_protos.writer_pb2 import Audit, BrokerMessage, Notification, ShardObject


@pytest.fixture()
def pubsub():
    mock = AsyncMock()
    mock.parse = lambda x: x
    yield mock


@pytest.fixture()
def reader():
    yield AsyncMock()


@pytest.fixture()
def shard_manager(reader):
    nm = MagicMock()
    nm.get_shards_by_kbid = AsyncMock(return_value=[ShardObject()])
    with (
        patch("nucliadb.ingest.consumer.auditing.get_shard_manager", return_value=nm),
    ):
        yield nm


@pytest.fixture()
def audit():
    yield AsyncMock()


@pytest.fixture()
async def index_audit_handler(pubsub, audit, shard_manager):
    iah = auditing.IndexAuditHandler(
        audit=audit,
        pubsub=pubsub,
        check_delay=0.05,
    )
    await iah.initialize()
    yield iah
    await iah.finalize()


@pytest.fixture()
async def writes_audit_handler(pubsub, audit, shard_manager):
    rwah = auditing.ResourceWritesAuditHandler(
        storage=AsyncMock(),
        audit=audit,
        pubsub=pubsub,
    )
    await rwah.initialize()
    yield rwah
    await rwah.finalize()


async def test_handle_message(index_audit_handler: auditing.IndexAuditHandler, reader, audit):
    reader.GetShard.return_value = nodereader_pb2.Shard(fields=5, paragraphs=6)

    notif = Notification(
        kbid="kbid",
        action=Notification.Action.INDEXED,
    )
    await index_audit_handler.handle_message(notif.SerializeToString())

    await asyncio.sleep(0.06)


async def test_handle_message_ignore_not_indexed(index_audit_handler: auditing.IndexAuditHandler, audit):
    notif = Notification(
        kbid="kbid",
        action=Notification.Action.COMMIT,
    )
    await index_audit_handler.handle_message(notif.SerializeToString())

    await index_audit_handler.finalize()

    audit.report.assert_not_called()


async def test_resource_handle_message_processor_messages_are_not_audited(
    writes_audit_handler: auditing.ResourceWritesAuditHandler, audit
):
    message_audit = Audit()
    message_audit.message_source = BrokerMessage.MessageSource.PROCESSOR
    notif = Notification(
        kbid="kbid",
        action=Notification.Action.COMMIT,
        write_type=Notification.WriteType.MODIFIED,
        message_audit=message_audit,
    )
    await writes_audit_handler.handle_message(notif.SerializeToString())

    await writes_audit_handler.finalize()

    audit.report.assert_not_called()
