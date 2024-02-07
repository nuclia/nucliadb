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
from nucliadb_protos.writer_pb2 import Notification, ShardObject, Shards

from nucliadb.common.cluster.settings import settings
from nucliadb.ingest.consumer import shard_creator
from nucliadb_protos import nodereader_pb2

pytestmark = pytest.mark.asyncio


@pytest.fixture()
def pubsub():
    mock = AsyncMock()
    mock.parse = lambda x: x
    yield mock


@pytest.fixture()
def reader():
    yield AsyncMock()


@pytest.fixture()
def kbdm():
    mock = MagicMock()
    mock.get_model_metadata = AsyncMock(return_value="model")
    with patch(
        "nucliadb.common.cluster.manager.KnowledgeBoxDataManager", return_value=mock
    ):
        yield mock


@pytest.fixture()
def shard_manager(reader):
    sm = MagicMock()
    node = MagicMock(reader=reader)
    shards = Shards(shards=[ShardObject()], actual=0)
    sm.get_shards_by_kbid_inner = AsyncMock(return_value=shards)
    sm.maybe_create_new_shard = AsyncMock()
    with patch(
        "nucliadb.ingest.consumer.shard_creator.get_shard_manager", return_value=sm
    ), patch(
        "nucliadb.ingest.consumer.shard_creator.choose_node",
        return_value=(node, "shard_id"),
    ):
        yield sm


@pytest.fixture()
async def shard_creator_handler(pubsub, shard_manager):
    sc = shard_creator.ShardCreatorHandler(
        driver=AsyncMock(transaction=MagicMock(return_value=AsyncMock())),
        storage=AsyncMock(),
        pubsub=pubsub,
        check_delay=0.05,
    )
    await sc.initialize()
    yield sc
    await sc.finalize()


async def test_handle_message_create_new_shard(
    shard_creator_handler: shard_creator.ShardCreatorHandler,
    reader,
    kbdm,
    shard_manager,
):
    reader.GetShard.return_value = nodereader_pb2.Shard(
        paragraphs=settings.max_shard_paragraphs + 1
    )

    notif = Notification(
        kbid="kbid",
        action=Notification.Action.INDEXED,
    )
    await shard_creator_handler.handle_message(notif.SerializeToString())
    await asyncio.sleep(0.06)
    shard_manager.maybe_create_new_shard.assert_called_with(
        "kbid", settings.max_shard_paragraphs + 1, 0, 0
    )


async def test_handle_message_do_not_create(
    shard_creator_handler: shard_creator.ShardCreatorHandler, reader, shard_manager
):
    reader.GetShard.return_value = nodereader_pb2.Shard(
        paragraphs=settings.max_shard_paragraphs - 1
    )

    notif = Notification(
        kbid="kbid",
        action=Notification.Action.INDEXED,
    )
    await shard_creator_handler.handle_message(notif.SerializeToString())

    await shard_creator_handler.finalize()

    shard_manager.create_shard_by_kbid.assert_not_called()


async def test_handle_message_ignore_not_indexed(
    shard_creator_handler: shard_creator.ShardCreatorHandler, shard_manager
):
    notif = Notification(
        kbid="kbid",
        action=Notification.Action.COMMIT,
    )
    await shard_creator_handler.handle_message(notif.SerializeToString())

    await shard_creator_handler.finalize()

    shard_manager.create_shard_by_kbid.assert_not_called()
