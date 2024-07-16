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
from typing import AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.common.cluster.settings import settings
from nucliadb.ingest.consumer import shard_creator
from nucliadb_protos import nodereader_pb2
from nucliadb_protos.writer_pb2 import Notification, ShardObject, Shards

pytestmark = pytest.mark.asyncio


@pytest.fixture()
async def shard_creator_handler(
    shard_creator_check_delay: float, pubsub, shard_manager, shards: Shards
) -> AsyncIterator[shard_creator.ShardCreatorHandler]:
    datamanagers_mock = MagicMock()
    datamanagers_mock.atomic.cluster.get_current_active_shard = AsyncMock(return_value=shards.shards[0])

    with (
        patch("nucliadb.ingest.consumer.shard_creator.datamanagers", new=datamanagers_mock),
        patch("nucliadb.common.cluster.manager.datamanagers", new=datamanagers_mock),
    ):
        sc = shard_creator.ShardCreatorHandler(
            driver=AsyncMock(transaction=MagicMock(return_value=AsyncMock())),
            storage=AsyncMock(),
            pubsub=pubsub,
            check_delay=shard_creator_check_delay,
        )
        await sc.initialize()
        yield sc
        await sc.finalize()


@pytest.fixture()
def shard_creator_check_delay() -> float:
    return 0.005


@pytest.fixture()
def pubsub():
    mock = AsyncMock()
    mock.parse = lambda x: x
    yield mock


@pytest.fixture()
def shard_manager(reader, shards):
    sm = KBShardManager()
    sm.maybe_create_new_shard = AsyncMock(side_effect=sm.maybe_create_new_shard)
    sm.create_shard_by_kbid = AsyncMock()
    with (
        patch("nucliadb.ingest.consumer.shard_creator.get_shard_manager", return_value=sm),
    ):
        yield sm


@pytest.fixture()
def reader():
    reader = AsyncMock()
    node = MagicMock(reader=reader)
    with (
        patch(
            "nucliadb.ingest.consumer.shard_creator.choose_node",
            return_value=(node, "shard_id"),
        ),
        patch(
            "nucliadb.ingest.consumer.shard_creator.locking.distributed_lock",
            return_value=AsyncMock(),
        ),
    ):
        yield reader


@pytest.fixture()
def shards():
    return Shards(shards=[ShardObject(read_only=False)], actual=0)


@pytest.mark.parametrize(
    "action,paragraphs,should_try_create,should_create",
    [
        (Notification.Action.INDEXED, settings.max_shard_paragraphs + 1, True, True),
        (Notification.Action.INDEXED, settings.max_shard_paragraphs - 1, True, False),
        # actions other than indexed don't trigger shard creation
        (Notification.Action.COMMIT, settings.max_shard_paragraphs + 1, False, False),
        (Notification.Action.ABORT, settings.max_shard_paragraphs + 1, False, False),
    ],
)
async def test_handle_message_new_shard_creation(
    shard_creator_handler: shard_creator.ShardCreatorHandler,
    shard_creator_check_delay: float,
    shard_manager,
    reader,
    action: Notification.Action.ValueType,
    paragraphs: int,
    should_try_create: bool,
    should_create: bool,
):
    reader.GetShard.return_value = nodereader_pb2.Shard(paragraphs=paragraphs)
    notification = Notification(
        kbid="kbid",
        action=action,
    )
    await shard_creator_handler.handle_message(notification.SerializeToString())

    # wait for task to be scheduled and executed
    await asyncio.sleep(shard_creator_check_delay + 0.001)

    assert shard_manager.maybe_create_new_shard.called is should_try_create
    assert shard_manager.create_shard_by_kbid.called is should_create
