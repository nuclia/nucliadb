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
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from nats.aio.client import Msg
from nucliadb_protos.nodewriter_pb2 import IndexMessage, TypeMessage

from nucliadb_node.listeners import IndexedPublisher
from nucliadb_node.listeners.gc_scheduler import ShardManager
from nucliadb_node.pull import Worker
from nucliadb_utils import const


@pytest.fixture(autouse=True)
def pubsub():
    pubsub = AsyncMock()
    with mock.patch(
        "nucliadb_node.listeners.indexed_publisher.get_pubsub", return_value=pubsub
    ):
        yield pubsub


class TestShardManager:
    @pytest.fixture()
    def gc_lock(self):
        yield asyncio.Semaphore(1)

    @pytest.fixture()
    def writer(self):
        writer = MagicMock(garbage_collector=AsyncMock())
        yield writer

    @pytest.fixture()
    def shard_manager(self, writer, gc_lock):
        sm = ShardManager("shard_id", writer, gc_lock)
        sm.target_gc_resources = 5
        yield sm

    @pytest.mark.asyncio
    async def test_gc(self, shard_manager: ShardManager, writer):
        await shard_manager.gc()

        writer.garbage_collector.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_schedule_gc(self, shard_manager: ShardManager, writer):
        shard_manager.shard_changed_event(0)

        await asyncio.sleep(0.1)

        writer.garbage_collector.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_schedule_gc_after_target(self, shard_manager: ShardManager, writer):
        for _ in range(shard_manager.target_gc_resources):
            shard_manager.shard_changed_event()

        await asyncio.sleep(0.1)

        writer.garbage_collector.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_schedule_gc_after_target_multiple_times(
        self, shard_manager: ShardManager, writer
    ):
        for _ in range(shard_manager.target_gc_resources):
            shard_manager.shard_changed_event()
            shard_manager.shard_changed_event()

        await asyncio.sleep(0.1)

        writer.garbage_collector.assert_awaited_once()


class TestIndexedPublisher:
    @pytest.fixture(scope="function")
    async def publisher(self):
        pub = IndexedPublisher()
        await pub.initialize()
        yield pub

    @pytest.fixture(scope="function")
    def index_message(self):
        delpb = IndexMessage()
        delpb.node = "node"
        delpb.typemessage = TypeMessage.DELETION
        delpb.partition = "11"
        delpb.resource = "rid"
        delpb.kbid = "kbid"
        return delpb

    @pytest.mark.asyncio
    async def test_initialize(self, publisher, pubsub):
        assert publisher.pubsub == pubsub

    @pytest.mark.asyncio
    async def test_finalize(self, publisher, pubsub):
        await publisher.finalize()

        pubsub.finalize.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_indexed(self, publisher, index_message, pubsub):
        await publisher.indexed(index_message)

        channel = const.PubSubChannels.RESOURCE_NOTIFY.format(kbid=index_message.kbid)
        pubsub.publish.assert_awaited_once()
        assert pubsub.publish.call_args[0][0] == channel

    @pytest.mark.asyncio
    async def test_indexed_skips_if_no_partition(
        self, publisher, index_message, pubsub
    ):
        index_message.ClearField("partition")

        await publisher.indexed(index_message)

        pubsub.publish.assert_not_awaited()


class TestSubscriptionWorker:
    @pytest.fixture
    def nats_manager(self):
        return AsyncMock()

    @pytest.fixture(scope="function")
    def worker(self, settings, nats_manager):
        writer = AsyncMock()
        with mock.patch("nucliadb_node.pull.get_storage"), mock.patch(
            "nucliadb_node.indexer.get_storage"
        ):
            worker = Worker(writer, "node", nats_manager)
            worker.store_seqid = Mock()
            yield worker

    def get_msg(self, seqid):
        client = AsyncMock()
        reply = f"foo.bar.ba.blan.ca.{seqid}.bar"
        msg = Msg(client, "subject", reply)
        msg.ack = AsyncMock()
        return msg
