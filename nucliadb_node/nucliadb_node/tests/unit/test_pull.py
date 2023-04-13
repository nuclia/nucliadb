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
from unittest import mock
from unittest.mock import AsyncMock, Mock

import nats as natslib
import pytest
from nats.aio.client import Msg
from nucliadb_protos.nodewriter_pb2 import IndexMessage, TypeMessage

from nucliadb_node.pull import IndexedPublisher, Worker


class NatsConnectionTest:
    def __init__(self):
        self.js = AsyncMock()
        self.jetstream = Mock(return_value=self.js)
        self.close = AsyncMock()
        self.flush = AsyncMock()


class NatsTest:
    def __init__(self, connection):
        self.connect = AsyncMock(return_value=connection)


class TestIndexedPublisher:
    stream = "stream"
    target = "target.{partition}"
    auth = "token"
    servers = ["nats://localhost:4222"]

    @pytest.fixture(scope="function")
    def publisher(self, nats_mock):
        return IndexedPublisher(self.stream, self.target, self.servers, self.auth)

    @pytest.fixture(scope="function")
    def index_message(self):
        delpb = IndexMessage()
        delpb.node = "node"
        delpb.typemessage = TypeMessage.DELETION
        delpb.partition = "11"
        delpb.resource = "rid"
        return delpb

    @pytest.fixture(scope="function")
    def nats_connection(self):
        return NatsConnectionTest()

    @pytest.fixture(scope="function")
    def nats_mock(self, nats_connection):
        nats_test = NatsTest(nats_connection)
        with mock.patch("nucliadb_node.pull.nats", nats_test):
            yield nats_test

    @pytest.mark.asyncio
    async def test_initialize(self, nats_mock, nats_connection, publisher):
        await publisher.initialize()

        nats_mock.connect.assert_called_once_with(
            **{
                "closed_cb": publisher.on_connection_closed,
                "reconnected_cb": publisher.on_reconnection,
                "user_credentials": self.auth,
                "servers": self.servers,
            }
        )
        assert publisher.nc == nats_connection
        assert publisher.js == nats_connection.js

    @pytest.mark.asyncio
    async def test_initialize_creates_stream_if_not_found(
        self, nats_connection, publisher
    ):
        nats_connection.js.stream_info.side_effect = [
            natslib.js.errors.NotFoundError,
            None,
        ]
        await publisher.initialize()

        nats_connection.js.add_stream.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_initialize_creates_stream_only_if_not_found(
        self, nats_connection, publisher
    ):
        nats_connection.js.stream_info.return_value = None
        await publisher.initialize()

        nats_connection.js.add_stream.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_finalize(self, nats_connection, publisher):
        await publisher.initialize()
        await publisher.finalize()
        await publisher.finalize()

        nats_connection.flush.assert_awaited_once()
        nats_connection.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_indexed(self, nats_connection, publisher, index_message):
        await publisher.initialize()

        await publisher.indexed(index_message)

        subject = self.target.format(partition=index_message.partition)
        nats_connection.js.publish.assert_awaited_once()
        assert nats_connection.js.publish.call_args[0][0] == subject

    @pytest.mark.asyncio
    async def test_indexed_skips_if_no_partition(
        self, nats_connection, publisher, index_message
    ):
        await publisher.initialize()
        index_message.ClearField("partition")

        await publisher.indexed(index_message)

        nats_connection.js.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_indexed_errors_if_not_initialized(self, publisher, index_message):
        publisher.js = None
        with pytest.raises(RuntimeError):
            await publisher.indexed(index_message)


class TestSubsciptionWorker:
    @pytest.fixture(scope="function")
    def worker(self):
        writer = AsyncMock()
        reader = AsyncMock()
        worker = Worker(writer, reader, "node")
        worker.set_resource = AsyncMock()
        yield worker

    def get_msg(self, seqid):
        client = AsyncMock()
        reply = f"foo.bar.ba.blan.ca.{seqid}.bar"
        msg = Msg(client, "subject", reply)
        msg.ack = AsyncMock()
        return msg

    @pytest.mark.asyncio
    async def test_discards_old_messages(self, worker):
        worker.last_seqid = 10
        msg = self.get_msg(seqid=9)
        await worker.subscription_worker(msg)

        msg.ack.assert_awaited_once()
        worker.set_resource.assert_not_awaited()
