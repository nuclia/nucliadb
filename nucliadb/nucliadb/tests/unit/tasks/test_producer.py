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
from unittest.mock import AsyncMock, MagicMock, Mock

import nats
import pydantic
import pytest

from nucliadb.tasks.producer import create_producer


class Message(pydantic.BaseModel):
    kbid: str


def test_create_producer():
    stream = MagicMock()

    producer = create_producer("foo", stream=stream, msg_type=Message)
    assert not producer.initialized

    assert producer.name == "foo"
    assert producer.stream == stream


class TestProducer:
    @pytest.fixture(scope="function")
    def stream(self):
        return MagicMock()

    @pytest.fixture(scope="function")
    def nats_manager(self):
        mgr = MagicMock()
        mgr.js.stream_info = AsyncMock(side_effect=nats.js.errors.NotFoundError)
        mgr.js.add_stream = AsyncMock()
        mgr.js.publish = AsyncMock()
        yield mgr

    @pytest.fixture(scope="function")
    async def producer(self, context, stream, nats_manager):
        async def callback(context, msg: Message):
            pass

        producer = create_producer("foo", stream=stream, msg_type=Message)
        await producer.initialize(context)
        producer.context.nats_manager = nats_manager
        yield producer

    async def test_initialize_creates_stream(self, producer, nats_manager):
        # Check that the stream is on inialization
        assert nats_manager.js.add_stream.call_count == 1
        assert nats_manager.js.add_stream.call_args[1]["name"] == producer.stream.name
        assert nats_manager.js.add_stream.call_args[1]["subjects"] == [
            producer.stream.subject
        ]

    async def test_produce_raises_error_if_not_initialized(self, producer):
        producer.initialized = False
        with pytest.raises(RuntimeError):
            await producer(Mock())

    async def test_produce_ok(self, producer, stream):
        msg = Message(kbid="kbid")

        await producer(msg)

        publish_args = producer.context.nats_manager.js.publish.call_args[0]
        assert publish_args[0] == stream.subject

        raw_message = publish_args[1]
        sent_message = Message.parse_raw(raw_message)
        assert sent_message == msg

    async def test_produce_raises_publish_errors(self, producer, nats_manager):
        nats_manager.js.publish.side_effect = ValueError("foo")

        with pytest.raises(ValueError):
            await producer(Mock())
