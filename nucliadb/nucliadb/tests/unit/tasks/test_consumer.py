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
from unittest.mock import AsyncMock, MagicMock

import pydantic
import pytest

from nucliadb.tasks.consumer import create_consumer


class Message(pydantic.BaseModel):
    kbid: str


def test_create_consumer():
    stream = MagicMock()

    async def callback():
        ...

    consumer = create_consumer(
        "foo", stream=stream, msg_type=Message, callback=callback
    )
    assert not consumer.initialized

    assert consumer.name == "foo"
    assert consumer.stream == stream
    assert consumer.callback == callback
    assert consumer.msg_type == Message


class TestSubscriptionWorker:
    @pytest.fixture(scope="function")
    async def callback(self):
        yield AsyncMock()

    @pytest.fixture(scope="function")
    async def consumer(self, context, callback):
        consumer = create_consumer(
            "foo", stream=MagicMock(), callback=callback, msg_type=Message
        )
        await consumer.initialize(context)
        yield consumer

    @pytest.fixture(scope="function")
    def task_message(self):
        yield Message(kbid="kbid")

    @pytest.fixture(scope="function")
    def msg(self, task_message):
        data = task_message.json().encode("utf-8")
        msg = MagicMock()
        msg.data = data
        msg.ack = AsyncMock()
        msg.nak = AsyncMock()
        yield msg

    async def test_callback_ok(self, consumer, msg, callback):
        await consumer.subscription_worker(msg)

        callback.assert_called_once()

    async def test_callback_error(self, consumer, msg, callback):
        callback.side_effect = Exception("foo")

        await consumer.subscription_worker(msg)

        callback.assert_called_once()
        msg.nak.assert_called_once()

    async def test_validation_error(self, consumer, msg):
        msg.data = b"foo"

        await consumer.subscription_worker(msg)

        msg.ack.assert_called_once()
