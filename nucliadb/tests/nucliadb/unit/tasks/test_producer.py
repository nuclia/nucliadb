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
from unittest.mock import Mock

import pydantic
import pytest

from nucliadb.tasks.producer import create_producer
from nucliadb.tasks.utils import NatsConsumer, NatsStream
from nucliadb_utils.utilities import Utility, clean_utility, set_utility


class Message(pydantic.BaseModel):
    kbid: str


stream = NatsStream(name="stream", subjects=["stream.>"])
consumer = NatsConsumer(subject="stream.subject", group="group")


@pytest.fixture(scope="function")
def nats_manager_utility(nats_manager):
    set_utility(Utility.NATS_MANAGER, nats_manager)
    yield
    clean_utility(Utility.NATS_MANAGER)


def test_create_producer(nats_manager_utility):
    producer = create_producer(
        "foo",
        stream=stream,
        producer_subject=consumer.subject,
        msg_type=Message,
    )
    assert producer.name == "foo"
    assert producer.stream == stream


class TestProducer:
    @pytest.fixture(scope="function")
    def nats_manager_utility(self, nats_manager):
        set_utility(Utility.NATS_MANAGER, nats_manager)
        yield nats_manager
        clean_utility(Utility.NATS_MANAGER)

    @pytest.fixture(scope="function")
    async def producer(self, context, nats_manager_utility):
        async def callback(context, msg: Message):
            pass

        producer = create_producer(
            "foo",
            stream=stream,
            producer_subject=consumer.subject,
            msg_type=Message,
        )
        yield producer

    async def test_produce_ok(self, producer):
        msg = Message(kbid="kbid")

        await producer.send(msg)

        publish_args = producer.nats_manager.js.publish.call_args[0]
        assert publish_args[0] == consumer.subject

        raw_message = publish_args[1]
        sent_message = Message.model_validate_json(raw_message)
        assert sent_message == msg

    async def test_produce_raises_publish_errors(self, producer, nats_manager):
        nats_manager.js.publish.side_effect = ValueError("foo")

        with pytest.raises(ValueError):
            await producer.send(Mock())
