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
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from nucliadb.tasks.models import TaskNatsMessage, TaskStatus
from nucliadb.tasks.producer import create_producer


def test_create_producer():
    stream = MagicMock()

    producer = create_producer("foo", stream=stream)
    assert not producer.initialized

    assert producer.name == "foo"
    assert producer.stream == stream


class TestProducer:
    @pytest.fixture(scope="function")
    def stream(self):
        return MagicMock()

    @pytest.fixture(scope="function")
    def datamanager(self):
        dm = MagicMock()
        dm.set_task = AsyncMock()
        dm.delete_task = AsyncMock()
        yield dm

    @pytest.fixture(scope="function")
    def nats_manager(self):
        mgr = MagicMock()
        mgr.js.add_stream = AsyncMock()
        mgr.js.publish = AsyncMock()
        yield mgr

    @pytest.fixture(scope="function")
    async def producer(self, context, datamanager, stream, nats_manager):
        async def callback(context, kbid, foo=1, bar=2):
            pass

        producer = create_producer("foo", stream=stream, callback=callback)
        await producer.initialize(context)
        producer._dm = datamanager
        producer.context.nats_manager = nats_manager
        yield producer

    async def test_initialize(self, producer, nats_manager):
        assert nats_manager.js.add_stream.call_args[1]["name"] == producer.stream.name
        assert nats_manager.js.add_stream.call_args[1]["subjects"] == [
            producer.stream.subject
        ]

    async def test_produce_not_initialized(self, producer):
        producer.initialized = False
        with pytest.raises(RuntimeError):
            await producer("kbid", "foo", bar="baz")

    async def test_produce(self, producer, stream):
        task_id = await producer("kbid", "foo", bar="baz")
        uuid.UUID(task_id)

        task_created = producer.dm.set_task.call_args[0][0]
        task_created.kbid == "kbid"
        task_created.task_id == task_id
        task_created.status == TaskStatus.SCHEDULED

        publish_args = producer.context.nats_manager.js.publish.call_args[0]
        assert publish_args[0] == stream.subject

        raw_task_nats_message = publish_args[1]
        tnm = TaskNatsMessage.parse_raw(raw_task_nats_message)
        assert tnm.task_id == task_id
        assert tnm.kbid == "kbid"

    async def test_produce_error(self, producer, nats_manager):
        nats_manager.js.publish.side_effect = ValueError("foo")

        with pytest.raises(ValueError):
            await producer("kbid", "foo", bar="baz")

        producer.dm.delete_task.assert_called_once()

    async def test_raises_error_on_invalid_callback_arguments(self, producer):
        with pytest.raises(TypeError):
            await producer("kbid", "foo", not_a_valid_arg="baz")
