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

import pytest

from nucliadb.async_tasks import TaskNotFoundError
from nucliadb.async_tasks.consumer import create_consumer
from nucliadb.async_tasks.models import Task, TaskNatsMessage, TaskStatus


def test_create_consumer():
    stream = MagicMock()

    async def callback():
        ...

    consumer = create_consumer("foo", stream=stream, callback=callback, max_retries=3)
    assert not consumer.initialized

    assert consumer.name == "foo"
    assert consumer.stream == stream
    assert consumer.callback == callback
    assert consumer.max_retries == 3


class TestSubscriptionWorker:
    @pytest.fixture(scope="function")
    async def callback(self):
        yield AsyncMock()

    @pytest.fixture(scope="function")
    def task(self):
        task = Task(kbid="kbid", task_id="task_id", status=TaskStatus.SCHEDULED)
        yield task

    @pytest.fixture(scope="function")
    async def datamanager(self, task):
        dm = MagicMock()
        dm.get_task = AsyncMock(return_value=task)
        dm.set_task = AsyncMock()
        yield dm

    @pytest.fixture(scope="function")
    async def consumer(self, context, callback, datamanager):
        consumer = create_consumer(
            "foo", stream=MagicMock(), callback=callback, max_retries=1
        )
        await consumer.initialize(context)
        consumer._dm = datamanager
        yield consumer

    @pytest.fixture(scope="function")
    def task_nats_message(self):
        yield TaskNatsMessage(kbid="kbid", task_id="task_id")

    @pytest.fixture(scope="function")
    def msg(self, task_nats_message):
        data = task_nats_message.json().encode("utf-8")
        msg = MagicMock()
        msg.data = data
        msg.ack = AsyncMock()
        msg.nak = AsyncMock()
        yield msg

    async def test_success(self, consumer, msg):
        await consumer.subscription_worker(msg)

        assert consumer.dm.set_task.call_args[0][0].status == TaskStatus.FINISHED

    async def test_validation_error(self, consumer, msg):
        msg.data = b"foo"

        await consumer.subscription_worker(msg)

        msg.ack.assert_called_once()

    async def test_task_not_found(self, consumer, msg):
        consumer.dm.get_task.side_effect = TaskNotFoundError

        await consumer.subscription_worker(msg)

        msg.ack.assert_called_once()

    async def test_task_finished_are_not_scheduled(self, consumer, msg):
        consumer.dm.get_task.return_value.status = TaskStatus.FINISHED

        await consumer.subscription_worker(msg)

        msg.ack.assert_called_once()

    async def test_task_errored_are_not_scheduled(self, consumer, msg):
        consumer.dm.get_task.return_value.status = TaskStatus.ERRORED

        await consumer.subscription_worker(msg)

        msg.ack.assert_called_once()

    async def test_task_cancelled_are_not_scheduled(self, consumer, msg):
        consumer.dm.get_task.return_value.status = TaskStatus.CANCELLED

        await consumer.subscription_worker(msg)

        msg.ack.assert_called_once()

    async def test_task_max_tries_are_not_scheduled(self, consumer, msg):
        consumer.dm.get_task.return_value.tries = 30

        await consumer.subscription_worker(msg)

        msg.ack.assert_called_once()

    async def test_task_failed_are_scheduled(self, consumer, msg):
        consumer.dm.get_task.return_value.status = TaskStatus.FAILED
        consumer.dm.get_task.return_value.tries = 1

        await consumer.subscription_worker(msg)

        assert consumer.dm.set_task.call_args[0][0].status == TaskStatus.FINISHED

    async def test_task_callback_retriable_errors(self, consumer, msg, callback):
        callback.side_effect = [Exception("foo"), None]

        await consumer.subscription_worker(msg)

        # First call is the one that fails
        msg.nak.assert_called_once()
        end_status = consumer.dm.set_task.call_args_list[-1][0][0].status
        assert end_status == TaskStatus.FAILED

        await consumer.subscription_worker(msg)

        # Second call is the one that succeeds
        end_status = consumer.dm.set_task.call_args_list[-1][0][0].status
        assert end_status == TaskStatus.FINISHED

    async def test_task_callback_unrecoverable_errors(self, consumer, msg, callback):
        callback.side_effect = TaskNotFoundError

        await consumer.subscription_worker(msg)

        msg.ack.assert_called_once()
        end_status = consumer.dm.set_task.call_args_list[-1][0][0].status
        assert end_status == TaskStatus.ERRORED
