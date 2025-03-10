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
import time
from contextlib import contextmanager
from unittest.mock import AsyncMock, Mock, patch

from nucliadb.tasks.retries import TaskMetadata, TaskRetryHandler
import pydantic
import pytest

from nucliadb import tasks
from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.common.context import ApplicationContext
from nucliadb.tasks.utils import NatsConsumer, NatsStream
from nucliadb_utils.settings import indexing_settings


@contextmanager
def set_standalone_mode(value: bool):
    prev_value = cluster_settings.standalone_mode
    cluster_settings.standalone_mode = value
    yield
    cluster_settings.standalone_mode = prev_value


@pytest.fixture()
async def context(nucliadb, natsd):
    with set_standalone_mode(False), patch.object(indexing_settings, "index_jetstream_servers", [natsd]):
        context = ApplicationContext()
        await context.initialize()
        yield context
        await context.finalize()


class Message(pydantic.BaseModel):
    kbid: str


async def test_tasks_factory_api(context):
    nstream = NatsStream(name="work", subjects=["work"])
    nconsumer = NatsConsumer(subject="work", group="work")

    work_done = asyncio.Event()

    async def some_work(context: ApplicationContext, msg: Message):
        nonlocal work_done

        work_done.set()

    producer = tasks.create_producer(
        name="some_work",
        stream=nstream,
        producer_subject=nconsumer.subject,
        msg_type=Message,
    )

    consumer = tasks.create_consumer(
        name="some_work",
        stream=nstream,
        consumer=nconsumer,
        callback=some_work,
        msg_type=Message,
    )
    await consumer.initialize(context=context)

    msg = Message(kbid="kbid1")
    await producer.send(msg)

    await work_done.wait()
    work_done.clear()

    await consumer.finalize()


async def test_consumer_consumes_multiple_messages_concurrently(context):
    nstream = NatsStream(name="concurrent_work", subjects=["concurrent_work"])
    nconsumer = NatsConsumer(subject="concurrent_work", group="concurrent_work")

    work_duration_s = 2
    work_done = {
        "kbid1": asyncio.Event(),
        "kbid2": asyncio.Event(),
        "kbid3": asyncio.Event(),
    }

    async def some_work(context: ApplicationContext, msg: Message):
        nonlocal work_done
        print(f"Doing some work! {msg.kbid}")
        await asyncio.sleep(work_duration_s)
        work_done[msg.kbid].set()

    producer = tasks.create_producer(
        name="some_work",
        stream=nstream,
        producer_subject=nconsumer.subject,
        msg_type=Message,
    )

    consumer = tasks.create_consumer(
        name="some_work",
        stream=nstream,
        consumer=nconsumer,
        callback=some_work,
        msg_type=Message,
        max_concurrent_messages=10,
    )
    await consumer.initialize(context=context)

    # Produce three messages
    await producer.send(Message(kbid="kbid1"))
    await producer.send(Message(kbid="kbid2"))
    await producer.send(Message(kbid="kbid3"))

    # Wait for them to finish
    start = time.perf_counter()
    for event in work_done.values():
        await event.wait()
        event.clear()
    work_done.clear()
    elapsed = time.perf_counter() - start

    # To verify that the tasks run concurrently, we assert that the sum of the
    # total consume duration should be at most slightly bigger than the time needed
    # to consume a single message.
    assert elapsed < (work_duration_s * 1.1)

    await consumer.finalize()


async def test_consumer_finalize_cancels_tasks(context):
    nstream = NatsStream(name="work", subjects=["work"])
    nconsumer = NatsConsumer(subject="work", group="work")

    cancelled_event = asyncio.Event()

    async def some_work(context: ApplicationContext, msg: Message):
        print(f"Doing some work! {msg.kbid}")
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled_event.set()

    producer = tasks.create_producer(
        name="some_work",
        stream=nstream,
        producer_subject=nconsumer.subject,
        msg_type=Message,
    )

    consumer = tasks.create_consumer(
        name="some_work",
        stream=nstream,
        consumer=nconsumer,
        callback=some_work,
        msg_type=Message,
    )
    await consumer.initialize(context=context)

    # Produce three messages
    await producer.send(Message(kbid="kbid1"))
    # Give a bit of time for the message to be delivered to the consumer via nats
    await asyncio.sleep(0.3)

    assert len(consumer.running_tasks) == 1

    await consumer.finalize()

    assert cancelled_event.is_set()
    assert consumer.running_tasks == []


async def test_consumer_max_concurrent_tasks(context):
    nstream = NatsStream(name="work_with_max", subjects=["work_with_max"])
    nconsumer = NatsConsumer(subject="work_with_max", group="work_with_max")

    async def some_work(context: ApplicationContext, msg: Message):
        print(f"Doing some work! {msg.kbid}")
        start = time.perf_counter()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            elapsed = time.perf_counter() - start
            print(f"Work cancelled after {elapsed:.2f}s! {msg.kbid}")

    producer = tasks.create_producer(
        name="some_work",
        stream=nstream,
        producer_subject=nconsumer.subject,
        msg_type=Message,
    )

    consumer = tasks.create_consumer(
        name="some_work",
        stream=nstream,
        consumer=nconsumer,
        callback=some_work,
        msg_type=Message,
        max_concurrent_messages=5,
    )
    await consumer.initialize(context=context)

    for i in range(30):
        await producer.send(Message(kbid=f"kbid_{i}"))

    # Give a bit of time for the messages to be delivered to the consumer via nats
    await asyncio.sleep(0.3)

    assert len(consumer.running_tasks) == consumer.max_concurrent_messages

    await consumer.finalize()

    assert consumer.running_tasks == []


class TestTaskRetryHandler:
    @pytest.fixture(scope="function")
    def callback(self):
        return AsyncMock()

    @pytest.fixture(scope="function")
    def txn(self):
        txn = Mock()
        txn.get = AsyncMock()
        txn.set = AsyncMock()
        txn.commit = AsyncMock()
        txn.delete = AsyncMock()
        yield txn

    @pytest.fixture(scope="function")
    def context(self, txn):
        ctxt = Mock()
        ctxt.kv_driver = Mock()
        txn_cm = Mock()
        txn_cm.__aenter__ = AsyncMock(return_value=txn)
        txn_cm.__aexit__ = AsyncMock()
        ctxt.kv_driver.transaction.return_value = txn_cm
        yield ctxt

    async def test_ok(self, callback, context, txn):
        # Metadata does not exist at first
        txn.get.return_value = None

        callback.return_value = 100
        trh = TaskRetryHandler(kbid="kbid", task_type="foo", task_id="bar", context=context)
        callback_retried = trh.wrap(callback)

        result = await callback_retried("some", param="param")
        assert result == 100

        callback.assert_called_once_with("some", param="param")

        final_metadata_args = txn.set.call_args_list[-1][0]
        assert final_metadata_args[0] == "/kbs/kbid/tasks/foo/bar"
        metadata = TaskMetadata.model_validate_json(final_metadata_args[1])
        assert metadata.status == TaskMetadata.Status.COMPLETED
        assert metadata.task_id == "bar"
        assert metadata.retries == 0
        assert metadata.error_messages == []

    async def test_errors_are_retried(self, callback, context, txn):
        txn.get.return_value = None
        callback.side_effect = ValueError("foo")

        trh = TaskRetryHandler(kbid="kbid", task_type="foo", task_id="bar", context=context, max_retries=2)
        callback_retried = trh.wrap(callback)

        with pytest.raises(ValueError):
            await callback_retried("foo", bar="baz")

        callback.assert_called_once_with("foo", bar="baz")

        final_metadata_args = txn.set.call_args_list[-1][0]
        metadata = TaskMetadata.model_validate_json(final_metadata_args[1])
        assert metadata.status == TaskMetadata.Status.RUNNING
        assert metadata.retries == 1
        assert "foo" in metadata.error_messages[0]

        with pytest.raises(ValueError):
            await callback_retried("foo", bar="baz")

        final_metadata_args = txn.set.call_args_list[-1][0]
        metadata = TaskMetadata.model_validate_json(final_metadata_args[1])
        assert metadata.task.status == TaskMetadata.Status.RUNNING
        assert metadata.task.retries == 2

    async def test_ignored_statuses(self, callback, context, txn):
        trh = TaskRetryHandler(kbid="kbid", task_type="foo", task_id="bar", context=context)
        callback_retried = trh.wrap(callback)

        for status in (TaskMetadata.Status.FAILED, TaskMetadata.Status.COMPLETED):
            txn.get.return_value = TaskMetadata(
                task_id="bar",
                status=status,
                retries=0,
                error_messages=[],
            ).model_dump_json().encode()
            await callback_retried("foo", bar="baz")
            callback.assert_not_called()