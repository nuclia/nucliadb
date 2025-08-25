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

import asyncio
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pydantic
import pytest

from nucliadb import tasks
from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.common.context import ApplicationContext
from nucliadb.tasks.retries import TaskMetadata, TaskRetryHandler, purge_metadata
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
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass

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


async def test_task_retry_handler_ok(context):
    task_id = str(uuid.uuid4())
    kbid = uuid.uuid4().hex

    async def callback(*args, **kwargs):
        return 100

    trh = TaskRetryHandler(kbid=kbid, task_type="foo", task_id=task_id, context=context)
    callback_retried = trh.wrap(callback)

    result = await callback_retried("some", param="param")
    assert result == 100

    metadata = await trh.get_metadata()
    assert metadata.status == TaskMetadata.Status.COMPLETED
    assert metadata.task_id == task_id
    assert metadata.retries == 0
    assert metadata.error_messages == []


async def test_task_retry_handler_errors_are_retried(context):
    callback = AsyncMock()
    callback.side_effect = ValueError("foo")

    task_id = str(uuid.uuid4())
    kbid = uuid.uuid4().hex
    trh = TaskRetryHandler(kbid=kbid, task_type="foo", task_id=task_id, context=context, max_retries=2)
    callback_retried = trh.wrap(callback)

    with pytest.raises(ValueError):
        await callback_retried("foo", bar="baz")

    callback.assert_called_once_with("foo", bar="baz")

    metadata = await trh.get_metadata()
    assert metadata.status == TaskMetadata.Status.RUNNING
    assert metadata.retries == 1
    assert "foo" in metadata.error_messages[0]

    callback.side_effect = None
    callback.return_value = 100
    result = await callback_retried("foo", bar="baz")
    assert result == 100

    metadata = await trh.get_metadata()
    assert metadata.status == TaskMetadata.Status.COMPLETED
    assert metadata.retries == 1


async def test_task_retry_handler_ignored_statuses(context):
    callback = AsyncMock()

    task_id = str(uuid.uuid4())
    kbid = uuid.uuid4().hex
    trh = TaskRetryHandler(kbid=kbid, task_type="foo", task_id=task_id, context=context)
    callback_retried = trh.wrap(callback)

    for status in (TaskMetadata.Status.FAILED, TaskMetadata.Status.COMPLETED):
        metadata = TaskMetadata(
            task_id="bar",
            status=status,
            retries=0,
            error_messages=[],
        )
        await trh.set_metadata(metadata)
        await callback_retried("foo", bar="baz")
        callback.assert_not_called()


async def test_task_retry_handler_max_retries(context):
    callback = AsyncMock()

    task_id = str(uuid.uuid4())
    kbid = uuid.uuid4().hex
    trh = TaskRetryHandler(
        kbid=kbid,
        task_type="foo",
        task_id=task_id,
        context=context,
        max_retries=2,
    )
    callback_retried = trh.wrap(callback)

    metadata = TaskMetadata(
        task_id=task_id,
        status=TaskMetadata.Status.RUNNING,
        retries=40,
        error_messages=[],
    )
    await trh.set_metadata(metadata)

    await callback_retried("foo", bar="baz")

    callback.assert_not_called()

    metadata = await trh.get_metadata()
    assert metadata.status == TaskMetadata.Status.FAILED
    assert "Max retries reached" in metadata.error_messages[-1]


async def test_purge_metadata(context):
    kbid = uuid.uuid4().hex
    trh1 = TaskRetryHandler(kbid=kbid, task_type="foo", task_id="task_id", context=context)
    old_metadata = TaskMetadata(
        task_id="task_id",
        status=TaskMetadata.Status.COMPLETED,
        retries=0,
        error_messages=[],
        last_modified=datetime(2021, 1, 1, tzinfo=timezone.utc),
    )
    await trh1.set_metadata(old_metadata)

    trh2 = TaskRetryHandler(kbid=kbid, task_type="bar", task_id="task_id", context=context)
    recent_metadata = TaskMetadata(
        task_id="task_id",
        status=TaskMetadata.Status.RUNNING,
        retries=0,
        error_messages=[],
        last_modified=datetime.now(timezone.utc),
    )
    await trh2.set_metadata(recent_metadata)

    assert await purge_metadata(context.kv_driver) >= 1
    assert await purge_metadata(context.kv_driver) == 0

    assert await trh1.get_metadata() is None
    assert await trh2.get_metadata() == recent_metadata
