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
from unittest.mock import patch

import pydantic
import pytest

from nucliadb import tasks
from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.common.context import ApplicationContext
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
    work_done = asyncio.Event()

    async def some_work(context: ApplicationContext, msg: Message):
        nonlocal work_done

        work_done.set()

    producer = tasks.create_producer(
        name="some_work",
        stream="work",
        stream_subjects=["work"],
        producer_subject="work",
        msg_type=Message,
    )
    await producer.initialize(context=context)

    consumer = tasks.create_consumer(
        name="some_work",
        stream="work",
        stream_subjects=["work"],
        consumer_subject="work",
        callback=some_work,
        msg_type=Message,
    )
    await consumer.initialize(context=context)

    msg = Message(kbid="kbid1")
    await producer(msg)

    await work_done.wait()
    work_done.clear()

    await consumer.finalize()


async def test_consumer_consumes_multiple_messages_concurrently(context):
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
        stream="concurrent_work",
        stream_subjects=["concurrent_work"],
        producer_subject="concurrent_work",
        msg_type=Message,
    )
    await producer.initialize(context=context)

    consumer = tasks.create_consumer(
        name="some_work",
        stream="concurrent_work",
        stream_subjects=["concurrent_work"],
        consumer_subject="concurrent_work",
        callback=some_work,
        msg_type=Message,
        max_concurrent_messages=10,
    )
    await consumer.initialize(context=context)

    # Produce three messages
    await producer(Message(kbid="kbid1"))
    await producer(Message(kbid="kbid2"))
    await producer(Message(kbid="kbid3"))

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
    cancelled_event = asyncio.Event()

    async def some_work(context: ApplicationContext, msg: Message):
        print(f"Doing some work! {msg.kbid}")
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled_event.set()

    producer = tasks.create_producer(
        name="some_work",
        stream="work",
        stream_subjects=["work"],
        producer_subject="work",
        msg_type=Message,
    )
    await producer.initialize(context=context)

    consumer = tasks.create_consumer(
        name="some_work",
        stream="work",
        stream_subjects=["work"],
        consumer_subject="work",
        callback=some_work,
        msg_type=Message,
    )
    await consumer.initialize(context=context)

    # Produce three messages
    await producer(Message(kbid="kbid1"))
    # Give a bit of time for the message to be delivered to the consumer via nats
    await asyncio.sleep(0.3)

    assert len(consumer.running_tasks) == 1

    await consumer.finalize()

    assert cancelled_event.is_set()
    assert consumer.running_tasks == []


async def test_consumer_max_concurrent_tasks(context):
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
        stream="work_with_max",
        stream_subjects=["work_with_max"],
        producer_subject="work_with_max",
        msg_type=Message,
    )
    await producer.initialize(context=context)

    consumer = tasks.create_consumer(
        name="some_work",
        stream="work_with_max",
        stream_subjects=["work_with_max"],
        consumer_subject="work_with_max",
        callback=some_work,
        msg_type=Message,
        max_concurrent_messages=5,
    )
    await consumer.initialize(context=context)

    for i in range(30):
        await producer(Message(kbid=f"kbid_{i}"))

    # Give a bit of time for the messages to be delivered to the consumer via nats
    await asyncio.sleep(0.3)

    assert len(consumer.running_tasks) == consumer.max_concurrent_messages

    await consumer.finalize()

    assert consumer.running_tasks == []
