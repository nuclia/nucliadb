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
from contextlib import contextmanager

import pydantic
import pytest

from nucliadb import tasks
from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.common.context import ApplicationContext
from nucliadb_utils import const

pytestmark = pytest.mark.asyncio


@contextmanager
def set_standalone_mode(value: bool):
    prev_value = cluster_settings.standalone_mode
    cluster_settings.standalone_mode = value
    yield
    cluster_settings.standalone_mode = prev_value


@pytest.fixture()
async def context(nucliadb, natsd):
    context = ApplicationContext()
    with set_standalone_mode(False):
        await context.initialize()
    yield context
    await context.finalize()


async def test_tasks_registry_api(context):
    work_done = asyncio.Event()

    class Message(pydantic.BaseModel):
        kbid: str

    class MyStreams(const.Streams):
        class SOME_WORK:
            name = "work"
            subject = "work"
            group = "work"

    @tasks.register_task(name="some_work", stream=MyStreams.SOME_WORK, msg_type=Message)
    async def some_work(context: ApplicationContext, msg: Message):
        nonlocal work_done
        work_done.set()

    producer = await tasks.get_producer("some_work", context=context)

    msg = Message(kbid="kbid1")
    await producer(msg)

    await tasks.start_consumer("some_work", context=context)

    await work_done.wait()
    work_done.clear()


async def test_tasks_factory_api(context):
    work_done = asyncio.Event()

    class Message(pydantic.BaseModel):
        kbid: str

    class MyStreams(const.Streams):
        class SOME_WORK:
            name = "work"
            subject = "work"
            group = "work"

    async def some_work(context: ApplicationContext, msg: Message):
        nonlocal work_done
        work_done.set()

    producer = tasks.create_producer(
        name="some_work",
        stream=MyStreams.SOME_WORK,
        msg_type=Message,
    )
    await producer.initialize(context=context)

    consumer = tasks.create_consumer(
        name="some_work",
        stream=MyStreams.SOME_WORK,
        callback=some_work,
        msg_type=Message,
    )
    await consumer.initialize(context=context)

    msg = Message(kbid="kbid1")
    await producer(msg)

    await work_done.wait()
    work_done.clear()
