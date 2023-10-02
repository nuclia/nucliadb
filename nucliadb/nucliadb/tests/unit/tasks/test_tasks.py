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
import pydantic
import pytest

from nucliadb import tasks
from nucliadb_utils import const


class StreamTest(const.Streams):
    name = "test"
    group = "test"
    subject = "test"


class Message(pydantic.BaseModel):
    kbid: str


async def test_start_consumer(context):
    with pytest.raises(ValueError):
        await tasks.start_consumer("foo", context)

    @tasks.register_task("foo", StreamTest, msg_type=Message)
    async def some_test_work(context, msg: Message):
        ...

    consumer = await tasks.start_consumer("foo", context)
    assert consumer.initialized


async def test_get_producer(context):
    with pytest.raises(ValueError):
        await tasks.get_producer("bar", context)

    @tasks.register_task("bar", StreamTest, msg_type=Message)
    async def some_test_work(context, msg: Message):
        ...

    producer = await tasks.get_producer("bar", context)
    assert producer.initialized
    assert producer.msg_type == Message
    assert producer.name == "bar_producer"
