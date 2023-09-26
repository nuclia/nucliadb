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
from unittest.mock import patch

import pytest

from nucliadb import tasks
from nucliadb.tasks.models import Task, TaskStatus
from nucliadb_utils import const


class StreamTest(const.Streams):
    name = "test"
    group = "test"
    subject = "test"


async def test_start_consumer(context):
    with pytest.raises(ValueError):
        await tasks.start_consumer("foo", context)

    @tasks.register_task("foo", StreamTest)
    async def some_test_work(context, kbid):
        ...

    consumer = await tasks.start_consumer("foo", context)
    assert consumer.initialized


async def test_get_producer(context):
    with pytest.raises(ValueError):
        await tasks.get_producer("bar", context)

    @tasks.register_task("bar", StreamTest)
    async def some_test_work(context, kbid):
        ...

    producer = await tasks.get_producer("bar", context)
    assert producer.initialized


@patch("nucliadb.tasks.AsyncTasksDataManager.get_task")
async def test_wait_for_task(get_task_mock, context):
    get_task_mock.return_value = Task(
        kbid="kbid", task_id="task_id", status=TaskStatus.CANCELLED
    )
    with pytest.raises(tasks.TaskCancelled):
        await tasks.wait_for_task("kbid", "task_id", context)

    get_task_mock.return_value = Task(
        kbid="kbid", task_id="task_id", status=TaskStatus.ERRORED
    )
    with pytest.raises(tasks.TaskErrored):
        await tasks.wait_for_task("kbid", "task_id", context)

    get_task_mock.return_value = Task(
        kbid="kbid", task_id="task_id", status=TaskStatus.FINISHED
    )
    await tasks.wait_for_task("kbid", "task_id", context)
