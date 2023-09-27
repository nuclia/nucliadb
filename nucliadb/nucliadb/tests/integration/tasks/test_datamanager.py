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
import pytest

from nucliadb.tasks import Task
from nucliadb.tasks.datamanager import TasksDataManager
from nucliadb.tasks.exceptions import TaskNotFoundError
from nucliadb.tasks.models import TaskStatus


async def test_crud(maindb_driver):
    dm = TasksDataManager(maindb_driver)
    task = Task(kbid="kbid", task_id="foo", status=TaskStatus.SCHEDULED)
    task.status = TaskStatus.ERRORED
    await dm.set_task(task)

    assert await dm.get_task(kbid="kbid", task_id="foo") == task

    await dm.delete_task(task)

    with pytest.raises(TaskNotFoundError):
        await dm.get_task(kbid="kbid", task_id="foo")

    await dm.delete_task(task)
