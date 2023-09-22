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

from datetime import datetime
from typing import Optional

from nucliadb.async_tasks.exceptions import TaskNotFound
from nucliadb.async_tasks.models import Task
from nucliadb.common.maindb.driver import Driver

KB_TASKS = "/kbs/{kbid}/async_tasks/{task_id}"


class AsyncTasksDataManager:
    def __init__(self, driver: Driver):
        self.driver = driver

    async def get_task(self, kbid: str, task_id: str) -> Task:
        key = KB_TASKS.format(kbid=kbid, task_id=task_id)
        data = await self._get(key)
        if data is None or data == b"":
            raise TaskNotFound()
        decoded = data.decode("utf-8")
        return Task.parse_raw(decoded)

    async def set_task(self, task: Task) -> None:
        task.updated_at = datetime.utcnow()
        key = KB_TASKS.format(kbid=task.kbid, task_id=task.task_id)
        data = task.json().encode("utf-8")
        await self._set(key, data)

    async def delete_task(self, task: Task) -> None:
        key = KB_TASKS.format(kbid=task.kbid, task_id=task.task_id)
        await self._delete(key)

    async def _get(self, key: str) -> Optional[bytes]:
        async with self.driver.transaction() as txn:
            return await txn.get(key)

    async def _set(self, key: str, data: bytes):
        async with self.driver.transaction() as txn:
            await txn.set(key, data)
            await txn.commit()

    async def _delete(self, key: str):
        async with self.driver.transaction() as txn:
            await txn.delete(key)
            await txn.commit()
