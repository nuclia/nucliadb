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
from typing import Any, Optional

from nucliadb.async_tasks.datamanager import AsyncTasksDataManager
from nucliadb.async_tasks.logger import logger
from nucliadb.async_tasks.models import Status, Task, TaskNatsMessage
from nucliadb.common.context import ApplicationContext
from nucliadb_telemetry import errors
from nucliadb_utils import const


class NatsTaskProducer:
    def __init__(
        self,
        name: str,
        stream: const.Streams,
    ):
        self.name = name
        self.stream = stream
        self.context: Optional[ApplicationContext] = None
        self.initialized = False

    @property
    def dm(self):
        if self._dm is None:
            self._dm = AsyncTasksDataManager(self.context.kv_driver)
        return self._dm

    async def initialize(self, context: ApplicationContext):
        self.context = context
        await self.context.nats_manager.js.add_stream(
            name=self.stream.name, subjects=[self.stream.subject]  # type: ignore
        )
        self.initialized = True

    async def __call__(self, kbid: str, *args: tuple[Any, ...], **kwargs) -> str:
        if not self.initialized:
            raise RuntimeError("NatsTaskProducer not initialized")

        task_id = uuid.uuid4().hex

        # Store task state
        task = Task(kbid=kbid, task_id=task_id, status=Status.SCHEDULED)
        await self.dm.set_task(task)

        try:
            # Send task to NATS
            task_msg = TaskNatsMessage(
                kbid=kbid, task_id=task_id, args=args, kwargs=kwargs
            )
            await self.context.nats_manager.js.publish(  # type: ignore
                self.stream.subject, task_msg.json().encode("utf-8")  # type: ignore
            )
        except Exception as e:
            errors.capture_exception(e)
            logger.error(
                f"Error sending task for {kbid} to NATS",
                extra={"kbid": kbid, "producer_name": self.name},
            )
            await self.dm.delete_task(task)
            raise
        return task_id


def create_producer(
    name: str,
    stream: const.Streams,
) -> NatsTaskProducer:
    return NatsTaskProducer(name=name, stream=stream)
