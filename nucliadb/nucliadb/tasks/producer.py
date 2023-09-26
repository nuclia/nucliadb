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
import inspect
import uuid
from typing import Any, Coroutine, Optional

from nucliadb.common.context import ApplicationContext
from nucliadb.tasks.datamanager import TasksDataManager
from nucliadb.tasks.logger import logger
from nucliadb.tasks.models import Task, TaskNatsMessage, TaskStatus
from nucliadb_telemetry import errors
from nucliadb_utils import const


class NatsTaskProducer:
    def __init__(
        self,
        name: str,
        stream: const.Streams,
        callback=None,
    ):
        self.name = name
        self.stream = stream
        self.context: Optional[ApplicationContext] = None
        self.initialized = False
        self._dm = None
        self.callback = callback

    @property
    def dm(self):
        if self._dm is None:
            self._dm = TasksDataManager(self.context.kv_driver)
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

        if self.callback is not None:
            callback_args = (self.context, kbid, *args)
            check_is_callable_with(self.callback, *callback_args, **kwargs)

        task_id = uuid.uuid4().hex

        # Store task state
        task = Task(kbid=kbid, task_id=task_id, status=TaskStatus.SCHEDULED)
        await self.dm.set_task(task)

        try:
            # Send task to NATS
            task_msg = TaskNatsMessage(
                kbid=kbid, task_id=task_id, args=args, kwargs=kwargs
            )
            await self.context.nats_manager.js.publish(  # type: ignore
                self.stream.subject, task_msg.json().encode("utf-8")  # type: ignore
            )
            logger.info(
                "Task sent to NATS",
                extra={"kbid": kbid, "producer_name": self.name},
            )
        except Exception as e:
            errors.capture_exception(e)
            logger.error(
                "Error sending task to NATS",
                extra={"kbid": kbid, "producer_name": self.name},
            )
            await self.dm.delete_task(task)
            raise
        return task_id


def check_is_callable_with(f, *args, **kwargs) -> None:
    """
    Will raise a TypeError if f is not callable with the given arguments.
    """
    inspect.getcallargs(f, *args, **kwargs)


def create_producer(
    name: str,
    stream: const.Streams,
    callback: Optional[Coroutine[Any, Any, Any]] = None,
) -> NatsTaskProducer:
    return NatsTaskProducer(name=name, stream=stream, callback=callback)
